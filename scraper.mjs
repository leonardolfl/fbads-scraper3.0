import fs from "fs";
import path from "path";
import os from "os";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

/**
 * scraper.mjs (completo)
 * - Sharding via TOTAL_BATCHES / BATCH_ID
 * - Claim atômico (claimed_at, claimed_by, claim_expires)
 * - Debug saving (HTML + PNG) em caso de falha
 * - Rotação de devices + user-agents por contexto
 *
 * ENV suportadas:
 *  - TOTAL_BATCHES (default 1)
 *  - BATCH_ID (1..TOTAL_BATCHES, default 1)
 *  - PROCESS_LIMIT (opcional)
 *  - PARALLEL (default 5)
 *  - WAIT_TIME (ms) default 7000
 *  - NAV_TIMEOUT (ms) default 60000
 *  - WORKER_ID (opcional) identificador do worker
 */

const TOTAL_BATCHES = Math.max(1, parseInt(process.env.TOTAL_BATCHES || "1", 10));
const BATCH_ID = Math.max(1, parseInt(process.env.BATCH_ID || "1", 10)); // 1-based
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;
let PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "5", 10));
const WAIT_TIME = parseInt(process.env.WAIT_TIME || "7000", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug";
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;

const DEVICE_NAMES = [
  "iPhone 13 Pro Max",
  "iPhone 12",
  "iPhone 11 Pro",
  "Pixel 7",
  "Pixel 5",
  "Galaxy S21 Ultra",
  "Galaxy Note 10",
  "iPad Mini",
  "Desktop Chrome",
];

const DEVICE_POOL = DEVICE_NAMES.map((n) => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
];

// Helpers
function ensureDebugDir() {
  if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });
}
function nowTs() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}
function jitter(ms) {
  return Math.floor(Math.random() * ms);
}
function fisherYatesShuffle(array) {
  const a = array.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
function pickDeviceAndUA(index) {
  const device = DEVICE_POOL[index % DEVICE_POOL.length] || {};
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  return { device, ua };
}

async function saveDebug(page, offerId, note) {
  try {
    ensureDebugDir();
    const ts = nowTs();
    const safe = `offer-${offerId}-${ts}`;
    const htmlPath = path.join(DEBUG_DIR, `${safe}.html`);
    const pngPath = path.join(DEBUG_DIR, `${safe}.png`);
    const content = await page.content();
    await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + content, "utf8");
    await page.screenshot({ path: pngPath, fullPage: true }).catch(() => {});
    console.log(`🧾 Debug salvo: ${htmlPath} , ${pngPath}`);
    return { htmlPath, pngPath };
  } catch (err) {
    console.warn("❌ Falha ao salvar debug:", err?.message || err);
    return {};
  }
}

// Claim helper: attempt atomic claim (status_updated IS NULL OR 'pending')
// Requires claimed_at/claimed_by/claim_expires columns present in DB
async function tryClaimOffer(offerId) {
  try {
    const now = new Date().toISOString();
    const expires = new Date(Date.now() + 10 * 60 * 1000).toISOString(); // +10 min
    const { data, error } = await supabase
      .from("swipe_file_offers")
      .update({
        status_updated: "in_progress",
        claimed_at: now,
        claimed_by: WORKER_ID,
        claim_expires: expires,
      })
      .or("status_updated.is.null,status_updated.eq.pending")
      .eq("id", offerId)
      .select("id");
    if (error) {
      console.warn(`[${offerId}] claim error:`, error.message || error);
      return false;
    }
    return Array.isArray(data) && data.length > 0;
  } catch (err) {
    console.warn(`[${offerId}] exception during claim:`, err?.message || err);
    return false;
  }
}

// Clear claim fields (keeps claimed_at as history, clears claimed_by and claim_expires)
async function clearClaim(offerId) {
  try {
    const { error } = await supabase
      .from("swipe_file_offers")
      .update({ claimed_by: null, claim_expires: null })
      .eq("id", offerId);
    if (error) console.warn(`[${offerId}] clearClaim error:`, error.message || error);
  } catch (err) {
    console.warn(`[${offerId}] exception during clearClaim:`, err?.message || err);
  }
}

(async () => {
  console.log("🚀 Scraper iniciado (sharding + claim + debug).");
  console.log(`ENV: TOTAL_BATCHES=${TOTAL_BATCHES} BATCH_ID=${BATCH_ID} PROCESS_LIMIT=${PROCESS_LIMIT || "none"} PARALLEL=${PARALLEL} WORKER_ID=${WORKER_ID}`);

  ensureDebugDir();

  const browser = await chromium.launch({ headless: true });
  let shuttingDown = false;

  const safeClose = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("🛑 Encerrando: fechando browser...");
    try { await browser.close(); } catch (e) {}
    process.exit(0);
  };
  process.on("SIGINT", safeClose);
  process.on("SIGTERM", safeClose);

  // fetch offers
  const { data: offersRaw, error: offersError } = await supabase.from("swipe_file_offers").select("*");
  if (offersError) {
    console.error("❌ Erro ao buscar ofertas:", offersError);
    try { await browser.close(); } catch {}
    process.exit(1);
  }
  if (!Array.isArray(offersRaw) || offersRaw.length === 0) {
    console.log("ℹ️ Nenhuma oferta encontrada. Encerrando.");
    try { await browser.close(); } catch {}
    process.exit(0);
  }

  // deterministic ordering then shuffle to spread batches
  const offersSorted = offersRaw.slice().sort((a, b) => {
    if (a.id == null) return 1;
    if (b.id == null) return -1;
    return String(a.id).localeCompare(String(b.id), undefined, { numeric: true });
  });
  const offersShuffled = fisherYatesShuffle(offersSorted);

  // apply sharding
  const offersSharded = offersShuffled.filter((_, idx) => (idx % TOTAL_BATCHES) === (BATCH_ID - 1));
  const offersToProcess = PROCESS_LIMIT ? offersSharded.slice(0, PROCESS_LIMIT) : offersSharded;

  console.log(`📦 Total ofertas originais: ${offersRaw.length}`);
  console.log(`📂 Batch ${BATCH_ID}/${TOTAL_BATCHES}: ${offersToProcess.length} ofertas a processar`);

  let processedCount = 0;
  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < offersToProcess.length; i += PARALLEL) {
    if (shuttingDown) break;
    const batch = offersToProcess.slice(i, i + PARALLEL);
    console.log(`\n🧩 Bloco ${Math.floor(i / PARALLEL) + 1}: ${batch.length} ofertas (PARALLEL=${PARALLEL})`);

    await Promise.all(batch.map(async (offer, idxInBatch) => {
      if (!offer?.id || !offer?.adLibraryUrl) {
        console.warn(`⚠️ [${offer?.id}] oferta inválida, pulando.`);
        return;
      }

      // attempt claim
      const claimed = await tryClaimOffer(offer.id);
      if (!claimed) {
        console.log(`🔒 [${offer.id}] Não foi possível claim (outro worker pode ter claimado). Pulando.`);
        return;
      }

      const globalIndex = i + idxInBatch;
      const { device, ua } = pickDeviceAndUA(globalIndex);

      const contextOptions = { ...(device || {}) };
      if (ua) contextOptions.userAgent = ua;

      let context, page;
      try {
        context = await browser.newContext(contextOptions);
        await context.route("**/*", (route) => {
          const type = route.request().resourceType();
          if (["image", "stylesheet", "font", "media"].includes(type)) route.abort();
          else route.continue();
        });

        page = await context.newPage();
        console.log(`⌛ [${offer.id}] Acessando (${device?.name || "custom"} / UA=${ua ? ua.slice(0,60) + "..." : "n/a"}): ${offer.adLibraryUrl}`);

        await page.goto(offer.adLibraryUrl, { waitUntil: "networkidle", timeout: NAV_TIMEOUT });
        await page.waitForTimeout(WAIT_TIME);

        try {
          await page.waitForSelector('div:has-text("resultados"), div:has-text("results")', { timeout: 15000 });
        } catch {}

        const html = await page.content();
        let match = html.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);
        if (!match) {
          const bodyText = await page.textContent("body");
          match = bodyText && bodyText.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);
        }

        const updated_at = new Date().toISOString();

        if (match) {
          const activeAds = parseInt(String(match[1]).replace(/\./g, "").replace(/,/g, ""), 10);
          console.log(`✅ [${offer.id}] ${offer.offerName || "offer"}: ${activeAds} anúncios`);

          const { error } = await supabase
            .from("swipe_file_offers")
            .update({ activeAds, updated_at, status_updated: "success", debug_html_path: null, debug_png_path: null })
            .eq("id", offer.id);

          if (error) console.warn(`⚠️ [${offer.id}] Erro ao atualizar DB: ${error.message || error}`);
          // clear claim metadata but keep claimed_at as history
          await clearClaim(offer.id);

          successCount++;
        } else {
          console.warn(`❌ [${offer.id}] contador não encontrado — salvando debug`);
          const dbg = await saveDebug(page, offer.id, "no-counter");
          const { error } = await supabase
            .from("swipe_file_offers")
            .update({
              activeAds: null,
              updated_at,
              status_updated: "no_counter",
              debug_html_path: dbg.htmlPath || dbg.htmlPath,
              debug_png_path: dbg.pngPath || dbg.pngPath,
            })
            .eq("id", offer.id);
          if (error) console.warn(`⚠️ [${offer.id}] Erro ao atualizar DB (no_counter): ${error.message || error}`);
          // clear claim metadata to allow re-processing later (keep claimed_at for history)
          await clearClaim(offer.id);
          failCount++;
        }
      } catch (err) {
        console.error(`🚫 [${offer.id}] Erro ao processar: ${err?.message || err}`);
        const dbg = page ? await saveDebug(page, offer.id, `exception-${String(err?.message || err)}`) : {};
        try {
          await supabase
            .from("swipe_file_offers")
            .update({
              activeAds: null,
              updated_at: new Date().toISOString(),
              status_updated: "error",
              debug_html_path: dbg.htmlPath || dbg.htmlPath,
              debug_png_path: dbg.pngPath || dbg.pngPath,
            })
            .eq("id", offer.id);
        } catch (e) {
          console.warn(`⚠️ [${offer.id}] Falha ao atualizar DB após erro: ${e?.message || e}`);
        }
        await clearClaim(offer.id);
        failCount++;
      } finally {
        try { if (page) await page.close(); } catch {}
        try { if (context) await context.close(); } catch {}
        processedCount++;
        // small randomized delay
        await sleep(500 + jitter(800));
      }
    })); // end Promise.all

    // small pause between batches
    await sleep(500 + jitter(1000));
  }

  try { await browser.close(); } catch (e) {}
  console.log("✅ Execução finalizada.");
  console.log(`📈 Processed: ${processedCount}`);
  console.log(`✔️ Success: ${successCount}`);
  console.log(`❌ Failures: ${failCount}`);
  console.log(`🗂 Debug files (se houver): ${path.resolve(DEBUG_DIR)}`);

  process.exit(0);
})();
