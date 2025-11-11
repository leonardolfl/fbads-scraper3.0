import fs from "fs";
import path from "path";
import os from "os";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

/**
 * scraper.mjs (atualizado)
 * - Sharding: TOTAL_BATCHES / BATCH_ID
 * - Claim atômico em scrape_status (in_progress)
 * - Campos DB usados: scrape_status, scrape_debug (jsonb), last_scraped_at, claimed_at, claimed_by, claim_expires
 * - Debug: salva HTML + PNG em ./debug e grava paths em scrape_debug
 * - Env:
 *    SUPABASE_URL, SUPABASE_KEY (secrets)
 *    TOTAL_BATCHES (default 1)
 *    BATCH_ID (1..TOTAL_BATCHES, default 1)
 *    PROCESS_LIMIT (opcional)
 *    PARALLEL (default 5)
 *    WAIT_TIME (ms) default 7000
 *    NAV_TIMEOUT (ms) default 60000
 *    WORKER_ID (opcional) identificador do worker (injetado pelo workflow)
 */

const TOTAL_BATCHES = Math.max(1, parseInt(process.env.TOTAL_BATCHES || "1", 10));
const BATCH_ID = Math.max(1, parseInt(process.env.BATCH_ID || "1", 10));
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

// Claim helper: attempt atomic claim (scrape_status IS NULL OR 'pending')
// Requires claimed_at/claimed_by/claim_expires columns present in DB
async function tryClaimOffer(offerId) {
  try {
    const now = new Date().toISOString();
    const expires = new Date(Date.now() + 10 * 60 * 1000).toISOString(); // +10 min
    const { data, error } = await supabase
      .from("swipe_file_offers")
      .update({
        scrape_status: "in_progress",
        claimed_at: now,
        claimed_by: WORKER_ID,
        claim_expires: expires,
      })
      .or("scrape_status.is.null,scrape_status.eq.pending")
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

// Clear claim metadata (keep claimed_at as history)
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
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;

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
            .update({
              activeAds,
              last_scraped_at: updated_at,
              scrape_status: "success",
              scrape_debug: null
            })
            .eq("id", offer.id);

          if (error) {
            console.warn(`⚠️ [${offer.id}] Erro ao atualizar DB: ${error.message || error}`);
          }

          // clear claim metadata (keep claimed_at as history)
          await clearClaim(offer.id);

          consecutiveSuccess++;
          consecutiveFails = 0;
          successCount++;
        } else {
          console.warn(`❌ [${offer.id}] contador não encontrado — salvando debug`);
          const dbg = await saveDebug(page, offer.id, "no-counter");
          const debugObj = {
            html_path: dbg.htmlPath || null,
            png_path: dbg.pngPath || null,
            note: "no_counter"
          };
          const { error } = await supabase
            .from("swipe_file_offers")
            .update({
              activeAds: null,
              last_scraped_at: updated_at,
              scrape_status: "no_counter",
              scrape_debug: debugObj
            })
            .eq("id", offer.id);
          if (error) console.warn(`⚠️ [${offer.id}] Erro ao atualizar DB (no_counter): ${error.message || error}`);
          await clearClaim(offer.id);
          consecutiveFails++;
          failCount++;
        }
      } catch (err) {
        console.error(`🚫 [${offer.id}] Erro ao processar: ${err?.message || err}`);
        const dbg = page ? await saveDebug(page, offer.id, `exception-${String(err?.message || err)}`) : {};
        const debugObj = {
          html_path: dbg.htmlPath || null,
          png_path: dbg.pngPath || null,
          note: `exception: ${String(err?.message || err)}`
        };
        try {
          await supabase
            .from("swipe_file_offers")
            .update({
              activeAds: null,
              last_scraped_at: new Date().toISOString(),
              scrape_status: "error",
              scrape_debug: debugObj
            })
            .eq("id", offer.id);
        } catch (e) {
          console.warn(`⚠️ [${offer.id}] Falha ao atualizar DB após erro: ${e?.message || e}`);
        }
        await clearClaim(offer.id);
        consecutiveFails++;
        failCount++;
      } finally {
        try { if (page) await page.close(); } catch {}
        try { if (context) await context.close(); } catch {}
        processedCount++;
        // small randomized delay
        await sleep(500 + jitter(800));
      }
    })); // end Promise.all

    // adaptive throttling
    if (consecutiveFails >= 5 && PARALLEL > 3) {
      PARALLEL = Math.max(3, Math.floor(PARALLEL / 2));
      console.log(`⚠️ Reduzindo paralelismo para ${PARALLEL} (muitas falhas).`);
    } else if (consecutiveSuccess >= 20 && PARALLEL < 8) {
      PARALLEL = Math.min(8, PARALLEL + 2);
      console.log(`✅ Aumentando paralelismo para ${PARALLEL} (estável).`);
      consecutiveSuccess = 0;
    }

    // occasional longer pause
    if ((i + PARALLEL) % 100 === 0) {
      const pause = Math.floor(Math.random() * 15000) + 30000;
      console.log(`⏸️ Pausa extra de ${(pause / 1000).toFixed(1)}s para reduzir risco de bloqueio`);
      await sleep(pause);
    }
  }

  try { await browser.close(); } catch (e) {}
  console.log("✅ Scraper finalizado.");
  console.log(`📈 Totais: processed=${processedCount} | success=${successCount} | fail=${failCount}`);
  console.log(`🗂 Debug files (se houver): ${path.resolve(DEBUG_DIR)}`);

  process.exit(0);
})();
