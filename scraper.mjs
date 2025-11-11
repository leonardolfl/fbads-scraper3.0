import fs from "fs";
import path from "path";
import os from "os";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

/**
 * scraper.mjs (melhorado)
 * - Mantém chunking por WORKER_INDEX (não altera DB/workflow)
 * - Device pool + rotação de user-agents
 * - Abort recursos pesados (images, styles, fonts, media)
 * - PARALLEL parametrizável por env (default reduzido)
 * - Salva debug HTML/PNG localmente quando contador não encontrado ou erro
 *
 * ENVs:
 * - SUPABASE_URL, SUPABASE_KEY (secrets existentes)
 * - WORKER_INDEX (injetado pelo workflow)
 * - TOTAL_WORKERS (default 4)
 * - PARALLEL (default 3)
 * - PROCESS_LIMIT (opcional)
 * - WAIT_TIME (ms) default 7000
 * - NAV_TIMEOUT (ms) default 60000
 * - DEBUG_DIR (opcional) default ./debug
 * - WORKER_ID (opcional) identificador do runner
 */

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX || process.env.WORKER_INDEX === "0" ? process.env.WORKER_INDEX : "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;
let PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "3", 10));
const WAIT_TIME = parseInt(process.env.WAIT_TIME || "7000", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug";
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;

// Device pool: escolha de devices do Playwright
const DEVICE_NAMES = [
  "iPhone 13 Pro Max",
  "iPhone 12",
  "iPhone 11 Pro",
  "Pixel 7",
  "Pixel 5",
  "Galaxy S21 Ultra",
  "iPad Mini",
  "Desktop Chrome"
];
const DEVICE_POOL = DEVICE_NAMES.map((n) => devices[n]).filter(Boolean);

// Pool de user-agents (ampliada)
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; rv:122.0) Gecko/20100101 Firefox/122.0",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
];

// Helpers
function ensureDebugDir() {
  if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });
}
function nowTs() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}
function nowIso() {
  return new Date().toISOString();
}
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}
function jitter(ms) {
  return Math.floor(Math.random() * ms);
}
function shuffle(array) {
  const a = array.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
function pickDeviceAndUA(index) {
  const device = DEVICE_POOL.length > 0 ? DEVICE_POOL[index % DEVICE_POOL.length] : {};
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  return { device, ua };
}

async function saveDebug(page, offerId, note = "") {
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
    return { htmlPath: null, pngPath: null };
  }
}

function randomDelay(min = 1500, max = 3500) {
  return new Promise((res) => setTimeout(res, Math.random() * (max - min) + min));
}

// Main processing
async function processOffers(offersSlice) {
  const browser = await chromium.launch({ headless: true });
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;

  for (let i = 0; i < offersSlice.length; i += PARALLEL) {
    const batch = offersSlice.slice(i, i + PARALLEL);
    console.log(`📦 Processando bloco: ${Math.floor(i / PARALLEL) + 1} (${batch.length} ofertas)`);

    await Promise.all(
      batch.map(async (offer, idxInBatch) => {
        if (!offer || !offer.adLibraryUrl) return;

        const globalIndex = i + idxInBatch;
        const { device, ua } = pickDeviceAndUA(globalIndex);
        const contextOptions = { ...(device || {}) };
        if (ua) contextOptions.userAgent = ua;

        const context = await browser.newContext(contextOptions);

        // Abort resources to speed up
        await context.route("**/*", (route) => {
          const type = route.request().resourceType();
          if (["image", "stylesheet", "font", "media"].includes(type)) return route.abort();
          return route.continue();
        });

        const page = await context.newPage();

        try {
          console.log(`⌛ [${offer.id}] Acessando (${device?.name || "custom"} / UA=${(ua || "").slice(0,60)}): ${offer.adLibraryUrl}`);
          await page.goto(offer.adLibraryUrl, { waitUntil: "networkidle", timeout: NAV_TIMEOUT });
          await page.waitForTimeout(WAIT_TIME + jitter(1000));

          const html = await page.content();
          let match = html.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);

          if (!match) {
            const bodyText = (await page.textContent("body")) || "";
            match = bodyText.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);
          }

          const updated_at = nowIso();

          if (match) {
            const activeAds = parseInt(String(match[1]).replace(/\./g, "").replace(/,/g, ""), 10);
            console.log(`✅ [${offer.id}] ${offer.offerName || "offer"}: ${activeAds} anúncios`);
            // Keep DB behavior unchanged: update status_updated as before
            const { error } = await supabase
              .from("swipe_file_offers")
              .update({ activeAds, updated_at, status_updated: "success" })
              .eq("id", offer.id);
            if (error) console.warn(`[${offer.id}] Erro ao atualizar DB:`, error.message || error);

            consecutiveSuccess++;
            consecutiveFails = 0;
          } else {
            console.warn(`❌ [${offer.id}] contador não encontrado — salvando debug`);
            const dbg = await saveDebug(page, offer.id, "no-counter");
            const { error } = await supabase
              .from("swipe_file_offers")
              .update({ activeAds: null, updated_at, status_updated: "no_counter" })
              .eq("id", offer.id);
            if (error) console.warn(`[${offer.id}] Erro ao atualizar DB (no_counter):`, error.message || error);

            consecutiveFails++;
          }
        } catch (err) {
          console.error(`⚠️ [${offer.id}] Erro ao processar:`, err?.message || err);
          try {
            const dbg = await saveDebug(page, offer.id, `exception-${String(err?.message || err)}`);
            await supabase
              .from("swipe_file_offers")
              .update({ activeAds: null, updated_at: nowIso(), status_updated: "error" })
              .eq("id", offer.id);
          } catch (e) {
            console.warn(`[${offer.id}] Falha ao atualizar DB após erro:`, e?.message || e);
          }
          consecutiveFails++;
        } finally {
          try { await page.close(); } catch {}
          try { await context.close(); } catch {}
          await randomDelay(800, 1800);
        }
      })
    );

    // adaptive throttling
    if (consecutiveFails >= 3 && PARALLEL > 3) {
      PARALLEL = 3;
      console.log("⚠️ Reduzindo paralelismo para 3.");
    } else if (consecutiveSuccess >= 20 && PARALLEL < 5) {
      PARALLEL = 5;
      console.log("✅ Restaurando paralelismo para 5.");
      consecutiveSuccess = 0;
    }

    // small pause between batches
    await sleep(500 + jitter(1000));
  }

  await browser.close();
}

(async () => {
  console.log("🚀 Scraper iniciado (WORKER_INDEX:", WORKER_INDEX, "TOTAL_WORKERS:", TOTAL_WORKERS, "WORKER_ID:", WORKER_ID, ")");
  ensureDebugDir();

  const { data: offers, error } = await supabase.from("swipe_file_offers").select("*");

  if (error) {
    console.error("❌ Erro ao buscar ofertas:", error);
    process.exit(1);
  }

  if (!Array.isArray(offers) || offers.length === 0) {
    console.log("ℹ️ Nenhuma oferta encontrada. Encerrando.");
    process.exit(0);
  }

  const shuffledOffers = shuffle(offers);
  console.log(`🚀 Total de ofertas: ${shuffledOffers.length}`);

  const total = shuffledOffers.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  let myOffers = shuffledOffers.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));
  if (PROCESS_LIMIT) myOffers = myOffers.slice(0, PROCESS_LIMIT);

  console.log(`📂 Worker ${WORKER_INDEX} processando ${myOffers.length} ofertas (chunk size ${chunkSize})`);
  await processOffers(myOffers);

  console.log("✅ Worker finalizado");
  process.exit(0);
})();
