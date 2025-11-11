import fs from "fs";
import path from "path";
import os from "os";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

/**
 * scraper.mjs - otimizado com fast-fetch + fallback Playwright
 * - Fast-path: fetch HTTP com timeout para extrair contador do HTML bruto.
 * - Fallback: Playwright com device pool + UA rotation, abort de recursos, retries.
 * - Mantém DB updates usando status_updated (sem alteração de schema).
 * - ENV:
 *   WORKER_INDEX, TOTAL_WORKERS
 *   PROCESS_LIMIT (opcional)
 *   PARALLEL (default 3)
 *   WAIT_TIME (ms) default 7000
 *   NAV_TIMEOUT (ms) default 60000
 *   FETCH_TIMEOUT_MS (ms) default 6000
 *   MAX_RETRIES (fallback attempts) default 3
 *   DEBUG_DIR default ./debug
 *   WORKER_ID optional
 */

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;
let PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "3", 10));
const WAIT_TIME = parseInt(process.env.WAIT_TIME || "7000", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);
const FETCH_TIMEOUT_MS = parseInt(process.env.FETCH_TIMEOUT_MS || "6000", 10);
const MAX_RETRIES = Math.max(1, parseInt(process.env.MAX_RETRIES || "3", 10));
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug";
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;

// Device pool and UA pool
const DEVICE_NAMES = [
  "iPhone 13 Pro Max",
  "iPhone 12",
  "iPhone 11 Pro",
  "Pixel 7",
  "Pixel 5",
  "Galaxy S21 Ultra",
  "iPad Mini",
  "Desktop Chrome",
];
const DEVICE_POOL = DEVICE_NAMES.map((n) => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; rv:122.0) Gecko/20100101 Firefox/122.0",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
];

// Helpers
function ensureDebugDir() {
  if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });
}
function nowIso() {
  return new Date().toISOString();
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

// Robust extraction: tries regex with multiple keywords and a fallback neighbor-scan
function extractCountFromText(text) {
  if (!text || typeof text !== "string") return null;
  const keywords = ["resultados", "results", "anúncios", "anuncios", "ads"];
  // primary regex: number + keyword
  const primary = new RegExp("(\\d{1,3}(?:[.,]\\d{3})*(?:[.,]\\d+)?)\\s*(?:" + keywords.join("|") + ")", "i");
  const m = text.match(primary);
  if (m && m[1]) {
    return normalizeNumberString(m[1]);
  }
  // secondary: keyword then number (e.g., 'results: 123')
  const secondary = new RegExp("(?:" + keywords.join("|") + ")[:\\s\\-]{0,8}(\\d{1,3}(?:[.,]\\d{3})*(?:[.,]\\d+)?)", "i");
  const m2 = text.match(secondary);
  if (m2 && m2[1]) {
    return normalizeNumberString(m2[1]);
  }
  // neighbor-scan: find any number token near a keyword (within 60 chars)
  const numTokenRegex = /\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?/g;
  for (const kw of keywords) {
    let idx = text.toLowerCase().indexOf(kw);
    while (idx >= 0) {
      const start = Math.max(0, idx - 60);
      const end = Math.min(text.length, idx + kw.length + 60);
      const slice = text.slice(start, end);
      const nums = slice.match(numTokenRegex);
      if (nums && nums.length) {
        // pick closest number to keyword by index distance
        let best = null;
        let bestDist = Infinity;
        for (const n of nums) {
          const nIdx = slice.indexOf(n);
          const dist = Math.abs(nIdx - (idx - start));
          if (dist < bestDist) {
            bestDist = dist;
            best = n;
          }
        }
        if (best) return normalizeNumberString(best);
      }
      idx = text.toLowerCase().indexOf(kw, idx + 1);
    }
  }
  // last resort: first reasonable number in text (avoid picking large unrelated numbers)
  const generalNums = text.match(numTokenRegex);
  if (generalNums && generalNums.length) {
    // pick the longest token under a massive threshold (max 7 digits)
    for (const n of generalNums) {
      const digitsOnly = n.replace(/[^\d]/g, "");
      if (digitsOnly.length <= 7) return normalizeNumberString(n);
    }
  }
  return null;
}

function normalizeNumberString(s) {
  if (!s || typeof s !== "string") return null;
  // If contains both '.' and ',', assume '.' thousands and ',' decimals? common in pt: '1.234' or '1.234,56'
  // Our target is integer counts; remove non-digits.
  const digits = s.replace(/[^\d]/g, "");
  if (!digits) return null;
  const n = parseInt(digits, 10);
  if (Number.isNaN(n)) return null;
  return n;
}

async function saveDebug(pageOrHtml, offerId, note = "") {
  try {
    ensureDebugDir();
    const ts = nowTs();
    const safe = `offer-${offerId}-${ts}`;
    const htmlPath = path.join(DEBUG_DIR, `${safe}.html`);
    const pngPath = path.join(DEBUG_DIR, `${safe}.png`);
    if (typeof pageOrHtml === "string") {
      await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + pageOrHtml, "utf8");
      return { htmlPath, pngPath: null };
    } else {
      // page object
      const content = await pageOrHtml.content();
      await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + content, "utf8");
      try {
        await pageOrHtml.screenshot({ path: pngPath, fullPage: true });
      } catch (e) {
        // ignore screenshot failures
      }
      return { htmlPath, pngPath };
    }
  } catch (err) {
    console.warn("❌ Falha ao salvar debug:", err?.message || err);
    return { htmlPath: null, pngPath: null };
  }
}

// FAST PATH: fetch HTML raw with timeout and UA
async function fastFetchExtract(url, ua) {
  try {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": ua,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      }
    }).catch((e) => { throw e; });
    clearTimeout(id);
    if (!res || !res.ok) return { ok: false, reason: `fetch_status_${res ? res.status : "nores"}` };
    const html = await res.text();
    const count = extractCountFromText(html);
    return { ok: !!count, count: count || null, html };
  } catch (err) {
    return { ok: false, reason: `fetch_error:${err?.name || err?.message || err}` };
  }
}

// Process offers slice in parallel batches
async function processOffers(offersSlice) {
  const browser = await chromium.launch({ headless: true });
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;
  for (let i = 0; i < offersSlice.length; i += PARALLEL) {
    const batch = offersSlice.slice(i, i + PARALLEL);
    console.log(`📦 Processando bloco: ${Math.floor(i / PARALLEL) + 1} (${batch.length} ofertas)`);

    await Promise.all(batch.map(async (offer, idxInBatch) => {
      if (!offer || !offer.adLibraryUrl) return;
      const globalIndex = i + idxInBatch;
      const { device, ua } = pickDeviceAndUA(globalIndex);

      // FAST PATH
      try {
        const fast = await fastFetchExtract(offer.adLibraryUrl, ua);
        if (fast.ok && fast.count != null) {
          const activeAds = fast.count;
          const updated_at = nowIso();
          console.log(`⚡ [FAST] [${offer.id}] ${offer.offerName || ""}: ${activeAds} anúncios (UA short: ${(ua || "").slice(0,60)})`);
          const { error } = await supabase
            .from("swipe_file_offers")
            .update({ activeAds, updated_at, status_updated: "success" })
            .eq("id", offer.id);
          if (error) console.warn(`[${offer.id}] DB update error (fast):`, error.message || error);
          consecutiveSuccess++;
          consecutiveFails = 0;
          return;
        } else {
          // fast path miss: continue to fallback
          console.log(`→ [FAST MISS] [${offer.id}] reason=${fast.reason || "no-match"} - fallback to Playwright`);
        }
      } catch (err) {
        console.warn(`⚠️ [${offer.id}] fast-fetch exception:`, err?.message || err);
      }

      // FALLBACK: Playwright
      const contextOptions = { ...(device || {}) };
      if (ua) contextOptions.userAgent = ua;
      const context = await browser.newContext(contextOptions);

      // Abort heavy resources
      await context.route("**/*", (route) => {
        const type = route.request().resourceType();
        if (["image", "stylesheet", "font", "media"].includes(type)) return route.abort();
        return route.continue();
      });

      const page = await context.newPage();
      let didSucceed = false;
      try {
        console.log(`⌛ [FALLBACK] [${offer.id}] Acessando (${device?.name || "custom"} / UA=${(ua || "").slice(0,60)}): ${offer.adLibraryUrl}`);
        await page.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch((e) => { throw e; });

        // retries for dynamic content
        let found = null;
        for (let attempt = 1; attempt <= MAX_RETRIES && !found; attempt++) {
          // Prefer visible text first
          const bodyText = await page.textContent("body").catch(() => "");
          found = extractCountFromText(bodyText || "");
          if (!found) {
            // fallback to HTML
            const html = await page.content().catch(() => "");
            found = extractCountFromText(html || "");
          }
          if (!found) {
            if (attempt < MAX_RETRIES) {
              console.log(`· [${offer.id}] fallback retry ${attempt}/${MAX_RETRIES} (waiting small)`);
              await page.waitForTimeout(1000 + Math.floor(Math.random() * 800));
            }
          }
        }

        const updated_at = nowIso();

        if (found != null) {
          const activeAds = found;
          console.log(`✅ [${offer.id}] ${offer.offerName || ""}: ${activeAds} anúncios (fallback)`);
          const { error } = await supabase
            .from("swipe_file_offers")
            .update({ activeAds, updated_at, status_updated: "success" })
            .eq("id", offer.id);
          if (error) console.warn(`[${offer.id}] DB update error (fallback):`, error.message || error);
          didSucceed = true;
          consecutiveSuccess++;
          consecutiveFails = 0;
        } else {
          console.warn(`❌ [${offer.id}] contador não encontrado (fallback) — salvando debug`);
          try {
            const dbg = await saveDebug(page, offer.id, "no-counter-fallback");
            console.log(`[${offer.id}] debug saved: ${dbg.htmlPath || "no-html"} ${dbg.pngPath || ""}`);
          } catch (e) {
            console.warn(`[${offer.id}] failed saving debug:`, e?.message || e);
          }
          await supabase
            .from("swipe_file_offers")
            .update({ activeAds: null, updated_at, status_updated: "no_counter" })
            .eq("id", offer.id)
            .catch((e) => console.warn(`[${offer.id}] DB update (no_counter) error:`, e?.message || e));
          consecutiveFails++;
        }
      } catch (err) {
        console.error(`🚫 [${offer.id}] Erro no fallback:`, err?.message || err);
        // Try to save debug HTML if possible
        try {
          const dbgHtml = await page.content().catch(() => null);
          if (dbgHtml) {
            const dbg = await saveDebug(dbgHtml, offer.id, `fallback-exception-${String(err?.message || err)}`);
            console.log(`[${offer.id}] debug saved after exception: ${dbg.htmlPath || "no-html"}`);
          }
        } catch (e) {
          console.warn(`[${offer.id}] falha ao salvar debug no exception:`, e?.message || e);
        }
        try {
          await supabase
            .from("swipe_file_offers")
            .update({ activeAds: null, updated_at: nowIso(), status_updated: "error" })
            .eq("id", offer.id);
        } catch (e) {
          console.warn(`[${offer.id}] DB update (error) failed:`, e?.message || e);
        }
        consecutiveFails++;
      } finally {
        try { await page.close(); } catch {}
        try { await context.close(); } catch {}
        await sleep(400 + jitter(900));
      }

      // adaptive per-offer behaviour done above
    }));

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
