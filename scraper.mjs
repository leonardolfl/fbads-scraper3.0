/**
 * scraper.mjs
 * Versão melhorada:
 * - sharding via env: TOTAL_BATCHES, BATCH_ID, PROCESS_LIMIT
 * - Fisher–Yates shuffle
 * - pool de contexts (PARALLEL)
 * - retries com exponential backoff + jitter
 * - detecta possíveis bloqueios e salva debug (HTML + screenshot) em ./debug
 * - valida retorno do Supabase em updates/selects
 * - metrics e graceful shutdown
 *
 * Use: TOTAL_BATCHES=2 BATCH_ID=1 node scraper.mjs
 */

import fs from "fs";
import path from "path";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

// ====== CONFIGURAÇÃO (ajustáveis via env) ======
const DEFAULT_PARALLEL = parseInt(process.env.PARALLEL || "3", 10); // número de contexts/páginas simultâneas
const PARALLEL = Math.max(1, DEFAULT_PARALLEL);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "3", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);
const WAIT_AFTER_NAV_MS = parseInt(process.env.WAIT_AFTER_NAV_MS || "7000", 10);
const REFRESH_CONTEXT_EVERY = parseInt(process.env.REFRESH_CONTEXT_EVERY || "25", 10);
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug";
const TOTAL_BATCHES = Math.max(1, parseInt(process.env.TOTAL_BATCHES || "1", 10));
const BATCH_ID = Math.max(1, parseInt(process.env.BATCH_ID || "1", 10)); // 1-based
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;
const BACKOFF_BASE_MS = parseInt(process.env.BACKOFF_BASE_MS || "3000", 10);
const BLOCKED_BACKOFF_MS = parseInt(process.env.BLOCKED_BACKOFF_MS || "60000", 10);

// ====== Devices & UAs ======
const DEVICE_LIST = [
  "iPhone 13 Pro Max",
  "iPhone 12",
  "iPhone 11 Pro",
  "Pixel 7",
  "Pixel 5",
  "Galaxy S21 Ultra",
  "Galaxy Note 10",
  "iPad Mini",
  "Desktop Chrome",
].map((n) => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
];

// ====== Patterns para detectar bloqueio / login / captcha ======
const BLOCK_PATTERNS = [
  /captcha/i,
  /verify/i,
  /verify your/i,
  /access denied/i,
  /temporarily blocked/i,
  /sign in/i,
  /log in/i,
  /rate limit/i,
  /challenge/i,
  /blocked/i,
];

// ====== Helpers ======
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

function detectBlocked(html) {
  if (!html) return false;
  const low = html.slice(0, 20000); // analisar apenas começo (performance)
  return BLOCK_PATTERNS.some((p) => p.test(low));
}

async function saveDebug(page, offer, note) {
  try {
    ensureDebugDir();
    const ts = nowTs();
    const safeName = `offer-${offer.id}-${ts}`;
    const htmlPath = path.join(DEBUG_DIR, `${safeName}.html`);
    const pngPath = path.join(DEBUG_DIR, `${safeName}.png`);
    const content = await page.content();
    await fs.promises.writeFile(htmlPath, `<!-- ${note} -->\n` + content, "utf8");
    await page.screenshot({ path: pngPath, fullPage: true }).catch(() => {});
    console.log(`🧾 Debug salvo: ${htmlPath} , ${pngPath}`);
    return { htmlPath, pngPath };
  } catch (err) {
    console.warn("❌ Falha ao salvar debug:", err.message);
    return null;
  }
}

async function updateOfferDb(id, payload) {
  try {
    const { data, error } = await supabase.from("swipe_file_offers").update(payload).eq("id", id);
    if (error) {
      console.error(`❌ Erro Supabase.update para id=${id}:`, error.message || error);
      return { ok: false, error };
    }
    return { ok: true, data };
  } catch (err) {
    console.error(`❌ Exception em Supabase.update para id=${id}:`, err.message || err);
    return { ok: false, error: err };
  }
}

// ====== Main ======
(async () => {
  console.log("🚀 Scraper iniciado (sharding + debug).");
  console.log(`ENV: TOTAL_BATCHES=${TOTAL_BATCHES} BATCH_ID=${BATCH_ID} PROCESS_LIMIT=${PROCESS_LIMIT || "none"} PARALLEL=${PARALLEL}`);

  ensureDebugDir();

  const browser = await chromium.launch({ headless: true });

  // graceful shutdown
  let shuttingDown = false;
  const safeClose = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("🛑 Encerrando: fechando browser...");
    try { await browser.close(); } catch (e) {}
    console.log("🛑 Encerramento concluído.");
    process.exit(0);
  };
  process.on("SIGINT", safeClose);
  process.on("SIGTERM", safeClose);

  // Busca ofertas no Supabase
  const { data: offersRaw, error: offersError } = await supabase.from("swipe_file_offers").select("*");
  if (offersError) {
    console.error("❌ Erro ao buscar ofertas:", offersError);
    await browser.close();
    process.exit(1);
  }
  if (!Array.isArray(offersRaw) || offersRaw.length === 0) {
    console.log("ℹ️ Nenhuma oferta encontrada. Encerrando.");
    await browser.close();
    process.exit(0);
  }

  // Ordena por id para sharding determinístico
  const offersSorted = offersRaw.slice().sort((a, b) => {
    if (a.id == null) return 1;
    if (b.id == null) return -1;
    return String(a.id).localeCompare(String(b.id), undefined, { numeric: true });
  });

  // Shuffle (Fisher–Yates) para reduzir padrão e dividir carga
  const offersShuffled = fisherYatesShuffle(offersSorted);

  // Aplica sharding: index % TOTAL_BATCHES === (BATCH_ID - 1)
  const offersSharded = offersShuffled.filter((_, idx) => (idx % TOTAL_BATCHES) === (BATCH_ID - 1));

  // Apply PROCESS_LIMIT if set
  const offersToProcess = PROCESS_LIMIT ? offersSharded.slice(0, PROCESS_LIMIT) : offersSharded;

  console.log(`📦 Total ofertas originais: ${offersRaw.length}`);
  console.log(`🔀 Após shuffle: ${offersShuffled.length}`);
  console.log(`📂 Batch ${BATCH_ID}/${TOTAL_BATCHES}: ${offersToProcess.length} ofertas a processar`);

  // Metrics
  let processedCount = 0;
  let successCount = 0;
  let failedCount = 0;
  let pendingRetryCount = 0;
  let blockedEvents = 0;

  // Create context pool
  async function createContextForIndex(browser, idx) {
    const device = DEVICE_LIST[idx % DEVICE_LIST.length] || {};
    const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    const contextOptions = { ...(device || {}) };
    if (ua) contextOptions.userAgent = ua;
    // route to abort heavy resources
    const ctx = await browser.newContext(contextOptions);
    await ctx.route("**/*", (route) => {
      const type = route.request().resourceType();
      if (["image", "stylesheet", "font", "media"].includes(type)) route.abort();
      else route.continue();
    });
    ctx._deviceName = device?.name || "generic";
    ctx._ua = ua;
    return ctx;
  }

  // Build initial pool
  let contextPool = [];
  for (let i = 0; i < Math.min(PARALLEL, offersToProcess.length); i++) {
    contextPool.push(await createContextForIndex(browser, i));
  }

  // Function to refresh pool
  async function refreshPool() {
    console.log("♻️ Renovando context pool...");
    for (const c of contextPool) { try { await c.close(); } catch {} }
    contextPool = [];
    for (let i = 0; i < Math.min(PARALLEL, offersToProcess.length); i++) {
      contextPool.push(await createContextForIndex(browser, i));
    }
  }

  // Process in batches of PARALLEL
  for (let i = 0; i < offersToProcess.length; i += PARALLEL) {
    if (shuttingDown) break;

    const batch = offersToProcess.slice(i, i + PARALLEL);
    console.log(`\n🧩 Bloco ${Math.floor(i / PARALLEL) + 1}: ${batch.length} ofertas (PARALLEL=${contextPool.length})`);

    // Map each offer to a worker using round-robin contexts
    await Promise.all(batch.map(async (offer, idxInBatch) => {
      if (!offer?.adLibraryUrl) {
        console.log(`⚠️ [${offer?.id}] Sem adLibraryUrl, pulando.`);
        return;
      }

      const poolIndex = idxInBatch % contextPool.length;
      let ctx = contextPool[poolIndex];

      let succeeded = false;
      let attempt = 0;

      while (attempt < MAX_RETRIES && !succeeded && !shuttingDown) {
        attempt++;
        const page = await ctx.newPage();
        const delayAfterNav = WAIT_AFTER_NAV_MS + jitter(3000);

        console.log(`🌐 [${offer.id}] Tentativa ${attempt} → ${offer.adLibraryUrl}`);
        console.log(`📱 Context: ${ctx._deviceName} UA: ${ctx._ua}`);

        try {
          await page.goto(offer.adLibraryUrl, { waitUntil: "networkidle", timeout: NAV_TIMEOUT });
          await page.waitForTimeout(delayAfterNav);

          // Try waiting for common result text render (short timeout)
          try {
            await page.waitForSelector('div:has-text("resultados"), div:has-text("results")', { timeout: 15000 });
          } catch {
            // continue, we'll fallback to content search
          }

          const html = await page.content();
          if (detectBlocked(html)) {
            blockedEvents++;
            console.warn(`⚠️ [${offer.id}] Possível bloqueio detectado (pattern). Salvando debug e aplicando backoff.`);
            await saveDebug(page, offer, `blocked-detection-attempt-${attempt}`);
            // Reduce parallelism reaction: refresh pool and pause
            await refreshPool();
            console.log(`⏳ Pausa longa de ${(BLOCKED_BACKOFF_MS / 1000).toFixed(1)}s devido a bloqueio detectado`);
            await sleep(BLOCKED_BACKOFF_MS + jitter(5000));
            // recreate ctx for this slot
            ctx = contextPool[poolIndex];
            // mark attempt as failed and retry (if attempts left)
            await page.close();
            continue;
          }

          // Look for the counter in HTML first, then body text
          let match = html.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);
          if (!match) {
            const bodyText = await page.textContent("body");
            match = bodyText && bodyText.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);
          }

          const updated_at = new Date().toISOString();

          if (match) {
            const activeAds = parseInt(String(match[1]).replace(/\./g, "").replace(/,/g, ""), 10);
            console.log(`✅ [${offer.id}] ${offer.offerName}: ${activeAds} anúncios`);

            const { ok, error } = await updateOfferDb(offer.id, {
              activeAds,
              updated_at,
              status_updated: "success",
            });

            if (!ok) {
              console.warn(`⚠️ [${offer.id}] Update DB falhou: ${error?.message || error}`);
            }

            succeeded = true;
            successCount++;
            processedCount++;
          } else {
            // No match found
            console.warn(`❌ [${offer.id}] contador não encontrado (tentativa ${attempt})`);
            await saveDebug(page, offer, `no-counter-attempt-${attempt}`);
            if (attempt >= MAX_RETRIES) {
              pendingRetryCount++;
              failedCount++;
              processedCount++;
              const { ok: ok2 } = await updateOfferDb(offer.id, {
                activeAds: null,
                updated_at,
                status_updated: "no_counter",
              });
              if (!ok2) console.warn(`⚠️ [${offer.id}] Update DB (no_counter) possivelmente falhou.`);
            } else {
              // backoff before next attempt
              const backoffMs = BACKOFF_BASE_MS * Math.pow(2, attempt - 1) + jitter(1000);
              console.log(`⏳ Backoff ${Math.round(backoffMs)}ms antes da próxima tentativa para [${offer.id}]`);
              await page.close();
              await sleep(backoffMs);
              // for retry > 1, we recreate a fresh context for this slot to vary fingerprint
              try { await ctx.close(); } catch {}
              ctx = await createContextForIndex(browser, Math.floor(Math.random() * DEVICE_LIST.length));
              contextPool[poolIndex] = ctx;
              continue;
            }
          }
        } catch (err) {
          console.error(`🚫 [${offer.id}] Erro na tentativa ${attempt}:`, err.message || err);
          // save debug
          try { await saveDebug(page, offer, `exception-${attempt}-${(err && err.message) || ""}`); } catch {}
          if (attempt >= MAX_RETRIES) {
            failedCount++;
            processedCount++;
            const { ok: ok3 } = await updateOfferDb(offer.id, {
              activeAds: null,
              updated_at: new Date().toISOString(),
              status_updated: "error",
            });
            if (!ok3) console.warn(`⚠️ [${offer.id}] Update DB (error) possivelmente falhou.`);
          } else {
            const backoffMs = BACKOFF_BASE_MS * Math.pow(2, attempt - 1) + jitter(1000);
            console.log(`⏳ Backoff ${Math.round(backoffMs)}ms após erro para [${offer.id}]`);
            await page.close();
            await sleep(backoffMs);
            // recreate context for this slot
            try { await ctx.close(); } catch {}
            ctx = await createContextForIndex(browser, Math.floor(Math.random() * DEVICE_LIST.length));
            contextPool[poolIndex] = ctx;
            continue;
          }
        } finally {
          try { await page.close(); } catch {}
        }
      } // end attempts

      if (!succeeded && !shuttingDown && attempt >= MAX_RETRIES) {
        console.log(`ℹ️ [${offer.id}] Marcado como pending_retry após ${attempt} tentativas.`);
      }
    })); // end Promise.all batch

    // Periodic refresh
    if (((i / PARALLEL) + 1) % REFRESH_CONTEXT_EVERY === 0) {
      await refreshPool();
    }

    // small randomized pause between batches to reduce rate
    const pauseMs = 800 + jitter(1200);
    await sleep(pauseMs);
  } // end for batches

  // Cleanup
  for (const c of contextPool) {
    try { await c.close(); } catch {}
  }

  try { await browser.close(); } catch {}

  // Summary
  console.log("\n✅ Execução finalizada.");
  console.log(`📈 Processed: ${processedCount}`);
  console.log(`✔️ Success: ${successCount}`);
  console.log(`⚠️ No counter/pending retries: ${pendingRetryCount}`);
  console.log(`❌ Failures: ${failedCount}`);
  console.log(`🔒 Blocked events detected: ${blockedEvents}`);
  console.log(`🗂 Debug files (se houver): ${path.resolve(DEBUG_DIR)}`);

  process.exit(0);
})();
