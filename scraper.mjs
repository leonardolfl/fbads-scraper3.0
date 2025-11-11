/**
 * scraper.mjs
 * - Reinício do browser mid-run (configurável)
 * - Rotação de dispositivos + user-agents (fingerprints mobile)
 * - Fisher–Yates shuffle
 * - Mantém a lógica de atualização no Supabase e paralelismo básico
 *
 * Env vars suportadas:
 *  - PARALLEL (padrão 5)
 *  - WAIT_TIME (ms) (padrão 7000)
 *  - RESTART_AT_PERCENT (padrão 50)  -> reinicia quando processou >= X% do total
 *  - RESTART_ON_CONSEC_FAILS (padrão 8) -> reinicia se houver N falhas consecutivas
 *  - RESTART_BACKOFF_MS (padrão 10000) -> pausa entre fechar e abrir browser
 *  - PROCESS_LIMIT (opcional, para testes)
 *
 * Use: TOTAL_BATCHES/BATCH_ID sharding não incluído aqui (mas pode ser adicionado).
 */

import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

// ===== Configuráveis via ENV =====
const PARALLEL = parseInt(process.env.PARALLEL || "15", 10);
const WAIT_TIME = parseInt(process.env.WAIT_TIME || "7000", 10);
const RESTART_AT_PERCENT = parseInt(process.env.RESTART_AT_PERCENT || "50", 10); // reinicia ao atingir X% do total
const RESTART_ON_CONSEC_FAILS = parseInt(process.env.RESTART_ON_CONSEC_FAILS || "8", 10);
const RESTART_BACKOFF_MS = parseInt(process.env.RESTART_BACKOFF_MS || "10000", 10);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;

// ===== Device pool & UA pool (mais variedade de fingerprints) =====
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
  // mobile / desktop mix - realisitc mobile UAs preferidos
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
];

// ===== Helpers =====
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}

function randomDelay(min = 1500, max = 3500) {
  return sleep(randomBetween(min, max));
}

// Fisher–Yates shuffle (mais robusto)
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

// ===== Main =====
(async () => {
  console.log("🚀 Iniciando scraper com restart mid-run e rotação de devices...");

  let browser = await chromium.launch({ headless: true });
  let processedCount = 0;
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;

  // fetch offers
  const { data: offersRaw, error: offersError } = await supabase
    .from("swipe_file_offers")
    .select("*");

  if (offersError) {
    console.error("❌ Erro ao buscar ofertas:", offersError);
    try { await browser.close(); } catch {}
    process.exit(1);
  }

  if (!Array.isArray(offersRaw) || offersRaw.length === 0) {
    console.log("ℹ️ Nenhuma oferta encontrada.");
    await browser.close();
    process.exit(0);
  }

  const offersShuffled = fisherYatesShuffle(offersRaw);
  const offers = PROCESS_LIMIT ? offersShuffled.slice(0, PROCESS_LIMIT) : offersShuffled;

  const totalOffers = offers.length;
  console.log(`📦 Total de ofertas a processar: ${totalOffers} (PROCESS_LIMIT=${PROCESS_LIMIT || "none"})`);
  const restartThreshold = Math.ceil((RESTART_AT_PERCENT / 100) * totalOffers);
  console.log(`♻️ Restart threshold: ${RESTART_AT_PERCENT}% -> reiniciar quando processed >= ${restartThreshold}`);

  // Process in batches of PARALLEL
  for (let i = 0; i < offers.length; i += PARALLEL) {
    const batch = offers.slice(i, i + PARALLEL);
    console.log(`\n🧩 Bloco ${Math.floor(i / PARALLEL) + 1}: ${batch.length} ofertas (PARALLEL=${PARALLEL})`);

    // Process batch in parallel
    await Promise.all(
      batch.map(async (offer, idxInBatch) => {
        if (!offer?.adLibraryUrl) {
          console.warn(`⚠️ [${offer?.id}] Sem adLibraryUrl, pulando.`);
          return;
        }

        // Choose device+UA based on global index to vary more
        const globalIndex = i + idxInBatch;
        const { device, ua } = pickDeviceAndUA(globalIndex);

        // create context per offer (isolated) but with rotated fingerprint
        const contextOptions = { ...(device || {}) };
        if (ua) contextOptions.userAgent = ua;

        const context = await browser.newContext(contextOptions);
        // block heavy resources
        await context.route("**/*", (route) => {
          const type = route.request().resourceType();
          if (["image", "stylesheet", "font", "media"].includes(type)) route.abort();
          else route.continue();
        });

        const page = await context.newPage();
        console.log(`⌛ [${offer.id}] Acessando (${context._deviceName || device?.name || "custom"} / UA=${ua ? ua.slice(0, 60) + "..." : "n/a"}): ${offer.adLibraryUrl}`);

        let matched = false;

        try {
          await page.goto(offer.adLibraryUrl, { waitUntil: "networkidle", timeout: NAV_TIMEOUT });
          await page.waitForTimeout(WAIT_TIME);

          // small wait to let async rendering happen
          try {
            await page.waitForSelector('div:has-text("resultados"), div:has-text("results")', { timeout: 15000 });
          } catch {
            // ignore; fallback to content parsing
          }

          const html = await page.content();
          let match = html.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);

          if (!match) {
            const bodyText = await page.textContent("body");
            match = bodyText && bodyText.match(/(\d+(?:[.,]\d+)?)\s*(?:resultados|results)/i);
          }

          const updated_at = new Date().toISOString();

          if (match) {
            const activeAds = parseInt(String(match[1]).replace(/\./g, "").replace(/,/g, ""), 10);
            console.log(`✅ [${offer.id}] ${offer.offerName}: ${activeAds} anúncios`);

            const { data, error } = await supabase
              .from("swipe_file_offers")
              .update({ activeAds, updated_at, status_updated: "success" })
              .eq("id", offer.id);

            if (error) console.warn(`⚠️ [${offer.id}] Erro ao atualizar Supabase: ${error.message || error}`);
            matched = true;
            consecutiveSuccess++;
            consecutiveFails = 0;
          } else {
            console.warn(`❌ [${offer.id}] contador não encontrado`);
            const { error } = await supabase
              .from("swipe_file_offers")
              .update({ activeAds: null, updated_at, status_updated: "no_counter" })
              .eq("id", offer.id);

            if (error) console.warn(`⚠️ [${offer.id}] Erro ao atualizar Supabase (no_counter): ${error.message || error}`);
            consecutiveFails++;
            consecutiveSuccess = 0;
          }
        } catch (err) {
          console.error(`🚫 [${offer.id}] Erro ao processar: ${err?.message || err}`);
          consecutiveFails++;
          consecutiveSuccess = 0;

          try {
            await supabase
              .from("swipe_file_offers")
              .update({ activeAds: null, updated_at: new Date().toISOString(), status_updated: "error" })
              .eq("id", offer.id);
          } catch (e) {
            console.warn(`⚠️ [${offer.id}] Falha ao atualizar Supabase em erro: ${e?.message || e}`);
          }
        } finally {
          try { await page.close(); } catch {}
          try { await context.close(); } catch {}
          processedCount++;
          // small randomized delay to avoid strict rate
          await randomDelay(800, 1600);
        }
      })
    ); // end Promise.all batch

    // After batch checks: decide if need to restart browser
    console.log(`📊 Progresso: processed=${processedCount}/${totalOffers} | consecutiveFails=${consecutiveFails} | consecutiveSuccess=${consecutiveSuccess}`);

    // restart on percent threshold
    if (processedCount >= restartThreshold) {
      console.log(`♻️ Reiniciando browser por atingir ${RESTART_AT_PERCENT}% do total (processed=${processedCount})`);
      try {
        await browser.close();
      } catch (e) { console.warn("⚠️ Erro ao fechar browser anterior:", e?.message || e); }
      const backoff = RESTART_BACKOFF_MS + Math.floor(Math.random() * 3000);
      console.log(`⏳ Pausa de ${Math.round(backoff)}ms antes de iniciar novo browser`);
      await sleep(backoff);
      browser = await chromium.launch({ headless: true });
      // reset counters sensíveis a sessão
      consecutiveFails = 0;
      consecutiveSuccess = 0;
      // continue processing remaining batches
      continue;
    }

    // restart reactively on consecutive fails threshold
    if (consecutiveFails >= RESTART_ON_CONSEC_FAILS) {
      console.log(`♻️ Reiniciando browser por ${consecutiveFails} falhas consecutivas (threshold=${RESTART_ON_CONSEC_FAILS})`);
      try {
        await browser.close();
      } catch (e) { console.warn("⚠️ Erro ao fechar browser anterior:", e?.message || e); }
      const backoff = RESTART_BACKOFF_MS + Math.floor(Math.random() * 5000);
      console.log(`⏳ Pausa de ${Math.round(backoff)}ms antes de iniciar novo browser`);
      await sleep(backoff);
      browser = await chromium.launch({ headless: true });
      consecutiveFails = 0;
      consecutiveSuccess = 0;
      continue;
    }

    // adaptative parallel logic from original (keeps PARALLEL constant here; leave behavior minimal)
    // small longer pause occasionally to reduce throttling
    if ((i + PARALLEL) % 100 === 0) {
      const pause = Math.floor(Math.random() * 15000) + 30000;
      console.log(`⏸️ Pausa extra de ${(pause / 1000).toFixed(1)}s para reduzir risco de bloqueio`);
      await sleep(pause);
    }
  } // end for batches

  try { await browser.close(); } catch (e) {}
  console.log("✅ Scraper finalizado.");
  console.log(`📈 Processed: ${processedCount}/${totalOffers} | final consecutiveFails=${consecutiveFails}`);
  process.exit(0);
})();
