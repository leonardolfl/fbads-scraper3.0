import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

/**
 * Versão atualizada do scraper.mjs
 * - Reinicia o browser mid-run em dois gatilhos configuráveis:
 *   * RESTART_AT_PERCENT: reinicia quando processou >= X% do total
 *   * RESTART_ON_CONSEC_FAILS: reinicia se houver N falhas consecutivas
 * - Rotaciona dispositivos Playwright + user-agents por contexto
 * - Fisher–Yates shuffle para embaralhamento
 * - Variáveis configuráveis via ENV: PARALLEL, WAIT_TIME, RESTART_AT_PERCENT, RESTART_ON_CONSEC_FAILS, RESTART_BACKOFF_MS, PROCESS_LIMIT
 *
 * Uso (exemplos):
 *  PARALLEL=5 RESTART_AT_PERCENT=50 RESTART_ON_CONSEC_FAILS=8 node scraper.mjs
 */

// ===== Configurações (podem vir do env) =====
const PARALLEL = parseInt(process.env.PARALLEL || "5", 10);
const WAIT_TIME = parseInt(process.env.WAIT_TIME || "7000", 10);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "2", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "60000", 10);

const RESTART_AT_PERCENT = parseInt(process.env.RESTART_AT_PERCENT || "50", 10); // reiniciar quando processar >= X%
const RESTART_ON_CONSEC_FAILS = parseInt(process.env.RESTART_ON_CONSEC_FAILS || "8", 10); // reiniciar se N falhas consecutivas
const RESTART_BACKOFF_MS = parseInt(process.env.RESTART_BACKOFF_MS || "10000", 10); // pausa entre fechar/abrir browser
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;

// ===== Pool de devices e user-agents =====
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
  // mobile-first realistic UAs + some desktop fallback
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
];

// ===== Helpers utilitários =====
function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}

function randomDelay(min = 1500, max = 3500) {
  return sleep(randomBetween(min, max));
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
  const device = DEVICE_POOL.length ? DEVICE_POOL[index % DEVICE_POOL.length] : {};
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  return { device, ua };
}

// ===== Main =====
(async () => {
  console.log("🚀 Iniciando scraper (reinício mid-run ativado).");
  console.log(`Config: PARALLEL=${PARALLEL}, WAIT_TIME=${WAIT_TIME}ms, RESTART_AT_PERCENT=${RESTART_AT_PERCENT}%, RESTART_ON_CONSEC_FAILS=${RESTART_ON_CONSEC_FAILS}, RESTART_BACKOFF_MS=${RESTART_BACKOFF_MS}ms, PROCESS_LIMIT=${PROCESS_LIMIT || "none"}`);

  // iniciar browser
  let browser = await chromium.launch({ headless: true });

  // fetch ofertas
  const { data: offersRaw, error: offersError } = await supabase
    .from("swipe_file_offers")
    .select("*");

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

  // shuffle + limit
  const offersShuffled = fisherYatesShuffle(offersRaw);
  const offers = PROCESS_LIMIT ? offersShuffled.slice(0, PROCESS_LIMIT) : offersShuffled;
  const totalOffers = offers.length;
  const restartThreshold = Math.ceil((RESTART_AT_PERCENT / 100) * totalOffers);

  console.log(`📦 Total de ofertas a processar: ${totalOffers} (restartThreshold=${restartThreshold})`);

  // counters
  let processedCount = 0;
  let consecutiveFails = 0;
  let consecutiveSuccess = 0;

  // graceful shutdown
  let shuttingDown = false;
  async function safeCloseBrowser() {
    if (!browser) return;
    try { await browser.close(); } catch (e) { /* ignore */ }
    browser = null;
  }
  async function handleShutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("🛑 Recebido sinal de encerramento. Fechando browser...");
    await safeCloseBrowser();
    process.exit(0);
  }
  process.on("SIGINT", handleShutdown);
  process.on("SIGTERM", handleShutdown);

  // main loop em blocos
  for (let i = 0; i < offers.length; i += PARALLEL) {
    if (shuttingDown) break;

    const batch = offers.slice(i, i + PARALLEL);
    console.log(`\n🧩 Bloco ${Math.floor(i / PARALLEL) + 1}: ${batch.length} ofertas (PARALLEL=${PARALLEL})`);

    // processar bloco em paralelo
    await Promise.all(batch.map(async (offer, idxInBatch) => {
      if (!offer?.adLibraryUrl) {
        console.warn(`⚠️ [${offer?.id}] Sem adLibraryUrl — pulando.`);
        return;
      }

      const globalIndex = i + idxInBatch;
      const { device, ua } = pickDeviceAndUA(globalIndex);

      // criar contexto com device + UA
      const contextOptions = { ...(device || {}) };
      if (ua) contextOptions.userAgent = ua;

      let context;
      let page;
      try {
        context = await browser.newContext(contextOptions);
        await context.route("**/*", (route) => {
          const type = route.request().resourceType();
          if (["image", "stylesheet", "font", "media"].includes(type)) route.abort();
          else route.continue();
        });

        page = await context.newPage();
        console.log(`⌛ [${offer.id}] Acessando (device=${device?.name || "custom"}, ua=${ua.slice(0, 60)}...): ${offer.adLibraryUrl}`);

        // navegação e wait
        await page.goto(offer.adLibraryUrl, { waitUntil: "networkidle", timeout: NAV_TIMEOUT });
        await page.waitForTimeout(WAIT_TIME);

        // tentar esperar por seletor que geralmente mostra resultados
        try {
          await page.waitForSelector('div:has-text("resultados"), div:has-text("results")', { timeout: 15000 });
        } catch {
          // fallback para leitura do conteúdo
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

          if (error) {
            console.warn(`⚠️ [${offer.id}] Erro ao atualizar Supabase: ${error.message || error}`);
          }

          consecutiveSuccess++;
          consecutiveFails = 0;
        } else {
          console.warn(`❌ [${offer.id}] contador não encontrado`);
          const { error } = await supabase
            .from("swipe_file_offers")
            .update({ activeAds: null, updated_at, status_updated: "no_counter" })
            .eq("id", offer.id);

          if (error) {
            console.warn(`⚠️ [${offer.id}] Erro ao atualizar Supabase (no_counter): ${error.message || error}`);
          }

          consecutiveFails++;
          consecutiveSuccess = 0;
        }
      } catch (err) {
        console.error(`🚫 [${offer.id}] Erro ao processar: ${err?.message || err}`);
        try {
          await supabase
            .from("swipe_file_offers")
            .update({ activeAds: null, updated_at: new Date().toISOString(), status_updated: "error" })
            .eq("id", offer.id);
        } catch (e) {
          console.warn(`⚠️ [${offer.id}] Falha ao atualizar Supabase após erro: ${e?.message || e}`);
        }
        consecutiveFails++;
        consecutiveSuccess = 0;
      } finally {
        try { if (page) await page.close(); } catch {}
        try { if (context) await context.close(); } catch {}
        processedCount++;
        // small randomized delay to spread requests
        await randomDelay(800, 1600);
      }
    })); // end Promise.all batch

    // logs de progresso
    console.log(`📊 Progresso: processed=${processedCount}/${totalOffers} | consecutiveFails=${consecutiveFails} | consecutiveSuccess=${consecutiveSuccess}`);

    // gatilho 1: reiniciar quando processed >= restartThreshold (percentual do total)
    if (processedCount >= restartThreshold) {
      console.log(`♻️ Reiniciando browser: atingido ${RESTART_AT_PERCENT}% do total (processed=${processedCount})`);
      try { await browser.close(); } catch (e) { console.warn("⚠️ Erro ao fechar browser:", e?.message || e); }
      const backoff = RESTART_BACKOFF_MS + Math.floor(Math.random() * 3000);
      console.log(`⏳ Pausa de ${Math.round(backoff)}ms antes de novo browser`);
      await sleep(backoff);
      browser = await chromium.launch({ headless: true });
      consecutiveFails = 0;
      consecutiveSuccess = 0;
      // continue implicitamente para próximos blocos
      continue;
    }

    // gatilho 2: reiniciar reativamente se muitas falhas consecutivas
    if (consecutiveFails >= RESTART_ON_CONSEC_FAILS) {
      console.log(`♻️ Reiniciando browser: ${consecutiveFails} falhas consecutivas (threshold=${RESTART_ON_CONSEC_FAILS})`);
      try { await browser.close(); } catch (e) { console.warn("⚠️ Erro ao fechar browser:", e?.message || e); }
      const backoff = RESTART_BACKOFF_MS + Math.floor(Math.random() * 5000);
      console.log(`⏳ Pausa de ${Math.round(backoff)}ms antes de novo browser`);
      await sleep(backoff);
      browser = await chromium.launch({ headless: true });
      consecutiveFails = 0;
      consecutiveSuccess = 0;
      continue;
    }

    // pausa ocasional para reduzir taxa (igual lógica original)
    if ((i + PARALLEL) % 100 === 0) {
      const pause = Math.floor(Math.random() * 15000) + 30000;
      console.log(`⏸️ Pausa extra de ${(pause / 1000).toFixed(1)}s para reduzir risco de bloqueio`);
      await sleep(pause);
    }
  } // end for batches

  // final cleanup
  try { await browser.close(); } catch (e) {}
  console.log("✅ Scraper finalizado.");
  console.log(`📈 Totais: processed=${processedCount}/${totalOffers} | final consecutiveFails=${consecutiveFails}`);
  process.exit(0);
})();
