import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const device = devices["iPhone 13 Pro Max"];
const WAIT_TIME = 7000;
let PARALLEL = 5;
let consecutiveFails = 0;
let consecutiveSuccess = 0;

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; rv:122.0) Gecko/20100101 Firefox/122.0",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
];

function randomDelay(min = 1500, max = 3500) {
  return new Promise((res) =>
    setTimeout(res, Math.random() * (max - min) + min)
  );
}

function shuffle(array) {
  return array.sort(() => Math.random() - 0.5);
}

(async () => {
  const browser = await chromium.launch({ headless: true });

  const { data: offers, error } = await supabase
    .from("swipe_file_offers")
    .select("*");

  if (error) {
    console.error("❌ Erro ao buscar ofertas:", error);
    process.exit(1);
  }

  const shuffledOffers = shuffle(offers);
  console.log(`🚀 Total de ofertas: ${shuffledOffers.length}`);

  for (let i = 0; i < shuffledOffers.length; i += PARALLEL) {
    const batch = shuffledOffers.slice(i, i + PARALLEL);
    console.log(`📦 Bloco ${i / PARALLEL + 1}: ${batch.length} ofertas (PARALLEL=${PARALLEL})`);

    await Promise.all(
      batch.map(async (offer) => {
        if (!offer.adLibraryUrl) return;

        const randomUA = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
        const context = await browser.newContext({
          ...device,
          userAgent: randomUA,
        });

        const page = await context.newPage();
        console.log(`⌛ Acessando: ${offer.adLibraryUrl}`);

        try {
          await page.goto(offer.adLibraryUrl, {
            waitUntil: "networkidle",
            timeout: 60000,
          });
          await page.waitForTimeout(WAIT_TIME);

          const html = await page.content();
          let match = html.match(/(\d+(?:\.\d+)?)\s*(?:resultados|results)/i);

          if (!match) {
            const bodyText = await page.textContent("body");
            match = bodyText.match(/(\d+(?:\.\d+)?)\s*(?:resultados|results)/i);
          }

          const updated_at = new Date().toISOString();

          if (match) {
            const activeAds = parseInt(match[1].replace(/\./g, ""));
            console.log(`✅ ${offer.offerName}: ${activeAds} anúncios`);

            await supabase
              .from("swipe_file_offers")
              .update({
                activeAds,
                updated_at,
                status_updated: "success",
              })
              .eq("id", offer.id);

            consecutiveSuccess++;
            consecutiveFails = 0;
          } else {
            console.log(`❌ ${offer.offerName}: contador não encontrado`);
            await supabase
              .from("swipe_file_offers")
              .update({
                activeAds: null,
                updated_at,
                status_updated: "no_counter",
              })
              .eq("id", offer.id);
            consecutiveFails++;
          }
        } catch (err) {
          console.error(`⚠️ Erro ao processar ${offer.offerName}:`, err.message);
          consecutiveFails++;
          await supabase
            .from("swipe_file_offers")
            .update({
              activeAds: null,
              updated_at: new Date().toISOString(),
              status_updated: "error",
            })
            .eq("id", offer.id);
        } finally {
          await page.close();
          await context.close();
          await randomDelay();
        }
      })
    );

    if (consecutiveFails >= 3 && PARALLEL > 3) {
      PARALLEL = 30;
      console.log("⚠️ Reduzindo paralelismo para 3 (detecção de erros).");
    } else if (consecutiveSuccess >= 20 && PARALLEL < 5) {
      PARALLEL = 5;
      console.log("✅ Restaurando paralelismo para 5 (estável novamente).");
      consecutiveSuccess = 0;
    }

    if ((i + PARALLEL) % 100 === 0) {
      const pause = Math.floor(Math.random() * 15000) + 30000;
      console.log(`⏸️ Pausa de ${(pause / 1000).toFixed(1)}s para evitar bloqueio`);
      await new Promise((res) => setTimeout(res, pause));
    }
  }

  await browser.close();
  console.log("✅ Scraper finalizado com ajuste automático de paralelismo");
})();
