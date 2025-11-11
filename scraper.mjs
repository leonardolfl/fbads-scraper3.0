import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const device = devices["iPhone 13 Pro Max"];
const WAIT_TIME = 7000; // tempo para renderização
const PARALLEL = 5; // quantidade de abas simultâneas

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext(device);

  // Pega todas as ofertas do Supabase
  const { data: offers, error } = await supabase
    .from("swipe_file_offers")
    .select("*");

  if (error) {
    console.error("❌ Erro ao buscar ofertas:", error);
    process.exit(1);
  }

  console.log(`🚀 Total de ofertas: ${offers.length}`);

  // Processa em blocos de 5
  for (let i = 0; i < offers.length; i += PARALLEL) {
    const batch = offers.slice(i, i + PARALLEL);

    console.log(`📦 Processando bloco de ${batch.length} ofertas em paralelo`);

    await Promise.all(batch.map(async (offer) => {
      if (!offer.adLibraryUrl) return;

      const page = await context.newPage();
      console.log(`⌛ Acessando: ${offer.adLibraryUrl}`);
      try {
        await page.goto(offer.adLibraryUrl, { waitUntil: "networkidle", timeout: 60000 });
        await page.waitForTimeout(WAIT_TIME);

        const html = await page.content();
        let match = html.match(/(\d+(?:\.\d+)?)\s*(?:resultados|results)/i);

        if (!match) {
          const bodyText = await page.textContent("body");
          match = bodyText.match(/(\d+(?:\.\d+)?)\s*(?:resultados|results)/i);
        }

        if (match) {
          const activeAds = parseInt(match[1].replace(/\./g, ""));
          console.log(`✅ ${offer.offerName}: ${activeAds} anúncios`);

          await supabase
            .from("swipe_file_offers")
            .update({ activeAds, dateAdded: new Date() })
            .eq("id", offer.id);
        } else {
          console.log(`❌ ${offer.offerName}: não foi possível obter o contador`);
        }
      } catch (err) {
        console.error(`⚠️ Erro ao processar ${offer.offerName}:`, err.message);
      } finally {
        await page.close();
      }
    }));
  }

  await browser.close();
  console.log("✅ Scraper finalizado (com 5 abas paralelas)");
})();
