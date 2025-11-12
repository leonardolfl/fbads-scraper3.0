/**
 * test_scraper_manual.mjs
 *
 * Agora lê do Supabase (tabela swipe_file_offers) apenas ofertas com activeAds IS NULL.
 * Para cada oferta:
 *  - Extrai número de anúncios ativos do Facebook Ad Library
 *  - Se encontrar, atualiza activeAds, status_updated='success', attempts=0, updated_at=now()
 *  - Se não encontrar, apenas loga aviso
 *
 * Pode rodar com múltiplos runners (TOTAL_WORKERS / WORKER_INDEX)
 */

import fs from "fs";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "2", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const WAIT_TIME = 15000;
const NAV_TIMEOUT = 90000;
const SELECTOR_TIMEOUT = 15000;
const DEBUG_DIR = "./debug_manual";

if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });

// apenas dispositivos MOBILE
const DEVICE_NAMES = ["iPhone 13 Pro Max", "Pixel 7", "Galaxy S21 Ultra"];
const DEVICE_POOL = DEVICE_NAMES.map(n => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
];

const CANDIDATE_SELECTORS = [
  'div[role="heading"][aria-level="3"]',
  'div[role="heading"]',
  'h3', 'h2',
  'div:has-text("resultados")',
  'span:has-text("resultados")',
  'div:has-text("results")',
  'span:has-text("results")',
  'div[class*="counter"]',
  'div[class*="results"]',
  'span[class*="results"]',
];

function parseCountFromText(text) {
  if (!text) return null;
  let m = text.match(/~\s*([0-9.,]+)/) ||
          text.match(/([\d\.,\s\u00A0\u202F]+)\s*(?:resultados|results|anúncios|anuncios)/i) ||
          text.match(/([0-9][0-9\.,\s\u00A0\u202F]{0,})/);
  if (m && m[1]) {
    const cleaned = m[1].replace(/[.,\s\u00A0\u202F]/g, "");
    const n = parseInt(cleaned, 10);
    return isNaN(n) ? null : n;
  }
  return null;
}

async function attemptExtract(page) {
  for (const sel of CANDIDATE_SELECTORS) {
    try {
      const txt = await page.locator(sel).first().textContent({ timeout: SELECTOR_TIMEOUT }).catch(() => null);
      if (txt) {
        const parsed = parseCountFromText(txt);
        if (parsed != null) return { found: true, count: parsed, selector: sel, raw: txt };
      }
    } catch {}
  }
  return { found: false };
}

(async () => {
  console.log(`🚀 Iniciando leitura do Supabase (worker ${WORKER_INDEX}/${TOTAL_WORKERS})`);

  const { data: offersAll, error } = await supabase
    .from("swipe_file_offers")
    .select("id, \"adLibraryUrl\", attempts")
    .is("activeAds", null)
    .is("deleted_at", null)
    .order("id", { ascending: true });

  if (error) {
    console.error("❌ Erro ao buscar ofertas:", error);
    process.exit(1);
  }

  if (!offersAll?.length) {
    console.log("✅ Nenhuma oferta com activeAds NULL encontrada.");
    process.exit(0);
  }

  // dividir entre runners
  const total = offersAll.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  const myOffers = offersAll.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));

  console.log(`🔹 Worker ${WORKER_INDEX} processando ${myOffers.length}/${total} ofertas`);

  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const context = await browser.newContext({
    ...(DEVICE_POOL[Math.floor(Math.random() * DEVICE_POOL.length)]),
    userAgent: USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]
  });
  const page = await context.newPage();

  for (const offer of myOffers) {
    const url = offer.adLibraryUrl;
    if (!url) {
      console.log(`⚠️ [${offer.id}] sem adLibraryUrl`);
      continue;
    }

    console.log(`\n🔗 [${offer.id}] Testando: ${url}`);
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
      await page.waitForTimeout(WAIT_TIME);
      const result = await attemptExtract(page);

      if (result.found) {
        console.log(`✅ [${offer.id}] ${result.count} anúncios encontrados (sel: ${result.selector})`);

        const { error: updateError } = await supabase
          .from("swipe_file_offers")
          .update({
            activeAds: result.count,
            status_updated: "success",
            attempts: 0,
            updated_at: new Date().toISOString()
          })
          .eq("id", offer.id);

        if (updateError) console.log(`⚠️ [${offer.id}] erro ao atualizar: ${updateError.message}`);
      } else {
        console.log(`⚠️ [${offer.id}] nenhum número encontrado`);
        const html = await page.content();
        await fs.promises.writeFile(`${DEBUG_DIR}/debug-${offer.id}.html`, html, "utf8");
      }

    } catch (e) {
      console.log(`💥 [${offer.id}] erro: ${e.message}`);
    }
  }

  await browser.close();
  console.log("\n✅ Worker finalizado com sucesso.");
})();
