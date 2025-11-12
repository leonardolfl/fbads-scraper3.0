/**
 * test_scraper_manual.mjs
 * Testa manualmente scraping do número de anúncios do Facebook Ad Library.
 * Usa seletores expandidos, dispositivos mobile e salva HTML para debug.
 */

import fs from "fs";
import path from "path";
import { chromium, devices } from "playwright";

const WAIT_TIME = 15000;
const NAV_TIMEOUT = 90000;
const SELECTOR_TIMEOUT = 15000;

const DEBUG_DIR = "./debug_manual";
if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true });

const DEVICE_NAMES = ["iPhone 13 Pro Max", "Pixel 7", "Galaxy S21 Ultra"];
const DEVICE_POOL = DEVICE_NAMES.map(n => devices[n]).filter(Boolean);
const USER_AGENTS = [
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
];

const LINKS = [
  "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page&view_all_page_id=198128936727398",
  "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page&source=page-transparency-widget&view_all_page_id=149229165196898",
  "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page&source=page-transparency-widget&view_all_page_id=100991206258839"
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
  console.log(`🚀 Iniciando teste manual com ${LINKS.length} links...`);
  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
  const context = await browser.newContext({
    ...(DEVICE_POOL[Math.floor(Math.random() * DEVICE_POOL.length)]),
    userAgent: USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]
  });
  const page = await context.newPage();

  for (const url of LINKS) {
    console.log(`\n🔗 Testando: ${url}`);
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
      await page.waitForTimeout(WAIT_TIME);
      const result = await attemptExtract(page);

      if (result.found) {
        console.log(`✅ Encontrado: ${result.count} anúncios (seletor: ${result.selector})`);
      } else {
        console.log(`⚠️  Nenhum número encontrado — salvando HTML...`);
        const html = await page.content();
        const file = path.join(DEBUG_DIR, `debug-${Date.now()}.html`);
        await fs.promises.writeFile(file, html, "utf8");
        console.log(`💾 HTML salvo em ${file}`);
      }
    } catch (e) {
      console.log(`💥 Erro ao testar ${url}: ${e.message}`);
    }
  }

  await browser.close();
  console.log("\n✅ Teste manual finalizado.");
})();
