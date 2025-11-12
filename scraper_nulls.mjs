/**
 * scraper_nulls.mjs
 *
 * Atualizado:
 * - WAIT_TIME = 15000 ms
 * - MAX_FAILS = 5
 * - Apenas dispositivos e user-agents MOBILE
 * - Seletores múltiplos para garantir contagem
 * - Salva HTML de debug se não encontrar contador
 */

import fs from "fs";
import os from "os";
import path from "path";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;

const PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "3", 10));
const CONTEXT_POOL_SIZE = Math.max(1, parseInt(process.env.CONTEXT_POOL_SIZE || String(PARALLEL), 10));

const WAIT_TIME = parseInt(process.env.WAIT_TIME || "15000", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "90000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "15000", 10);
const RETRY_ATTEMPTS = Math.max(0, parseInt(process.env.RETRY_ATTEMPTS || "1", 10));

const DEBUG = String(process.env.DEBUG || "false").toLowerCase() === "true";
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
const MAX_FAILS = parseInt(process.env.MAX_FAILS || "5", 10);

const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug_nulls";

const DEVICE_NAMES = [
  "iPhone 13 Pro Max",
  "iPhone 12",
  "Pixel 7",
  "Pixel 5",
  "Galaxy S21 Ultra",
];
const DEVICE_POOL = DEVICE_NAMES.map(n => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
];

function nowIso() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }
function jitter(ms) { return Math.floor(Math.random() * ms); }
function shuffle(a) { const arr = a.slice(); for (let i = arr.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [arr[i], arr[j]] = [arr[j], arr[i]]; } return arr; }

function logInfo(...args) { if (["info"].includes(LOG_LEVEL)) console.log(...args); }
function logWarn(...args) { if (["info","warn"].includes(LOG_LEVEL)) console.warn(...args); }
function logError(...args) { if (["info","warn","error"].includes(LOG_LEVEL)) console.error(...args); }

function ensureDebugDir() { if (!DEBUG) return; if (!fs.existsSync(DEBUG_DIR)) fs.mkdirSync(DEBUG_DIR, { recursive: true }); }
async function saveDebugHtml(html, id, note = "") {
  if (!DEBUG) return null;
  try {
    ensureDebugDir();
    const file = path.join(DEBUG_DIR, `offer-${id}-${Date.now()}-${note.replace(/\s+/g,'_')}.html`);
    await fs.promises.writeFile(file, `<!-- ${note} -->\n` + html, "utf8");
    return file;
  } catch (e) {
    logWarn("saveDebugHtml error:", e && e.message);
    return null;
  }
}

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

function contentLooksBlocked(html) {
  if (!html) return false;
  const ln = html.toLowerCase();
  const blockers = ["captcha","verify","temporarily blocked","unusual activity","confirm you're human","log in to facebook","sign in to continue","please enable javascript","blocked"];
  return blockers.some(k => ln.includes(k));
}

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

async function createContext(browser) {
  const device = DEVICE_POOL[Math.floor(Math.random() * DEVICE_POOL.length)] || {};
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  const ctx = await browser.newContext({ ...(device||{}), userAgent: ua });
  await ctx.route("**/*", route => {
    const t = route.request().resourceType();
    if (["image","stylesheet","font","media"].includes(t)) return route.abort();
    return route.continue();
  });
  ctx.__pagesProcessed = 0;
  return ctx;
}

async function attemptExtractFromPage(page) {
  for (const sel of CANDIDATE_SELECTORS) {
    try {
      const txt = await page.locator(sel).first().textContent({ timeout: SELECTOR_TIMEOUT }).catch(() => null);
      if (txt) {
        const parsed = parseCountFromText(txt);
        if (parsed != null) return { found: true, count: parsed, rawSelector: sel, raw: txt };
      }
    } catch {}
  }
  return { found: false };
}

async function processOffers(offersSlice) {
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox'] });
  const contexts = [];
  for (let i = 0; i < CONTEXT_POOL_SIZE; i++) contexts.push(await createContext(browser));

  try {
    for (let i = 0; i < offersSlice.length; i += PARALLEL) {
      const batch = offersSlice.slice(i, i + PARALLEL);
      await Promise.all(batch.map(async (offer) => {
        if (!offer || !offer.adLibraryUrl) return;
        const ctx = contexts[Math.floor(Math.random() * contexts.length)];
        const page = await ctx.newPage();
        try {
          let resp = await page.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(()=>null);
          await page.waitForTimeout(WAIT_TIME + jitter(800));
          let result = await attemptExtractFromPage(page);
          let html = result.found ? null : await page.content().catch(()=>null);
          const blocked = (resp && [401,403,429].includes(resp.status())) || contentLooksBlocked(html);
          const ts = nowIso();

          if (result.found) {
            await supabase.from("swipe_file_offers")
              .update({ activeAds: result.count, updated_at: ts, status_updated: "success", attempts: 0 })
              .eq("id", offer.id);
          } else if (blocked) {
            const attempts = (offer.attempts ?? 0) + 1;
            const status = attempts >= MAX_FAILS ? "soft_deleted" : "blocked_ip";
            await supabase.from("swipe_file_offers")
              .update({
                activeAds: null,
                updated_at: ts,
                status_updated: status,
                attempts,
                ...(attempts >= MAX_FAILS ? { deleted_at: ts } : {})
              }).eq("id", offer.id);
          } else {
            const attempts = (offer.attempts ?? 0) + 1;
            const status = attempts >= MAX_FAILS ? "soft_deleted" : "erro";
            await supabase.from("swipe_file_offers")
              .update({
                activeAds: null,
                updated_at: ts,
                status_updated: status,
                attempts,
                ...(attempts >= MAX_FAILS ? { deleted_at: ts } : {})
              }).eq("id", offer.id);
          }

        } catch (e) {
          logWarn(`[${offer.id}] error: ${e.message}`);
        } finally {
          try { await page.close(); } catch {}
        }
      }));
    }
  } finally {
    for (const c of contexts) try { await c.close(); } catch {}
    try { await browser.close(); } catch {}
  }
}

(async () => {
  logInfo(`scraper_nulls starting worker ${WORKER_INDEX}/${TOTAL_WORKERS} - ${WORKER_ID}`);
  if (DEBUG) ensureDebugDir();

  const { data: offersAll, error } = await supabase
    .from("swipe_file_offers")
    .select("*")
    .is("activeAds", null)
    .is("deleted_at", null)
    .order("id", { ascending: true });

  if (error) {
    logError("Error fetching NULL offers:", error);
    process.exit(1);
  }
  if (!offersAll?.length) {
    logInfo("No NULL offers found - exiting.");
    process.exit(0);
  }

  const shuffled = shuffle(offersAll);
  const total = shuffled.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  const myOffers = shuffled.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));
  const work = PROCESS_LIMIT ? myOffers.slice(0, PROCESS_LIMIT) : myOffers;

  logInfo(`Worker ${WORKER_INDEX} processing ${work.length}/${total} offers`);
  await processOffers(work);

  logInfo(`Worker ${WORKER_INDEX} finished`);
  process.exit(0);
})();
