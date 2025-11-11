// Parte 1/3
#!/usr/bin/env node

/**
 * scraper_nulls.mjs
 *
 * Versão atualizada (mantém estrutura original):
 * - WAIT_TIME = 15000 ms
 * - MAX_FAILS = 5
 * - Somente devices/user-agents mobile
 * - Seletores múltiplos para capturar contagem
 * - Salva debug HTML quando não encontrar resultado
 * - Mantém retries, confirmação de bloqueio, soft-delete
 *
 * Salve como scraper_nulls.mjs (substitui o anterior).
 */

import fs from "fs";
import os from "os";
import path from "path";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;

const PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "3", 10)); // processa N por vez
const CONTEXT_POOL_SIZE = Math.max(1, parseInt(process.env.CONTEXT_POOL_SIZE || String(PARALLEL), 10));

const WAIT_TIME = parseInt(process.env.WAIT_TIME || "15000", 10); // aumentado pra 15s
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "90000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "15000", 10);
const RETRY_ATTEMPTS = Math.max(0, parseInt(process.env.RETRY_ATTEMPTS || "1", 10));

const DEBUG = String(process.env.DEBUG || "false").toLowerCase() === "true";
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase(); // info|warn|error|silent
const MAX_FAILS = parseInt(process.env.MAX_FAILS || "5", 10); // agora 5

const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug_nulls";

/**
 * Devices e user agents MOBILE apenas.
 * Mantemos alguns dispositivos representativos Android/iOS.
 */
const DEVICE_NAMES = [
  "iPhone 13 Pro Max",
  "iPhone 12",
  "Pixel 7",
  "Pixel 5",
  "Galaxy S21 Ultra",
];
const DEVICE_POOL = DEVICE_NAMES.map(n => devices[n]).filter(Boolean);

const USER_AGENTS = [
  // iOS Safari
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
  // Android Chrome
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

/** Parseador simples para extrair número de um texto */
function parseCountFromText(text) {
  if (!text) return null;
  // tenta formas com ~, com "resultados", com "results", com somente números
  let m = text.match(/~\s*([0-9.,]+)/) ||
          text.match(/([\d\.,\s\u00A0\u202F]+)\s*(?:resultados|results)/i) ||
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
  const blockers = [
    "captcha", "verify", "temporarily blocked", "unusual activity", "confirm you're human",
    "log in to facebook", "sign in to continue", "please enable javascript", "blocked",
    "access denied", "not available in your country"
  ];
  return blockers.some(k => ln.includes(k));
}

/**
 * Lista de seletores candidatos (mais abrangente).
 * Tentamos cada um até encontrar um texto com número.
 */
const CANDIDATE_SELECTORS = [
  'div[role="heading"][aria-level="3"]',
  'div[role="heading"]',
  'h3',
  'h2',
  'div:has-text("resultados")',
  'span:has-text("resultados")',
  'div:has-text("results")',
  'span:has-text("results")',
  'div[class*="counter"]',
  'div[class*="results"]',
  'span[class*="results"]',
];

// Parte 2/3

/** Create randomized browser context (mobile) */
async function createContext(browser) {
  const device = DEVICE_POOL[Math.floor(Math.random() * DEVICE_POOL.length)] || {};
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  // merge device but ensure userAgent override
  const contextOptions = { ...(device || {}), userAgent: ua };
  const ctx = await browser.newContext(contextOptions);
  // block heavy resources
  await ctx.route("**/*", route => {
    const t = route.request().resourceType();
    if (["image","stylesheet","font","media"].includes(t)) return route.abort();
    return route.continue();
  });
  ctx.__pagesProcessed = 0;
  return ctx;
}

/** Try extract once using candidate selectors & fallbacks */
async function attemptExtractFromPage(page, offer) {
  // primary attempts: try candidate selectors
  try {
    for (const sel of CANDIDATE_SELECTORS) {
      try {
        const locator = page.locator(sel);
        const txt = await locator.first().textContent({ timeout: SELECTOR_TIMEOUT }).catch(() => null);
        if (txt) {
          const parsed = parseCountFromText(txt);
          if (parsed != null) return { found: true, count: parsed, rawSelector: sel, raw: txt };
        }
      } catch (e) { /* continue */ }
    }
  } catch (e) {}

  // try frames
  try {
    for (const frame of page.frames()) {
      for (const sel of CANDIDATE_SELECTORS) {
        try {
          const ft = await frame.locator(sel).first().textContent().catch(() => null);
          if (ft) {
            const parsed = parseCountFromText(ft);
            if (parsed != null) return { found: true, count: parsed, rawSelector: sel, raw: ft, frame: true };
          }
        } catch (e) { /* ignore */ }
      }
    }
  } catch (e) {}

  // final fallback: evaluate and search innerText for numeric patterns
  try {
    const evalRes = await page.evaluate(() => {
      try { return document.body ? document.body.innerText : null; } catch (e) { return null; }
    });
    if (evalRes) {
      // heurística: procura linhas contendo "resultados" ou "results" + número
      const lines = String(evalRes).split("\n").map(l => l.trim()).filter(Boolean);
      for (const ln of lines) {
        if (/resultados|results|anúncio|anuncios|anúncios/i.test(ln)) {
          const parsed = parseCountFromText(ln);
          if (parsed != null) return { found: true, count: parsed, rawSelector: "body-text-line", raw: ln };
        }
      }
      // se nada, procura qualquer número grande plausível
      for (const ln of lines) {
        const parsed = parseCountFromText(ln);
        if (parsed != null && parsed > 0) return { found: true, count: parsed, rawSelector: "body-any-number", raw: ln };
      }
    }
  } catch (e) {}

  return { found: false };
}

/** Main per-worker processing */
async function processOffers(offersSlice) {
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox'] });
  const contexts = [];
  for (let i = 0; i < CONTEXT_POOL_SIZE; i++) contexts.push(await createContext(browser));

  try {
    for (let i = 0; i < offersSlice.length; i += PARALLEL) {
      const batch = offersSlice.slice(i, i + PARALLEL);
      logInfo(`Worker ${WORKER_INDEX} processing batch ${Math.floor(i/PARALLEL)+1} (${batch.length} items)`);
      await Promise.all(batch.map(async (offer) => {
        if (!offer || !offer.adLibraryUrl) {
          logWarn(`[${offer && offer.id}] skipping - no adLibraryUrl`);
          return;
        }

        const ctxIdx = Math.floor(Math.random() * contexts.length);
        const context = contexts[ctxIdx];
        const page = await context.newPage();

        try {
          logInfo(`[${offer.id}] visiting ${offer.adLibraryUrl}`);
          let resp = null;
          try {
            resp = await page.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
          } catch (e) {
            logWarn(`[${offer.id}] page.goto error: ${e && e.message}`);
          }

          // espera maior + jitter
          await page.waitForTimeout(WAIT_TIME + jitter(1200));

          // primeira tentativa de extração
          let result = await attemptExtractFromPage(page, offer);

          // coletar HTML e status se não achou
          let html = null;
          let statusCode = resp ? resp.status() : null;
          if (!result.found) {
            try { html = await page.content().catch(() => null); } catch (e) { html = null; }
          }

          const initialBlocked = (statusCode && [401,403,407,429].includes(statusCode)) || contentLooksBlocked(html);

          // Se parecer bloqueado, confirmar através de outro contexto ou reload
          let confirmedBlock = false;
          if (initialBlocked) {
            logWarn(`[${offer.id}] possible block (status=${statusCode}), confirming...`);
            if (contexts.length > 1) {
              const altIdx = (ctxIdx + 1) % contexts.length;
              const altPage = await contexts[altIdx].newPage();
              try {
                let resp2 = null;
                try { resp2 = await altPage.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }); } catch(e){}
                await altPage.waitForTimeout(WAIT_TIME + jitter(600));
                const evalRes2 = await attemptExtractFromPage(altPage, offer);
                const html2 = await altPage.content().catch(() => null);
                const status2 = resp2 ? resp2.status() : null;
                const blocked2 = (status2 && [401,403,407,429].includes(status2)) || contentLooksBlocked(html2);
                if (evalRes2.found) {
                  result = evalRes2;
                  logInfo(`[${offer.id}] alt context succeeded: ${result.count}`);
                } else if (blocked2 && initialBlocked) {
                  confirmedBlock = true;
                  logWarn(`[${offer.id}] block confirmed`);
                } else {
                  logInfo(`[${offer.id}] alt did not extract but not confirmed block`);
                }
              } catch (e) {
                logWarn(`[${offer.id}] alt context confirm error: ${e && e.message}`);
              } finally {
                try { await altPage.close(); } catch(e) {}
              }
            } else {
              // single context: reload e re-tentar
              try {
                await page.reload({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(()=>null);
                await page.waitForTimeout(WAIT_TIME + jitter(600));
                const r2 = await attemptExtractFromPage(page, offer);
                if (r2.found) { result = r2; logInfo(`[${offer.id}] reload extracted: ${result.count}`); }
                else {
                  const html2 = await page.content().catch(()=>null);
                  const status2 = resp && resp.status ? resp.status() : null;
                  if ((status2 && [401,403,407,429].includes(status2)) || contentLooksBlocked(html2)) {
                    confirmedBlock = true;
                    logWarn(`[${offer.id}] block confirmed after reload`);
                  }
                }
              } catch (e) { logWarn(`[${offer.id}] reload confirm error: ${e && e.message}`); }
            }

            if (confirmedBlock) {
              // Soft-delete decision if attempts threshold reached
              const currentAttempts = Number(offer.attempts ?? 0);
              const newAttempts = currentAttempts + 1;
              if (newAttempts >= MAX_FAILS) {
                logInfo(`💀 [${offer.id}] confirmed block & attempts=${newAttempts} >=${MAX_FAILS} -> soft-deleting`);
                try {
                  const { data, error } = await supabase
                    .from("swipe_file_offers")
                    .update({ activeAds: null, updated_at: nowIso(), status_updated: "soft_deleted", attempts: newAttempts, deleted_at: nowIso() })
                    .eq("id", offer.id);
                  if (error) {
                    logWarn(`[${offer.id}] DB soft-delete failed for blocked: ${error.message || JSON.stringify(error)} - falling back to update attempts/status`);
                    try {
                      const { data: f, error: fe } = await supabase
                        .from("swipe_file_offers")
                        .update({ activeAds: null, updated_at: nowIso(), status_updated: "blocked_ip", attempts: newAttempts })
                        .eq("id", offer.id);
                      if (fe) logWarn(`[${offer.id}] fallback update also failed: ${fe.message || JSON.stringify(fe)}`);
                      else logInfo(`[${offer.id}] fallback update succeeded (blocked_ip)`);
                    } catch (e) {
                      logWarn(`[${offer.id}] fallback exception: ${String(e?.message || e)}`);
                    }
                  } else {
                    logInfo(`[${offer.id}] soft-deleted (blocked) - rows=${(data||[]).length}`);
                  }
                } catch (e) {
                  logWarn(`[${offer.id}] exception during soft-delete (blocked): ${String(e?.message || e)}`);
                }
              } else {
                // mark blocked_ip and increment attempts
                try {
                  const { data, error } = await supabase
                    .from("swipe_file_offers")
                    .update({ activeAds: null, updated_at: nowIso(), status_updated: "blocked_ip", attempts: newAttempts })
                    .eq("id", offer.id);
                  if (error) logWarn(`[${offer.id}] DB update blocked_ip failed: ${error.message || JSON.stringify(error)}`);
                  else logInfo(`[${offer.id}] marked blocked_ip (attempts=${newAttempts})`);
                } catch (e) {
                  logWarn(`[${offer.id}] exception updating blocked_ip: ${String(e?.message || e)}`);
                }
              }
              try { if (!page.isClosed()) await page.close(); } catch (e) {}
              return;
            }
          } // end block confirm

          // retries if needed (não inclui attempt=0)
          for (let attempt = 1; attempt <= RETRY_ATTEMPTS && !result.found; attempt++) {
            logInfo(`[${offer.id}] retry attempt ${attempt}/${RETRY_ATTEMPTS}`);
            try {
              await page.reload({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(()=>null);
              await page.waitForTimeout(WAIT_TIME + jitter(600));
              result = await attemptExtractFromPage(page, offer);
              if (result.found) logInfo(`[${offer.id}] retry succeeded: ${result.count}`);
            } catch (e) {
              logWarn(`[${offer.id}] retry error: ${e && e.message}`);
            }
          }

// Parte 3/3

          const ts = nowIso();
          if (result.found) {
            const activeAds = result.count;
            logInfo(`✅ [${offer.id}] success, updating activeAds=${activeAds} (selector=${result.rawSelector || 'unknown'})`);
            try {
              const { data, error } = await supabase
                .from("swipe_file_offers")
                .update({ activeAds, updated_at: ts, status_updated: "success", attempts: 0 })
                .eq("id", offer.id);
              if (error) logWarn(`[${offer.id}] DB update success failed: ${error.message || JSON.stringify(error)}`);
              else logInfo(`[${offer.id}] DB updated success (rows=${(data||[]).length})`);
            } catch (e) {
              logWarn(`[${offer.id}] DB exception on success update: ${e && e.message}`);
            }
          } else {
            // Not found after retries => soft-delete if attempts threshold reached, else mark erro and increment attempts
            const currentAttempts = Number(offer.attempts ?? 0);
            const newAttempts = currentAttempts + 1;
            if (newAttempts >= MAX_FAILS) {
              logWarn(`💀 [${offer.id}] not found after retries & attempts=${newAttempts} >=${MAX_FAILS} -> soft-deleting`);
              try {
                const { data, error } = await supabase
                  .from("swipe_file_offers")
                  .update({ activeAds: null, updated_at: ts, status_updated: "soft_deleted", attempts: newAttempts, deleted_at: ts })
                  .eq("id", offer.id);
                if (error) {
                  logWarn(`[${offer.id}] DB soft-delete failed: ${error.message || JSON.stringify(error)} - falling back to update attempts/status`);
                  try {
                    const { data: f, error: fe } = await supabase
                      .from("swipe_file_offers")
                      .update({ activeAds: null, updated_at: ts, status_updated: "erro", attempts: newAttempts })
                      .eq("id", offer.id);
                    if (fe) logWarn(`[${offer.id}] fallback update also failed: ${fe.message || JSON.stringify(fe)}`);
                    else logInfo(`[${offer.id}] fallback update succeeded (erro)`);
                  } catch (e) {
                    logWarn(`[${offer.id}] fallback exception: ${String(e?.message || e)}`);
                  }
                } else {
                  logInfo(`[${offer.id}] soft-deleted (erro) - rows=${(data||[]).length}`);
                }
              } catch (e) {
                logWarn(`[${offer.id}] exception during soft-delete: ${String(e?.message || e)}`);
              }
            } else {
              // regular update: mark erro and increment attempts
              logWarn(`[${offer.id}] not found after retries -> setting activeAds=null, attempts=${newAttempts}`);
              try {
                const { data, error } = await supabase
                  .from("swipe_file_offers")
                  .update({ activeAds: null, updated_at: ts, status_updated: "erro", attempts: newAttempts })
                  .eq("id", offer.id);
                if (error) logWarn(`[${offer.id}] DB update final erro failed: ${error.message || JSON.stringify(error)}`);
                else logInfo(`[${offer.id}] DB updated erro & activeAds=null (rows=${(data||[]).length})`);
              } catch (e) {
                logWarn(`[${offer.id}] DB exception on final erro update: ${e && e.message}`);
              }
            }

            // Save debug HTML when not found to inspect later
            if (DEBUG) {
              try {
                const pageHtml = await page.content().catch(()=>null);
                if (pageHtml) {
                  const saved = await saveDebugHtml(pageHtml, offer.id, "no-counter-after-retries");
                  if (saved) logInfo(`[${offer.id}] debug HTML saved: ${saved}`);
                }
              } catch(e) {}
            }
          }
        } catch (err) {
          logError(`[${offer.id}] unexpected error: ${err && err.message}`);
          // increment attempts and mark error (best-effort)
          try {
            const currentAttempts = Number(offer.attempts ?? 0) + 1;
            const { data, error } = await supabase
              .from("swipe_file_offers")
              .update({ updated_at: nowIso(), status_updated: "error", attempts: currentAttempts })
              .eq("id", offer.id);
            if (error) logWarn(`[${offer.id}] DB update on exception failed: ${error.message || JSON.stringify(error)}`);
            else logInfo(`[${offer.id}] DB updated on exception (rows=${(data||[]).length})`);
          } catch (e) {
            logWarn(`[${offer.id}] DB exception when marking error: ${e && e.message}`);
          }
        } finally {
          try { if (!page.isClosed()) await page.close(); } catch(e) {}
          context.__pagesProcessed = (context.__pagesProcessed || 0) + 1;
          // recycle context if needed (simple)
          if (context.__pagesProcessed >= 50) {
            try { await context.close(); } catch(e) {}
            // replace with new context
            const idx = contexts.indexOf(context);
            try { contexts[idx] = await createContext(browser); } catch(e) { logWarn("recreate context failed:", e && e.message); }
          }
          await sleep(200 + jitter(300));
        }
      })); // end batch map

      await sleep(200 + jitter(200));
    } // end batches
  } finally {
    for (const c of contexts) try { await c.close(); } catch(e) {}
    try { await browser.close(); } catch(e) {}
  }
}

/** Entrypoint */
(async () => {
  logInfo(`scraper_nulls starting worker ${WORKER_INDEX}/${TOTAL_WORKERS} - ${WORKER_ID}`);
  if (DEBUG) ensureDebugDir();

  // fetch only NULL activeAds and not soft-deleted
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
  if (!Array.isArray(offersAll) || offersAll.length === 0) {
    logInfo("No NULL offers found - exiting.");
    process.exit(0);
  }

  const shuffled = shuffle(offersAll);
  const total = shuffled.length;
  const chunkSize = Math.ceil(total / TOTAL_WORKERS);
  const myOffers = shuffled.slice(WORKER_INDEX * chunkSize, Math.min((WORKER_INDEX + 1) * chunkSize, total));
  const work = PROCESS_LIMIT ? myOffers.slice(0, PROCESS_LIMIT) : myOffers;

  logInfo(`Worker ${WORKER_INDEX} processing ${work.length}/${total} offers (chunkSize ${chunkSize})`);
  if (work.length > 0) {
    await processOffers(work);
  }

  logInfo(`Worker ${WORKER_INDEX} finished`);
  process.exit(0);
})();
