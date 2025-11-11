#!/usr/bin/env node

/**
 * scraper_nulls.mjs
 *
 * Processa apenas ofertas com activeAds IS NULL e deleted_at IS NULL.
 * - Distribui trabalho entre TOTAL_WORKERS (default 4) via WORKER_INDEX.
 * - Cada worker processa blocos com PARALLEL (default 3).
 * - Tempos maiores para renderização (WAIT_TIME default 7000ms).
 * - RETRY_ATTEMPTS default 1.
 *
 * Soft-delete behavior:
 * - Quando, após uma tentativa (retry) numa execução, a extração NÂO obtiver resultado,
 *   incrementa attempts. Se attempts >= 3 -> marca deleted_at = now(), status_updated='soft_deleted'.
 * - Mesmo comportamento aplicado quando confirmar bloqueio.
 *
 * Uso sugerido (GitHub Actions job para 4 workers):
 *   TOTAL_WORKERS=4 WORKER_INDEX=<0..3> PARALLEL=3 node scraper_nulls.mjs
 *
 * Requer: supabase.js exportando `supabase` (cliente) no mesmo diretório.
 */

import fs from "fs";
import os from "os";
import path from "path";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PROCESS_LIMIT = process.env.PROCESS_LIMIT ? parseInt(process.env.PROCESS_LIMIT, 10) : null;

const PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "3", 10)); // processa 3 por vez
const CONTEXT_POOL_SIZE = Math.max(1, parseInt(process.env.CONTEXT_POOL_SIZE || String(PARALLEL), 10));

const WAIT_TIME = parseInt(process.env.WAIT_TIME || "7000", 10); // mais tempo para renderização
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "90000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "15000", 10);
const RETRY_ATTEMPTS = Math.max(0, parseInt(process.env.RETRY_ATTEMPTS || "1", 10));

const DEBUG = String(process.env.DEBUG || "false").toLowerCase() === "true";
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase(); // info|warn|error|silent
const MAX_FAILS = parseInt(process.env.MAX_FAILS || "3", 10);

const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug_nulls";

const DEVICE_NAMES = [
  "Desktop Chrome",
  "iPhone 13 Pro Max",
  "Pixel 7",
  "iPad Mini",
  "Pixel 5",
  "Galaxy S21 Ultra",
  "iPhone 12",
];
const DEVICE_POOL = DEVICE_NAMES.map(n => devices[n]).filter(Boolean);

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
    const file = path.join(DEBUG_DIR, `offer-${id}-${Date.now()}.html`);
    await fs.promises.writeFile(file, `<!-- ${note} -->\n` + html, "utf8");
    return file;
  } catch (e) {
    logWarn("saveDebugHtml error:", e && e.message);
    return null;
  }
}

/** Simple parser copied from extension */
function parseCountFromText(text) {
  if (!text) return null;
  let m = text.match(/~\s*([0-9.,]+)/) || text.match(/([\d\.,\s\u00A0\u202F]+)\s*(?:resultados|results)/i);
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

/** Create randomized browser context */
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

/** Try extract once using heading & fallback evaluate */
async function attemptExtractFromPage(page, offer) {
  try {
    const txt = await page.locator('div[role="heading"][aria-level="3"]').first().textContent({ timeout: SELECTOR_TIMEOUT }).catch(() => null);
    if (txt) {
      const parsed = parseCountFromText(txt);
      if (parsed != null) return { found: true, count: parsed, raw: txt };
    }
  } catch (e) {}

  try {
    for (const frame of page.frames()) {
      const ft = await frame.locator('div[role="heading"][aria-level="3"]').first().textContent().catch(() => null);
      if (ft) {
        const parsed = parseCountFromText(ft);
        if (parsed != null) return { found: true, count: parsed, raw: ft };
      }
    }
  } catch (e) {}

  try {
    const evalRes = await page.evaluate(() => {
      try {
        const el = document.querySelector('div[role="heading"][aria-level="3"]');
        const txt = el ? (el.innerText || "") : null;
        return txt;
      } catch (e) { return null; }
    });
    if (evalRes) {
      const p = parseCountFromText(evalRes);
      if (p != null) return { found: true, count: p, raw: evalRes };
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

        // pick a context
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

          await page.waitForTimeout(WAIT_TIME + jitter(800));

          let result = await attemptExtractFromPage(page, offer);

          // check block heuristics
          let html = null;
          let statusCode = resp ? resp.status() : null;
          if (!result.found) {
            try { html = await page.content().catch(() => null); } catch (e) { html = null; }
          }
          const initialBlocked = (statusCode && [401,403,407,429].includes(statusCode)) || contentLooksBlocked(html);

          // If blocked, confirm via alternate context or reload (like main scraper)
          let confirmedBlock = false;
          if (initialBlocked) {
            logWarn(`[${offer.id}] possible block (status=${statusCode}), confirming...`);
            if (contexts.length > 1) {
              const altIdx = (ctxIdx + 1) % contexts.length;
              const altPage = await contexts[altIdx].newPage();
              try {
                let resp2 = null;
                try {
                  resp2 = await altPage.goto(offer.adLibraryUrl, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT });
                } catch (e) {}
                await altPage.waitForTimeout(WAIT_TIME + jitter(400));
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
              // single context: reload
              try {
                await page.reload({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(() => null);
                await page.waitForTimeout(WAIT_TIME + jitter(400));
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
              if (newAttempts >= 3) {
                logInfo(`[${offer.id}] confirmed block & attempts=${newAttempts} >=3 -> soft-deleting`);
                try {
                  const { data, error } = await supabase
                    .from("swipe_file_offers")
                    .update({ activeAds: null, updated_at: nowIso(), status_updated: "soft_deleted", attempts: newAttempts, deleted_at: nowIso() })
                    .eq("id", offer.id);
                  if (error) {
                    logWarn(`[${offer.id}] DB soft-delete failed for blocked: ${error.message || JSON.stringify(error)} - falling back to update attempts/status`);
                    // fallback: try update without deleted_at if permission restricted
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

          // retries if needed
          for (let attempt = 0; attempt <= RETRY_ATTEMPTS && !result.found; attempt++) {
            if (attempt === 0) continue; // initial attempt already done
            logInfo(`[${offer.id}] retry attempt ${attempt}/${RETRY_ATTEMPTS}`);
            try {
              await page.reload({ waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT }).catch(()=>null);
              await page.waitForTimeout(WAIT_TIME + jitter(400));
              result = await attemptExtractFromPage(page, offer);
              if (result.found) logInfo(`[${offer.id}] retry succeeded: ${result.count}`);
            } catch (e) {
              logWarn(`[${offer.id}] retry error: ${e && e.message}`);
            }
          }

          const ts = nowIso();
          if (result.found) {
            const activeAds = result.count;
            logInfo(`[${offer.id}] success, updating activeAds=${activeAds}`);
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
            if (newAttempts >= 3) {
              logWarn(`[${offer.id}] not found after retries & attempts=${newAttempts} >=3 -> soft-deleting`);
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
