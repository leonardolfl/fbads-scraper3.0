#!/usr/bin/env node
/**
 * scraper_nulls.mjs (versão mobile otimizada)
 *
 * Atualiza ofertas com activeAds IS NULL.
 * - Busca ofertas ainda não processadas
 * - Tenta extrair número de anúncios ativos
 * - Se sucesso → atualiza activeAds e zera attempts
 * - Se falha → incrementa attempts
 * - Se attempts >= 5 → soft delete (deleted_at = now())
 *
 * Configurações via env:
 *   TOTAL_WORKERS, WORKER_INDEX, WAIT_TIME, MAX_FAILS, PARALLEL, RETRY_ATTEMPTS
 */

import fs from "fs";
import os from "os";
import path from "path";
import { chromium, devices } from "playwright";
import { supabase } from "./supabase.js";

const TOTAL_WORKERS = parseInt(process.env.TOTAL_WORKERS || "4", 10);
const WORKER_INDEX = parseInt(process.env.WORKER_INDEX ?? "0", 10);
const PARALLEL = Math.max(1, parseInt(process.env.PARALLEL || "3", 10));
const CONTEXT_POOL_SIZE = Math.max(1, parseInt(process.env.CONTEXT_POOL_SIZE || String(PARALLEL), 10));
const WAIT_TIME = parseInt(process.env.WAIT_TIME || "10000", 10);
const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || "90000", 10);
const SELECTOR_TIMEOUT = parseInt(process.env.SELECTOR_TIMEOUT || "15000", 10);
const RETRY_ATTEMPTS = Math.max(0, parseInt(process.env.RETRY_ATTEMPTS || "1", 10));
const MAX_FAILS = parseInt(process.env.MAX_FAILS || "5", 10);
const DEBUG = String(process.env.DEBUG || "false").toLowerCase() === "true";
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
const WORKER_ID = process.env.WORKER_ID || `${os.hostname()}-${process.pid}-${Date.now()}`;
const DEBUG_DIR = process.env.DEBUG_DIR || "./debug_nulls";

// apenas dispositivos e user-agents mobile
const DEVICE_NAMES = [
  "iPhone 13 Pro Max",
  "Pixel 7",
  "iPhone 12",
  "Galaxy S21 Ultra",
  "iPad Mini",
];
const DEVICE_POOL = DEVICE_NAMES.map(n => devices[n]).filter(Boolean);
const USER_AGENTS = [
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
];

function nowIso() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }
function jitter(ms) { return Math.floor(Math.random() * ms); }
function shuffle(a) { const arr = a.slice(); for (let i = arr.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [arr[i], arr[j]] = [arr[j], arr[i]]; } return arr; }

function logInfo(...a){if(["info"].includes(LOG_LEVEL))console.log(...a);}
function logWarn(...a){if(["info","warn"].includes(LOG_LEVEL))console.warn(...a);}
function logError(...a){if(["info","warn","error"].includes(LOG_LEVEL))console.error(...a);}

function ensureDebugDir(){if(!DEBUG)return;if(!fs.existsSync(DEBUG_DIR))fs.mkdirSync(DEBUG_DIR,{recursive:true});}
async function saveDebugHtml(html,id,note=""){if(!DEBUG)return;try{ensureDebugDir();const f=path.join(DEBUG_DIR,`offer-${id}-${Date.now()}.html`);await fs.promises.writeFile(f,`<!-- ${note} -->\n`+html,"utf8");return f;}catch(e){logWarn("saveDebugHtml error:",e?.message);return null;}}

function parseCountFromText(text){
  if(!text)return null;
  let m=text.match(/~\s*([\d.,]+)/)||text.match(/([\d\.,\s\u00A0\u202F]+)\s*(?:resultados|results)/i);
  if(m&&m[1]){
    const cleaned=m[1].replace(/[.,\s\u00A0\u202F]/g,"");
    const n=parseInt(cleaned,10);
    return isNaN(n)?null:n;
  }
  return null;
}
function contentLooksBlocked(html){
  if(!html)return false;
  const ln=html.toLowerCase();
  return ["captcha","verify","temporarily blocked","unusual activity","confirm you're human","log in to facebook","sign in to continue","please enable javascript","blocked"].some(k=>ln.includes(k));
}

async function createContext(browser){
  const device=DEVICE_POOL[Math.floor(Math.random()*DEVICE_POOL.length)]||{};
  const ua=USER_AGENTS[Math.floor(Math.random()*USER_AGENTS.length)];
  const ctx=await browser.newContext({...device,userAgent:ua});
  await ctx.route("**/*",route=>{
    const t=route.request().resourceType();
    if(["image","stylesheet","font","media"].includes(t))return route.abort();
    return route.continue();
  });
  ctx.__pagesProcessed=0;
  return ctx;
}

async function attemptExtractFromPage(page){
  try{
    const txt=await page.locator('div[role="heading"][aria-level="3"]').first().textContent({timeout:SELECTOR_TIMEOUT}).catch(()=>null);
    if(txt){const parsed=parseCountFromText(txt);if(parsed!=null)return{found:true,count:parsed,raw:txt};}
  }catch(e){}
  try{
    for(const frame of page.frames()){
      const ft=await frame.locator('div[role="heading"][aria-level="3"]').first().textContent().catch(()=>null);
      if(ft){const parsed=parseCountFromText(ft);if(parsed!=null)return{found:true,count:parsed,raw:ft};}
    }
  }catch(e){}
  try{
    const evalRes=await page.evaluate(()=>{
      try{const el=document.querySelector('div[role="heading"][aria-level="3"]');return el?(el.innerText||""):null;}catch(e){return null;}
    });
    if(evalRes){const p=parseCountFromText(evalRes);if(p!=null)return{found:true,count:p,raw:evalRes};}
  }catch(e){}
  return{found:false};
}

async function processOffers(offersSlice){
  const browser=await chromium.launch({headless:true,args:["--no-sandbox","--disable-setuid-sandbox"]});
  const contexts=[];for(let i=0;i<CONTEXT_POOL_SIZE;i++)contexts.push(await createContext(browser));

  try{
    for(let i=0;i<offersSlice.length;i+=PARALLEL){
      const batch=offersSlice.slice(i,i+PARALLEL);
      logInfo(`📦 Worker ${WORKER_INDEX} processando batch ${Math.floor(i/PARALLEL)+1} (${batch.length})`);
      await Promise.all(batch.map(async offer=>{
        if(!offer||!offer.adLibraryUrl){logWarn(`[${offer?.id}] ⚠️ sem URL`);return;}
        const ctxIdx=Math.floor(Math.random()*contexts.length);
        const context=contexts[ctxIdx];
        const page=await context.newPage();

        try{
          logInfo(`🔍 [${offer.id}] visitando ${offer.adLibraryUrl}`);
          let resp=null;
          try{resp=await page.goto(offer.adLibraryUrl,{waitUntil:"domcontentloaded",timeout:NAV_TIMEOUT});}
          catch(e){logWarn(`[${offer.id}] erro goto: ${e?.message}`);}
          await page.waitForTimeout(WAIT_TIME+jitter(1000));
          let result=await attemptExtractFromPage(page,offer);
          let html=null;let statusCode=resp?resp.status():null;
          if(!result.found){try{html=await page.content().catch(()=>null);}catch(e){}}
          const blocked=(statusCode&&[401,403,407,429].includes(statusCode))||contentLooksBlocked(html);

          if(result.found){
            const activeAds=result.count;
            logInfo(`✅ [${offer.id}] ${activeAds} anúncios encontrados`);
            const ts=nowIso();
            await supabase.from("swipe_file_offers")
              .update({activeAds,updated_at:ts,status_updated:"success",attempts:0})
              .eq("id",offer.id);
            return;
          }

          if(blocked){
            const newAttempts=(offer.attempts??0)+1;
            const ts=nowIso();
            logWarn(`⚠️ [${offer.id}] bloqueado ou vazio (tentativa ${newAttempts}/${MAX_FAILS})`);
            if(newAttempts>=MAX_FAILS){
              logWarn(`💀 [${offer.id}] soft delete após ${newAttempts} falhas`);
              await supabase.from("swipe_file_offers")
                .update({activeAds:null,updated_at:ts,status_updated:"soft_deleted",attempts:newAttempts,deleted_at:ts})
                .eq("id",offer.id);
            }else{
              await supabase.from("swipe_file_offers")
                .update({activeAds:null,updated_at:ts,status_updated:"erro",attempts:newAttempts})
                .eq("id",offer.id);
            }
            return;
          }

          // caso não encontre mas não bloqueou
          const newAttempts=(offer.attempts??0)+1;
          const ts=nowIso();
          if(newAttempts>=MAX_FAILS){
            logWarn(`💀 [${offer.id}] sem resultado após ${newAttempts} → soft delete`);
            await supabase.from("swipe_file_offers")
              .update({activeAds:null,updated_at:ts,status_updated:"soft_deleted",attempts:newAttempts,deleted_at:ts})
              .eq("id",offer.id);
          }else{
            logWarn(`⚠️ [${offer.id}] sem resultado, tentativa ${newAttempts}/${MAX_FAILS}`);
            await supabase.from("swipe_file_offers")
              .update({activeAds:null,updated_at:ts,status_updated:"erro",attempts:newAttempts})
              .eq("id",offer.id);
          }

        }catch(err){
          logError(`💥 [${offer.id}] erro inesperado: ${err?.message}`);
        }finally{
          try{await page.close();}catch(e){}
          context.__pagesProcessed++;
          if(context.__pagesProcessed>=50){
            try{await context.close();contexts[ctxIdx]=await createContext(browser);}catch(e){}
          }
          await sleep(300+jitter(300));
        }
      }));
    }
  }finally{
    for(const c of contexts)try{await c.close();}catch(e){}
    try{await browser.close();}catch(e){}
  }
}

(async()=>{
  logInfo(`🚀 scraper_nulls iniciado worker ${WORKER_INDEX}/${TOTAL_WORKERS} - ${WORKER_ID}`);
  if(DEBUG)ensureDebugDir();
  const {data:offersAll,error}=await supabase
    .from("swipe_file_offers")
    .select("*")
    .is("activeAds",null)
    .is("deleted_at",null)
    .order("id",{ascending:true});
  if(error){logError("Erro ao buscar ofertas:",error);process.exit(1);}
  if(!Array.isArray(offersAll)||offersAll.length===0){logInfo("Nada para processar.");process.exit(0);}

  const shuffled=shuffle(offersAll);
  const total=shuffled.length;
  const chunkSize=Math.ceil(total/TOTAL_WORKERS);
  const myOffers=shuffled.slice(WORKER_INDEX*chunkSize,Math.min((WORKER_INDEX+1)*chunkSize,total));

  logInfo(`Worker ${WORKER_INDEX} processando ${myOffers.length}/${total} ofertas`);
  if(myOffers.length>0)await processOffers(myOffers);
  logInfo(`✅ Worker ${WORKER_INDEX} finalizado`);
  process.exit(0);
})();
