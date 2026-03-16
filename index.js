// Hawk Execution Engine — index.js v1.7
"use strict";

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient } = require("@supabase/supabase-js");

console.log("=== HAWK ENGINE v1.7 STARTING ===");

const UPSTASH_URL   = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const CLIENT_ID     = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;
const ACCOUNT_ID    = parseInt(process.env.CTRADER_ACCOUNT_ID);
const ACCESS_TOKEN  = process.env.CTRADER_ACCESS_TOKEN;
const IS_PAPER      = process.env.IS_PAPER === "true";
const HOST          = IS_PAPER ? "demo.ctraderapi.com" : "live.ctraderapi.com";

console.log("IS_PAPER:", IS_PAPER, "| HOST:", HOST, "| ACCOUNT_ID:", ACCOUNT_ID);

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

function getVolume(score) {
  if (score >= 9) return 3;
  if (score >= 8) return 2;
  return 1;
}

const seenSignals = new Map();
function isDuplicate(signalId) {
  const now = Date.now();
  for (const [id, ts] of seenSignals) {
    if (now - ts > 10000) seenSignals.delete(id);
  }
  if (seenSignals.has(signalId)) return true;
  seenSignals.set(signalId, now);
  return false;
}

async function logSignal(signal, result, status, errorMsg = null) {
  try {
    await supabase.from("signal_log").insert({
      signal_id:     signal.signal_id,
      strategy_id:   signal.strategy_id,
      ticker:        signal.ticker,
      action:        signal.action,
      score:         parseInt(signal.score),
      atr:           parseFloat(signal.atr),
      close_price:   parseFloat(signal.close),
      status,
      error_message: errorMsg,
      api_response:  result ? JSON.stringify(result) : null,
      signal_time:   new Date(parseInt(signal.timestamp)).toISOString(),
      processed_at:  new Date().toISOString(),
      is_paper:      IS_PAPER
    });
    console.log("Logged to Supabase:", status);
  } catch(e) {
    console.error("Supabase error:", e.message);
  }
}

const SYMBOL_MAP = {
  "XAUUSD": "XAUUSD-F",
  "BTCUSD": "BTCUSD",
  "NAS100": "NAS100",
  "USOIL":  "XTIUSD",
};

as
