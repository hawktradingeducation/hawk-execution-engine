// Hawk Execution Engine — index.js v1.3
// Uses direct access token — no refresh mechanism

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient } = require("@supabase/supabase-js");

const UPSTASH_URL   = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const CLIENT_ID     = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;
const ACCOUNT_ID    = process.env.CTRADER_ACCOUNT_ID;
const ACCESS_TOKEN  = process.env.CTRADER_ACCESS_TOKEN;
const IS_PAPER      = process.env.IS_PAPER === "true";
const HOST          = IS_PAPER ? "demo.ctraderapi.com" : "live.ctraderapi.com";
const PORT          = 5035;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

function getVolume(score) {
  if (score >= 9) return 300;
  if (score >= 8) return 200;
  return 100;
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
  const { error } = await supabase.from("signal_log").insert({
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
  if (error) console.error("Supabase log error:", error.message);
}
