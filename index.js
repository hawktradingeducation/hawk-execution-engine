// Hawk Execution Engine — index.js v2.0
// Production build — automatic token refresh, correct lot sizing
"use strict";

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient } = require("@supabase/supabase-js");

console.log("=== HAWK ENGINE v2.0 STARTING ===");

const UPSTASH_URL    = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN  = process.env.UPSTASH_REDIS_REST_TOKEN;
const SUPABASE_URL   = process.env.SUPABASE_URL;
const SUPABASE_KEY   = process.env.SUPABASE_SERVICE_KEY;
const CLIENT_ID      = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET  = process.env.CTRADER_CLIENT_SECRET;
const REFRESH_TOKEN  = process.env.CTRADER_REFRESH_TOKEN;
const ACCOUNT_ID     = parseInt(process.env.CTRADER_ACCOUNT_ID);
const IS_PAPER       = process.env.IS_PAPER === "true";
const HOST           = IS_PAPER ? "demo.ctraderapi.com" : "live.ctraderapi.com";

console.log("IS_PAPER:", IS_PAPER, "| HOST:", HOST, "| ACCOUNT_ID:", ACCOUNT_ID);

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── TOKEN MANAGEMENT ─────────────────────────────────────────────────────────
// Fetches a fresh access token from Spotware using the refresh token.
// Called at startup and automatically every 25 days.
// Never requires manual intervention.

let currentAccessToken = null;

async function refreshAccessToken() {
  console.log("Refreshing access token from Spotware...");
  const res = await fetch("https://connect.spotware.com/apps/token", {
    method:  "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type:    "refresh_token",
      refresh_token: REFRESH_TOKEN,
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
    }).toString(),
  });
  const data = await res.json();
  if (!data.access_token) {
    throw new Error("Token refresh failed: " + JSON.stringify(data));
  }
  currentAccessToken = data.access_token;
  console.log("Access token refreshed successfully. Expires in", data.expires_in, "seconds");
  return currentAccessToken;
}

// ── VOLUME: score -> cTrader volume cents ────────────────────────────────────
// cTrader volume is in cents: 100 cents = 0.01 lots
// Score 7 = 0.04 lots = 400 cents
// Score 8 = 0.05 lots = 500 cents
// Score 9 = 0.06 lots = 600 cents
// These match Pine Script qty values of 4, 5, 6 oz respectively
// and the agreed position sizing for a $10,000 / 1:30 account

function getVolume(score) {
  if (score >= 9) return 600;
  if (score >= 8) return 500;
  return 400;
}

// ── DEDUPLICATION ────────────────────────────────────────────────────────────
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

// ── SUPABASE LOGGING ─────────────────────────────────────────────────────────
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
  } catch(e) {
    console.error("Supabase log error:", e.message);
  }
}

// ── SYMBOL MAP ───────────────────────────────────────────────────────────────
// Maps TradingView ticker names to exact Pepperstone cTrader symbol names.
// XAUUSD verified as symbolId=41 on Pepperstone demo.
// Other symbols must be verified before enabling those strategies.

const SYMBOL_MAP = {
  "XAUUSD": "XAUUSD",
  "BTCUSD": "BTCUSD",
  "NAS100": "NAS100",
  "USOIL":  "XTIUSD",
};

// ── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {

  // Step 1: Get fresh access token at startup
  const accessToken = await refreshAccessToken();

  // Schedule automatic refresh every 25 days (well before 30-day expiry)
  const REFRESH_INTERVAL_MS = 25 * 24 * 60 * 60 * 1000;
  setInterval(async () => {
    try {
      await refreshAccessToken();
      console.log("Scheduled token refresh completed");
    } catch(e) {
      console.error("Scheduled token refresh failed:", e.message);
    }
  }, REFRESH_INTERVAL_MS);

  // Step 2: Connect to cTrader
  console.log("Connecting to cTrader...");
  const connection = new CTraderConnection({ host: HOST, port: 5035 });
  await connection.open();
  console.log("WebSocket connected");

  // Log execution events for monitoring
  connection.on("ProtoOAExecutionEvent", (e) => {
    console.log("Execution event received");
  });
  connection.on("ProtoOAOrderErrorEvent", (e) => {
    console.error("Order error event:", JSON.stringify(e));
  });

  // Step 3: Authenticate application
  await connection.sendCommand("ProtoOAApplicationAuthReq", {
    clientId:     CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  });
  console.log("Application authenticated");

  // Step 4: Authenticate account
  await connection.sendCommand("ProtoOAAccountAuthReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    accessToken:         currentAccessToken,
  });
  console.log("Account authenticated:", ACCOUNT_ID);

  // Step 5: Load symbol list
  const symRes = await connection.sendCommand("ProtoOASymbolsListReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    includeArchivedSymbols: false,
  });
  const symbolIdMap = {};
  (symRes.symbol || []).forEach(s => { symbolIdMap[s.symbolName] = s.symbolId; });
  console.log("Symbols loaded:", Object.keys(symbolIdMap).length);

  // Keep connection alive
  setInterval(() => connection.sendHeartbeat(), 25000);

  console.log("=== ENGINE READY | Mode:", IS_PAPER ? "PAPER" : "LIVE", "===");

  // ── SIGNAL PROCESSING LOOP ───────────────────────────────────────────────
  while (true) {
    try {
      const res = await fetch(
        `${UPSTASH_URL}/brpop/hawk:signals/5`,
        { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
      );

      if (!res.ok) {
        console.error("Upstash poll error:", res.status);
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }

      const data = await res.json();
      if (!data.result) continue; // 5-second timeout, no signal — loop back

      const raw    = Array.isArray(data.result) ? data.result[1] : data.result;
      const signal = typeof raw === "string" ? JSON.parse(raw) : raw;

      console.log(`Signal | ${signal.strategy_id} | ${signal.action} | Score:${signal.score} | ID:${signal.signal_id}`);

      // Deduplication check
      if (isDuplicate(signal.signal_id)) {
        console.log("Duplicate signal ignored:", signal.signal_id);
        await logSignal(signal, null, "DUPLICATE");
        continue;
      }

      const isEntry = signal.action === "LONG"  || signal.action === "SHORT";
      const isLong  = signal.action === "LONG"  || signal.action === "LONG_EXIT"  || signal.action === "LONG_STOP";
      const ctSymbol = SYMBOL_MAP[signal.ticker] || signal.ticker;
      const symbolId = symbolIdMap[ctSymbol];

      if (!symbolId) {
        console.error("Symbol not found in map:", ctSymbol);
        await logSignal(signal, null, "ERROR", "Symbol not found: " + ctSymbol);
        continue;
      }

      try {
        if (isEntry) {
          // ── PLACE ENTRY ORDER ──────────────────────────────────────────
          const volume = getVolume(parseInt(signal.score));

          // Fixed stop loss at entry: 2 x ATR
          // relativeStopLoss is in 1/100000 of price unit
          // ATR 5.5 * 2 * 100000 = 1,100,000 (verified working)
          const stopLoss = Math.round(parseFloat(signal.atr) * 2 * 100000);

          console.log(`Order | ${ctSymbol} | ${signal.action === "LONG" ? "BUY" : "SELL"} | ${volume} cents | SL: ${stopLoss}`);

          connection.sendCommand("ProtoOANewOrderReq", {
            ctidTraderAccountId: ACCOUNT_ID,
            symbolId,
            orderType:        "MARKET",
            tradeSide:        signal.action === "LONG" ? "BUY" : "SELL",
            volume,
            relativeStopLoss: stopLoss,
            comment:          `HAWK|${signal.strategy_id}|S${signal.score}`,
          }).then(() => {
            console.log("Order sent successfully");
          }).catch(e => {
            console.error("Order send error:", e.message);
          });

          await logSignal(signal, { symbolId, volume, stopLoss }, "EXECUTED");

        } else {
          // ── CLOSE POSITION ────────────────────────────────────────────
          const posRes = await connection.sendCommand("ProtoOAReconcileReq", {
            ctidTraderAccountId: ACCOUNT_ID,
          });

          const position = (posRes.position || []).find(p =>
            p.tradeData?.symbolId === symbolId &&
            (isLong
              ? p.tradeData?.tradeSide === "BUY"
              : p.tradeData?.tradeSide === "SELL")
          );

          if (!position) {
            // Normal — broker-level stop may have already closed it
            console.log("No matching open position for", signal.action, ctSymbol);
            await logSignal(signal, null, "NO_POSITION");
          } else {
            console.log("Closing position:", position.positionId);
            connection.sendCommand("ProtoOAClosePositionReq", {
              ctidTraderAccountId: ACCOUNT_ID,
              positionId:          position.positionId,
              volume:              position.tradeData.volume,
            }).then(() => {
              console.log("Close sent successfully");
            }).catch(e => {
              console.error("Close send error:", e.message);
            });

            await logSignal(signal, { positionId: position.positionId }, "CLOSED");
          }
        }

      } catch(err) {
        console.error("Execution error:", err.message);
        await logSignal(signal, null, "ERROR", err.message);
      }

    } catch(err) {
      console.error("Queue poll error:", err.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

main().catch(err => {
  console.error("Fatal startup error:", err.message);
  process.exit(1);
});
