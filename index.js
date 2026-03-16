// Hawk Execution Engine — index.js v1.9
"use strict";

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient } = require("@supabase/supabase-js");

console.log("=== HAWK ENGINE v1.9 STARTING ===");

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

// Volume in cTrader cents: 1000 = 0.01 lots, 2000 = 0.02 lots, 3000 = 0.03 lots
function getVolume(score) {
  if (score >= 9) return 3000;
  if (score >= 8) return 2000;
  return 1000;
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
  "XAUUSD": "XAUUSD",
  "BTCUSD": "BTCUSD",
  "NAS100": "NAS100",
  "USOIL":  "XTIUSD",
};

async function main() {
  console.log("Opening cTrader connection...");
  const connection = new CTraderConnection({ host: HOST, port: 5035 });
  await connection.open();
  console.log("Connection opened");

  connection.on("ProtoOAExecutionEvent", (e) => {
    console.log("EXECUTION EVENT:", JSON.stringify(e));
  });
  connection.on("ProtoOAOrderErrorEvent", (e) => {
    console.error("ORDER ERROR EVENT:", JSON.stringify(e));
  });
  connection.on("ProtoOAErrorRes", (e) => {
    console.error("ERROR RES EVENT:", JSON.stringify(e));
  });

  await connection.sendCommand("ProtoOAApplicationAuthReq", {
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  });
  console.log("Application authenticated");

  await connection.sendCommand("ProtoOAAccountAuthReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    accessToken: ACCESS_TOKEN,
  });
  console.log("Account authenticated:", ACCOUNT_ID);

  const symRes = await connection.sendCommand("ProtoOASymbolsListReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    includeArchivedSymbols: false,
  });

  const symbolIdMap = {};
  (symRes.symbol || []).forEach(s => { symbolIdMap[s.symbolName] = s.symbolId; });
  console.log("Symbols loaded:", Object.keys(symbolIdMap).length);

  // Log XAUUSD details
  const xau = (symRes.symbol || []).find(s => s.symbolName === "XAUUSD");
  console.log("XAUUSD details:", JSON.stringify(xau));

  setInterval(() => connection.sendHeartbeat(), 25000);

  console.log("=== ENGINE READY — polling queue ===");

  while (true) {
    try {
      const res = await fetch(
        `${UPSTASH_URL}/brpop/hawk:signals/5`,
        { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
      );

      if (!res.ok) {
        console.error("Upstash error:", res.status);
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }

      const data = await res.json();
      if (!data.result) continue;

      const raw = Array.isArray(data.result) ? data.result[1] : data.result;
      const signal = typeof raw === "string" ? JSON.parse(raw) : raw;

      console.log(`Signal: ${signal.strategy_id} | ${signal.action} | Score: ${signal.score} | ID: ${signal.signal_id}`);

      if (isDuplicate(signal.signal_id)) {
        console.warn("Duplicate — ignoring:", signal.signal_id);
        await logSignal(signal, null, "DUPLICATE");
        continue;
      }

      const isEntry = signal.action === "LONG" || signal.action === "SHORT";
      const isLong  = signal.action === "LONG" || signal.action === "LONG_EXIT" || signal.action === "LONG_STOP";
      const ctSymbol = SYMBOL_MAP[signal.ticker] || signal.ticker;
      const symbolId = symbolIdMap[ctSymbol];

      if (!symbolId) {
        console.error("Symbol not found:", ctSymbol);
        await logSignal(signal, null, "ERROR", "Symbol not found: " + ctSymbol);
        continue;
      }

      try {
        if (isEntry) {
          const volume   = getVolume(parseInt(signal.score));
          // relativeStopLoss in 1/100000 of price unit
          // ATR 5.5 * 2 * 100000 = 1,100,000 = 11 point stop
          const stopLoss = Math.round(parseFloat(signal.atr) * 2 * 100000);

          console.log(`Sending order: ${ctSymbol} | symbolId=${symbolId} | side=${signal.action === "LONG" ? "BUY" : "SELL"} | volume=${volume} | stopLoss=${stopLoss}`);

          connection.sendCommand("ProtoOANewOrderReq", {
            ctidTraderAccountId: ACCOUNT_ID,
            symbolId,
            orderType:        "MARKET",
            tradeSide:        signal.action === "LONG" ? "BUY" : "SELL",
            volume,
            relativeStopLoss: stopLoss,
            comment:          `HAWK|${signal.strategy_id}|${signal.signal_id}`,
          }).then(r => {
            console.log("Order acknowledged:", JSON.stringify(r));
          }).catch(e => {
            console.error("Order rejected:", e.message, JSON.stringify(e));
          });

          await logSignal(signal, { sent: true, symbolId, volume, stopLoss }, "EXECUTED");

        } else {
          const posRes = await connection.sendCommand("ProtoOAReconcileReq", {
            ctidTraderAccountId: ACCOUNT_ID,
          });

          const position = (posRes.position || []).find(p =>
            p.tradeData?.symbolId === symbolId &&
            (isLong ? p.tradeData?.tradeSide === "BUY" : p.tradeData?.tradeSide === "SELL")
          );

          if (!position) {
            console.warn("No matching position found");
            await logSignal(signal, null, "NO_POSITION");
          } else {
            console.log(`Closing position: ${position.positionId}`);
            connection.sendCommand("ProtoOAClosePositionReq", {
              ctidTraderAccountId: ACCOUNT_ID,
              positionId:          position.positionId,
              volume:              position.tradeData.volume,
            }).then(r => console.log("Close acknowledged:", JSON.stringify(r)))
              .catch(e => console.error("Close error:", e.message));

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
  console.error("Fatal:", err.message);
  process.exit(1);
});
