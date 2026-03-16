// Hawk Execution Engine — index.js v1.6 — Full debug logging
"use strict";

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient } = require("@supabase/supabase-js");

console.log("=== HAWK ENGINE v1.6 STARTING ===");

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

console.log("IS_PAPER:", IS_PAPER);
console.log("HOST:", HOST);
console.log("ACCOUNT_ID:", ACCOUNT_ID);

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

async function main() {
  console.log("Opening cTrader connection...");

  const connection = new CTraderConnection({ host: HOST, port: 5035 });

  await connection.open();
  console.log("Connection opened");

  // RAW MESSAGE LISTENER — logs everything cTrader sends back
  connection.on("message", (message) => {
    console.log("RAW cTrader message:", JSON.stringify(message));
  });

  console.log("Sending ProtoOAApplicationAuthReq...");
  await connection.sendCommand("ProtoOAApplicationAuthReq", {
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  });
  console.log("Application authenticated successfully");

  console.log("Sending ProtoOAAccountAuthReq...");
  await connection.sendCommand("ProtoOAAccountAuthReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    accessToken: ACCESS_TOKEN,
  });
  console.log("Account authenticated:", ACCOUNT_ID);

  console.log("Loading symbol list...");
  const symRes = await connection.sendCommand("ProtoOASymbolsListReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    includeArchivedSymbols: false,
  });

  const symbolIdMap = {};
  (symRes.symbol || []).forEach(s => {
    symbolIdMap[s.symbolName] = s.symbolId;
  });
  console.log("Symbols loaded:", Object.keys(symbolIdMap).length);

  // Log all gold-related symbols
  const goldSymbols = (symRes.symbol || []).filter(s =>
    s.symbolName.includes("XAU") || s.symbolName.toLowerCase().includes("gold")
  );
  console.log("Gold symbols found:", JSON.stringify(goldSymbols.map(s => ({
    name: s.symbolName, id: s.symbolId
  }))));

  // Keep connection alive
  setInterval(async () => {
    try {
      await connection.sendCommand("ProtoHeartbeatEvent", {});
    } catch(e) {
      console.error("Heartbeat error:", e.message);
    }
  }, 25000);

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
        console.warn("Duplicate signal — ignoring");
        await logSignal(signal, null, "DUPLICATE");
        continue;
      }

      const isEntry = signal.action === "LONG" || signal.action === "SHORT";
      const isLong  = signal.action === "LONG" || signal.action === "LONG_EXIT" || signal.action === "LONG_STOP";
      const ctSymbol = SYMBOL_MAP[signal.ticker] || signal.ticker;
      const symbolId = symbolIdMap[ctSymbol];

      console.log(`Symbol lookup: ${signal.ticker} -> ${ctSymbol} -> symbolId: ${symbolId}`);

      if (!symbolId) {
        console.error("Symbol not found in map:", ctSymbol);
        console.error("Available symbols containing XAU:", Object.keys(symbolIdMap).filter(k => k.includes("XAU")));
        await logSignal(signal, null, "ERROR", "Symbol not found: " + ctSymbol);
        continue;
      }

      try {
        let result;
        if (isEntry) {
          const volume   = getVolume(parseInt(signal.score));
          const stopLoss = Math.round(parseFloat(signal.atr) * 2 * 10);

          console.log(`Placing order: symbolId=${symbolId} side=${signal.action === "LONG" ? "BUY" : "SELL"} volume=${volume} stopLoss=${stopLoss}`);

          result = await connection.sendCommand("ProtoOANewOrderReq", {
            ctidTraderAccountId: ACCOUNT_ID,
            symbolId,
            orderType:        "MARKET",
            tradeSide:        signal.action === "LONG" ? "BUY" : "SELL",
            volume,
            relativeStopLoss: stopLoss,
            comment:          `HAWK|${signal.strategy_id}|${signal.signal_id}`,
          });
          console.log("Order response:", JSON.stringify(result));
          await logSignal(signal, result, "EXECUTED");

        } else {
          const posRes = await connection.sendCommand("ProtoOAReconcileReq", {
            ctidTraderAccountId: ACCOUNT_ID,
          });
          console.log("Positions:", JSON.stringify(posRes));

          const position = (posRes.position || []).find(p =>
            p.tradeData?.symbolId === symbolId &&
            (isLong ? p.tradeData?.tradeSide === "BUY" : p.tradeData?.tradeSide === "SELL")
          );

          if (!position) {
            console.warn("No matching position");
            await logSignal(signal, null, "NO_POSITION");
          } else {
            result = await connection.sendCommand("ProtoOAClosePositionReq", {
              ctidTraderAccountId: ACCOUNT_ID,
              positionId:          position.positionId,
              volume:              position.tradeData.volume,
            });
            console.log("Close response:", JSON.stringify(result));
            await logSignal(signal, result, "CLOSED");
          }
        }
      } catch(err) {
        console.error("Execution error:", err.message);
        console.error("Execution error full:", JSON.stringify(err));
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
