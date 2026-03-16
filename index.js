// Hawk Execution Engine v1.4 — raw WebSocket, verbose logging
const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

console.log("=== ENGINE STARTING ===");
console.log("Node version:", process.version);
console.log("IS_PAPER:", process.env.IS_PAPER);
console.log("ACCOUNT_ID:", process.env.CTRADER_ACCOUNT_ID);
console.log("Has ACCESS_TOKEN:", !!process.env.CTRADER_ACCESS_TOKEN);
console.log("Has CLIENT_ID:", !!process.env.CTRADER_CLIENT_ID);
console.log("Has UPSTASH_URL:", !!process.env.UPSTASH_REDIS_REST_URL);

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

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Message ID counter
let msgId = 1;

// Pending responses map
const pending = new Map();

// Symbol cache
let symbolMap = {};

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

function connectCTrader() {
  return new Promise((resolve, reject) => {
    console.log(`Connecting to ${HOST}:5035...`);
    const ws = new WebSocket(`wss://${HOST}:5035`);

    ws.on("open", () => {
      console.log("WebSocket connected");
      resolve(ws);
    });

    ws.on("error", (err) => {
      console.error("WebSocket error:", err.message);
      reject(err);
    });

    ws.on("close", (code, reason) => {
      console.error("WebSocket closed:", code, reason.toString());
      process.exit(1); // Railway will restart
    });

    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data);
        const id = msg.clientMsgId;
        if (id && pending.has(id)) {
          const { resolve, reject } = pending.get(id);
          pending.delete(id);
          if (msg.errorCode) {
            reject(new Error(`${msg.errorCode}: ${msg.description}`));
          } else {
            resolve(msg.payload || msg);
          }
        }
      } catch(e) {
        console.error("Message parse error:", e.message);
      }
    });
  });
}

function sendCommand(ws, payloadType, payload) {
  return new Promise((resolve, reject) => {
    const id = String(msgId++);
    pending.set(id, { resolve, reject });
    const msg = JSON.stringify({ clientMsgId: id, payloadType, payload });
    ws.send(msg);
    setTimeout(() => {
      if (pending.has(id)) {
        pending.delete(id);
        reject(new Error("Timeout waiting for response to " + payloadType));
      }
    }, 10000);
  });
}

async function main() {
  console.log("Connecting to cTrader WebSocket...");
  const ws = await connectCTrader();

  console.log("Authenticating application...");
  await sendCommand(ws, "ProtoOAApplicationAuthReq", {
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET
  });
  console.log("Application authenticated");

  console.log("Authenticating account...");
  await sendCommand(ws, "ProtoOAAccountAuthReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    accessToken: ACCESS_TOKEN
  });
  console.log("Account authenticated:", ACCOUNT_ID);

  console.log("Loading symbols...");
  const symRes = await sendCommand(ws, "ProtoOASymbolsListReq", {
    ctidTraderAccountId: ACCOUNT_ID,
    includeArchivedSymbols: false
  });
  const symbols = symRes.symbol || [];
  symbols.forEach(s => { symbolMap[s.symbolName] = s.symbolId; });
  console.log("Symbols loaded:", Object.keys(symbolMap).length);

  console.log("=== ENGINE READY — Polling queue ===");

  // Keep WebSocket alive with heartbeat every 25 seconds
  setInterval(() => {
    ws.send(JSON.stringify({ payloadType: "ProtoHeartbeatEvent", payload: {} }));
  }, 25000);

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

      console.log(`Signal: ${signal.strategy_id} | ${signal.action} | Score:${signal.score}`);

      if (isDuplicate(signal.signal_id)) {
        await logSignal(signal, null, "DUPLICATE");
        continue;
      }

      const isEntry = signal.action === "LONG" || signal.action === "SHORT";
      const isLong  = signal.action === "LONG" || signal.action === "LONG_EXIT" || signal.action === "LONG_STOP";
      const SYMBOL_MAP = { "XAUUSD":"XAUUSD", "BTCUSD":"BTCUSD", "NAS100":"NAS100", "USOIL":"XTIUSD" };

      try {
        const ctSymbol  = SYMBOL_MAP[signal.ticker] || signal.ticker;
        const symbolId  = symbolMap[ctSymbol];
        if (!symbolId) throw new Error("Symbol not found: " + ctSymbol);

        let result;
        if (isEntry) {
          const atr      = parseFloat(signal.atr);
          const stopLoss = Math.round(atr * 2 * 100);
          result = await sendCommand(ws, "ProtoOANewOrderReq", {
            ctidTraderAccountId: ACCOUNT_ID,
            symbolId,
            orderType:        "MARKET",
            tradeSide:        signal.action === "LONG" ? "BUY" : "SELL",
            volume:           getVolume(parseInt(signal.score)),
            relativeStopLoss: stopLoss,
            comment:          `HAWK|${signal.strategy_id}|${signal.signal_id}`
          });
          console.log("Order placed:", JSON.stringify(result));
          await logSignal(signal, result, "EXECUTED");

        } else {
          const posRes = await sendCommand(ws, "ProtoOAReconcileReq", {
            ctidTraderAccountId: ACCOUNT_ID
          });
          const position = (posRes.position || []).find(p =>
            p.tradeData?.symbolId === symbolId &&
            (isLong ? p.tradeData?.tradeSide === "BUY" : p.tradeData?.tradeSide === "SELL")
          );
          if (!position) {
            console.warn("No matching position");
            await logSignal(signal, null, "NO_POSITION");
          } else {
            result = await sendCommand(ws, "ProtoOAClosePositionReq", {
              ctidTraderAccountId: ACCOUNT_ID,
              positionId:          position.positionId,
              volume:              position.tradeData.volume
            });
            console.log("Position closed");
            await logSignal(signal, result, "CLOSED");
          }
        }
      } catch(err) {
        console.error("Execution error:", err.message);
        await logSignal(signal, null, "ERROR", err.message);
      }

    } catch(err) {
      console.error("Queue error:", err.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

main().catch(err => {
  console.error("Fatal:", err.message);
  process.exit(1);
});
