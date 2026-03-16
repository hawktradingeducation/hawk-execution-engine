// Hawk Execution Engine — index.js v1.2
// Uses ctrader-layer for WebSocket connection to cTrader Open API

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient } = require("@supabase/supabase-js");

// ── ENVIRONMENT VARIABLES ────────────────────────────────────────────────────
const UPSTASH_URL        = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN      = process.env.UPSTASH_REDIS_REST_TOKEN;
const SUPABASE_URL       = process.env.SUPABASE_URL;
const SUPABASE_KEY       = process.env.SUPABASE_SERVICE_KEY;
const CLIENT_ID          = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET      = process.env.CTRADER_CLIENT_SECRET;
const ACCOUNT_ID         = process.env.CTRADER_ACCOUNT_ID;
const ACCESS_TOKEN       = process.env.CTRADER_REFRESH_TOKEN; // we use refresh token as access token here
const IS_PAPER           = process.env.IS_PAPER === "true";

const HOST = IS_PAPER ? "demo.ctraderapi.com" : "live.ctraderapi.com";
const PORT = 5035;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── LOT SIZING ───────────────────────────────────────────────────────────────
// score 7 = 0.01 lots, 8 = 0.02 lots, 9 = 0.03 lots
// cTrader volume is in units: for XAUUSD, 1 lot = 100 units
// So 0.01 lots = 1 unit, 0.02 = 2 units, 0.03 = 3 units
function getVolume(score) {
  if (score >= 9) return 3;
  if (score >= 8) return 2;
  return 1;
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
}

// ── SYMBOL MAP ───────────────────────────────────────────────────────────────
const SYMBOL_MAP = {
  "XAUUSD": "XAUUSD",
  "BTCUSD": "BTCUSD",
  "NAS100": "NAS100",
  "USOIL":  "XTIUSD",
};

// ── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`╔════════════════════════════════════════╗`);
  console.log(`║ Hawk Execution Engine v1.2 STARTED     ║`);
  console.log(`║ Mode: ${IS_PAPER ? "PAPER (safe to test)    " : "LIVE  (real money!)    "}    ║`);
  console.log(`║ ${new Date().toISOString()}  ║`);
  console.log(`╚════════════════════════════════════════╝`);

  // Open WebSocket connection to cTrader
  const connection = new CTraderConnection({ host: HOST, port: PORT });
  await connection.open();
  console.log("cTrader WebSocket connected");

  // Step 1: Authenticate the application
  await connection.sendCommand("ProtoOAApplicationAuthReq", {
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
  });
  console.log("Application authenticated");

  // Step 2: Authenticate the trading account
  await connection.sendCommand("ProtoOAAccountAuthReq", {
    ctidTraderAccountId: parseInt(ACCOUNT_ID),
    accessToken: ACCESS_TOKEN,
  });
  console.log(`Account ${ACCOUNT_ID} authenticated`);

  // Step 3: Get symbol list so we can resolve symbol IDs
  const symbolsRes = await connection.sendCommand("ProtoOASymbolsListReq", {
    ctidTraderAccountId: parseInt(ACCOUNT_ID),
    includeArchivedSymbols: false,
  });
  const symbols = symbolsRes.symbol || [];
  console.log(`Loaded ${symbols.length} symbols`);

  function getSymbolId(ticker) {
    const name = SYMBOL_MAP[ticker] || ticker;
    const sym = symbols.find(s => s.symbolName === name);
    if (!sym) throw new Error(`Symbol not found: ${name}. Check SYMBOL_MAP.`);
    return sym.symbolId;
  }

  // Step 4: Poll queue and process signals
  console.log("Polling Upstash queue for signals...");

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
      if (!data.result) continue; // timeout — no signal, loop back

      const raw = Array.isArray(data.result) ? data.result[1] : data.result;
      const signal = typeof raw === "string" ? JSON.parse(raw) : raw;

      console.log(`--- Signal: ${signal.strategy_id} | ${signal.action} | Score: ${signal.score} | ID: ${signal.signal_id}`);

      if (isDuplicate(signal.signal_id)) {
        console.warn("Duplicate — ignoring:", signal.signal_id);
        await logSignal(signal, null, "DUPLICATE");
        continue;
      }

      const isEntry = signal.action === "LONG" || signal.action === "SHORT";
      const isLong  = signal.action === "LONG" || signal.action === "LONG_EXIT" || signal.action === "LONG_STOP";

      try {
        let result;

        if (isEntry) {
          const symbolId  = getSymbolId(signal.ticker);
          const volume    = getVolume(parseInt(signal.score));
          const atr       = parseFloat(signal.atr);
          const stopLoss  = Math.round(atr * 2 * 100); // 2x ATR in pips

          result = await connection.sendCommand("ProtoOANewOrderReq", {
            ctidTraderAccountId: parseInt(ACCOUNT_ID),
            symbolId,
            orderType:           "MARKET",
            tradeSide:           signal.action === "LONG" ? "BUY" : "SELL",
            volume:              volume * 100, // cTrader uses 1/100th lot units
            relativeStopLoss:    stopLoss,
            comment:             `HAWK|${signal.strategy_id}|${signal.signal_id}`,
          });
          console.log("Order placed:", result);
          await logSignal(signal, result, "EXECUTED");

        } else {
          // Exit or stop — find and close the open position
          const posRes = await connection.sendCommand("ProtoOAReconcileReq", {
            ctidTraderAccountId: parseInt(ACCOUNT_ID),
          });

          const ctSymbol = SYMBOL_MAP[signal.ticker] || signal.ticker;
          const position = (posRes.position || []).find(p =>
            p.tradeData?.symbolId === getSymbolId(signal.ticker) &&
            (isLong ? p.tradeData?.tradeSide === "BUY" : p.tradeData?.tradeSide === "SELL")
          );

          if (!position) {
            console.warn("No matching position found — may already be closed");
            await logSignal(signal, null, "NO_POSITION");
          } else {
            result = await connection.sendCommand("ProtoOAClosePositionReq", {
              ctidTraderAccountId: parseInt(ACCOUNT_ID),
              positionId:          position.positionId,
              volume:              position.tradeData.volume,
            });
            console.log("Position closed:", result);
            await logSignal(signal, result, "CLOSED");
          }
        }

      } catch (err) {
        console.error("Execution error:", err.message);
        await logSignal(signal, null, "ERROR", err.message);
      }

    } catch (err) {
      console.error("Queue poll error:", err.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

main().catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
