// Hawk Execution Engine — index.js v2.2
// Resilient FAF Pipeline — awaited sendCommand + reconcile polling fallback
"use strict";

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient }      = require("@supabase/supabase-js");
const express               = require("express");

console.log("=== HAWK ENGINE v2.2 STARTING ===");

// ── ENV VARS ──────────────────────────────────────────────────────────
const UPSTASH_URL     = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN   = process.env.UPSTASH_REDIS_REST_TOKEN;
const SUPABASE_URL    = process.env.SUPABASE_URL;
const SUPABASE_KEY    = process.env.SUPABASE_SERVICE_KEY;
const CLIENT_ID       = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET   = process.env.CTRADER_CLIENT_SECRET;
const REFRESH_TOKEN   = process.env.CTRADER_REFRESH_TOKEN;
const ACCOUNT_ID      = parseInt(process.env.CTRADER_ACCOUNT_ID);
const IS_PAPER        = process.env.IS_PAPER === 'true';
const INTERNAL_SECRET = process.env.INTERNAL_SECRET;
const PORT            = parseInt(process.env.PORT) || 3000;
const HOST            = IS_PAPER ? 'demo.ctraderapi.com' : 'live.ctraderapi.com';

console.log("IS_PAPER:", IS_PAPER, "| HOST:", HOST, "| ACCOUNT_ID:", ACCOUNT_ID);

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── TOKEN MANAGEMENT ──────────────────────────────────────────────────
let currentAccessToken = null;
let tokenExpiryTime    = null;

async function refreshAccessToken() {
  console.log("Refreshing cTrader access token...");
  const res = await fetch("https://connect.spotware.com/apps/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type:    "refresh_token",
      refresh_token: REFRESH_TOKEN,
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
    }).toString(),
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Token refresh failed: ' + JSON.stringify(data));
  currentAccessToken = data.access_token;
  tokenExpiryTime    = Date.now() + (data.expires_in * 1000);
  const daysLeft     = Math.floor(data.expires_in / 86400);
  console.log("Access token refreshed. Expires in", daysLeft, "days");
  await logHealth('RUNNING', daysLeft);
  if (daysLeft < 7) await logAlert('TOKEN_EXPIRY_WARNING', 'WARN',
    `cTrader access token expires in ${daysLeft} days. Refresh token rotation may be required.`);
  return currentAccessToken;
}

// ── VOLUME ────────────────────────────────────────────────────────────
// Score 7 = 0.04 lots = 400 cents
// Score 8 = 0.05 lots = 500 cents
// Score 9 = 0.06 lots = 600 cents
function getVolume(score) {
  if (score >= 9) return 600;
  if (score >= 8) return 500;
  return 400;
}

// ── SIGNAL VALIDITY ───────────────────────────────────────────────────
const SIGNAL_TTL_MS = 5000;

function isExpired(signal) {
  const receivedAt = signal.received_at ? new Date(signal.received_at).getTime() : null;
  if (!receivedAt) return false;
  return (Date.now() - receivedAt) > SIGNAL_TTL_MS;
}

function getLatencyMs(signal) {
  const receivedAt = signal.received_at ? new Date(signal.received_at).getTime() : null;
  if (!receivedAt) return null;
  return Date.now() - receivedAt;
}

// ── DEDUPLICATION ─────────────────────────────────────────────────────
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

// ── READ OBJECT — extract data from any object type ──────────────────
// Handles plain objects, protobuf messages, CTraderLayerEvent wrappers,
// and anything else. Returns a plain JS object with enumerable data.
function readObject(obj) {
  if (!obj) return {};
  // Try toObject() — standard protobufjs method
  if (typeof obj.toObject === 'function') {
    try { return obj.toObject(); } catch(_) {}
  }
  // Try toJSON() — alternative protobufjs method
  if (typeof obj.toJSON === 'function') {
    try { return obj.toJSON(); } catch(_) {}
  }
  // Check for Symbol-keyed properties (CTraderLayerEvent may use these)
  const symbols = Object.getOwnPropertySymbols(obj);
  if (symbols.length > 0) {
    const out = {};
    for (const sym of symbols) {
      try { out[sym.toString()] = obj[sym]; } catch(_) {}
    }
    return out;
  }
  // Check prototype chain for getters
  const proto = Object.getPrototypeOf(obj);
  if (proto && proto !== Object.prototype) {
    const protoProps = Object.getOwnPropertyNames(proto)
      .filter(k => k !== 'constructor');
    if (protoProps.length > 0) {
      const out = {};
      for (const k of protoProps) {
        try {
          const desc = Object.getOwnPropertyDescriptor(proto, k);
          if (desc && typeof desc.get === 'function') {
            out[k] = obj[k];
          }
        } catch(_) {}
      }
      if (Object.keys(out).length > 0) return out;
    }
  }
  // Fallback: plain JSON copy
  try { return JSON.parse(JSON.stringify(obj)); } catch(_) {}
  return {};
}

// ── SUPABASE LOGGING ──────────────────────────────────────────────────
async function logSignal(signal, result, status, errorMsg = null, latencyMs = null) {
  try {
    const { data, error } = await supabase.from('signal_log').insert({
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
      is_paper:      IS_PAPER,
      latency_ms:    latencyMs,
    }).select('id').single();

    if (error) throw error;
    console.log('Logged:', status, latencyMs ? `(${latencyMs}ms)` : '', '| dbId:', data?.id);
    return data?.id || null;
  } catch(e) {
    console.error('Supabase signal_log error:', e.message);
    return null;
  }
}

async function logHealth(status, tokenDaysLeft = null) {
  try {
    await supabase.from('health_log').insert({
      status,
      token_expiry_days: tokenDaysLeft,
      logged_at: new Date().toISOString(),
    });
  } catch(e) { console.error('Supabase health_log error:', e.message); }
}

async function logAlert(alertType, severity, message) {
  console.log(`ALERT [${severity}] ${alertType}: ${message}`);
  try {
    await supabase.from('alerts').insert({
      alert_type: alertType, severity, message,
      created_at: new Date().toISOString(),
    });
  } catch(e) { console.error('Supabase alerts error:', e.message); }
}

async function logError(signal, errorCode, errorDesc, rawEvent = null) {
  try {
    await supabase.from('recent_errors').insert({
      error_time:  new Date().toISOString(),
      signal_id:   signal ? signal.signal_id : null,
      ticker:      signal ? signal.ticker    : null,
      action:      signal ? signal.action    : null,
      error_code:  errorCode,
      error_desc:  errorDesc,
      raw_event:   rawEvent ? JSON.stringify(rawEvent).substring(0, 1000) : null,
      is_paper:    IS_PAPER,
    });
  } catch(e) { console.error('Supabase recent_errors error:', e.message); }
}

// ── SYMBOL MAP ────────────────────────────────────────────────────────
const SYMBOL_MAP = {
  "XAUUSD": "XAUUSD",
  "BTCUSD": "BTCUSD",
  "NAS100": "NAS100",
  "USOIL":  "XTIUSD",
};

// ── CTRADER CONNECTION ────────────────────────────────────────────────
let connection   = null;
let symbolIdMap  = {};
let isConnected  = false;
let reconnecting = false;

async function connectToCTrader() {
  if (reconnecting) return;
  reconnecting = true;
  isConnected  = false;

  try {
    console.log("Connecting to cTrader...");
    connection = new CTraderConnection({ host: HOST, port: 5035 });

    connection.on('close', () => {
      console.warn("cTrader WebSocket closed — scheduling reconnect");
      isConnected  = false;
      reconnecting = false;
      logAlert('WEBSOCKET_CLOSED', 'WARN', 'cTrader connection closed. Reconnecting...');
      setTimeout(connectToCTrader, 3000);
    });

    connection.on('error', (err) => {
      console.error("cTrader WebSocket error:", err.message);
      isConnected = false;
    });

    await connection.open();
    console.log("WebSocket connected");

    await connection.sendCommand('ProtoOAApplicationAuthReq', {
      clientId: CLIENT_ID, clientSecret: CLIENT_SECRET,
    });
    console.log("Application authenticated");

    await connection.sendCommand('ProtoOAAccountAuthReq', {
      ctidTraderAccountId: ACCOUNT_ID,
      accessToken: currentAccessToken,
    });
    console.log("Account authenticated:", ACCOUNT_ID);

    const symRes = await connection.sendCommand('ProtoOASymbolsListReq', {
      ctidTraderAccountId: ACCOUNT_ID,
      includeArchivedSymbols: false,
    });
    symbolIdMap = {};
    (symRes.symbol || []).forEach(s => { symbolIdMap[s.symbolName] = s.symbolId; });
    console.log("Symbols loaded:", Object.keys(symbolIdMap).length);

    setInterval(() => connection.sendHeartbeat(), 25000);

    isConnected  = true;
    reconnecting = false;
    console.log("=== ENGINE READY | Mode:", IS_PAPER ? "PAPER" : "LIVE", "===");
    await logAlert('ENGINE_READY', 'INFO', `Engine v2.2 connected. Mode: ${IS_PAPER ? 'PAPER' : 'LIVE'}`);

  } catch(err) {
    console.error("cTrader connection failed:", err.message);
    reconnecting = false;
    logAlert('CONNECTION_FAILED', 'CRITICAL', err.message);
    setTimeout(connectToCTrader, 5000);
  }
}

// ── PIPELINE WASHDOWN ─────────────────────────────────────────────────
async function washdownQueue() {
  try {
    let flushed = 0;
    while (true) {
      const res  = await fetch(`${UPSTASH_URL}/rpop/hawk:signals`,
        { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } });
      const data = await res.json();
      if (!data.result) break;
      flushed++;
    }
    if (flushed > 0) {
      console.log(`Pipeline washdown: flushed ${flushed} stale signals`);
      await logAlert('PIPELINE_RESET', 'WARN',
        `Startup washdown flushed ${flushed} stale signals from previous session.`);
    } else {
      console.log("Pipeline washdown: queue was empty");
    }
  } catch(e) {
    console.error("Washdown error:", e.message);
  }
}

// ── RECONCILE CONFIRMATION ────────────────────────────────────────────
// Called 1.5s after order send if the sendCommand result was uninformative.
// ProtoOAReconcileReq is known to return readable data — used as the
// reliable fallback for confirming execution.
async function reconcileConfirm(dbId, symbolId, isLong) {
  try {
    console.log(`Reconcile check | dbId:${dbId}`);
    const posRes = await connection.sendCommand('ProtoOAReconcileReq', {
      ctidTraderAccountId: ACCOUNT_ID,
    });

    const tradeSide = isLong ? 'BUY' : 'SELL';
    const position  = (posRes.position || []).find(p =>
      String(p.tradeData?.symbolId) === String(symbolId) &&
      p.tradeData?.tradeSide === tradeSide
    );

    if (position) {
      const positionId  = position.positionId ? String(position.positionId) : null;
      const filledVol   = position.tradeData?.volume ?? null;
      // Reconcile does not return fill price — that only comes via deal data.
      // Position is confirmed open; fill_price remains null until deal history queried.
      console.log(`Reconcile CONFIRMED | positionId:${positionId} | vol:${filledVol}`);
      await supabase.from('signal_log').update({
        status:        'EXECUTED',
        position_id:   positionId,
        filled_volume: filledVol,
        exec_type:     'RECONCILE_CONFIRMED',
        api_response:  JSON.stringify({ positionId, filledVol, tradeSide }),
      }).eq('id', dbId);
    } else {
      // No matching position — order was likely rejected or not filled
      console.warn(`Reconcile: no matching ${tradeSide} position for symbolId ${symbolId}`);
      await supabase.from('signal_log').update({
        status:    'RECONCILE_NO_POSITION',
        exec_type: 'RECONCILE_NO_POSITION',
      }).eq('id', dbId);
      await logAlert('ORDER_UNCONFIRMED', 'WARN',
        `Order sent but reconcile found no open position. dbId:${dbId}`);
    }
  } catch(err) {
    console.error('Reconcile confirm error:', err.message);
  }
}

// ── SIGNAL EXECUTION ──────────────────────────────────────────────────
async function executeSignal(signal) {
  const latencyMs = getLatencyMs(signal);

  if (isExpired(signal)) {
    console.warn(`Signal EXPIRED: ${signal.signal_id} | age: ${latencyMs}ms`);
    await logSignal(signal, null, 'EXPIRED', 'Signal age exceeded 5000ms', latencyMs);
    return;
  }

  if (isDuplicate(signal.signal_id)) {
    console.log("Duplicate signal ignored:", signal.signal_id);
    await logSignal(signal, null, 'DUPLICATE', null, latencyMs);
    return;
  }

  if (!isConnected) {
    console.warn("Engine not connected — signal dropped");
    await logSignal(signal, null, 'ERROR', 'Engine not connected', latencyMs);
    return;
  }

  const isEntry  = signal.action === 'LONG'  || signal.action === 'SHORT';
  const isLong   = signal.action === 'LONG'  || signal.action === 'LONG_EXIT'  || signal.action === 'LONG_STOP';
  const ctSymbol = SYMBOL_MAP[signal.ticker] || signal.ticker;
  const symbolId = symbolIdMap[ctSymbol];

  if (!symbolId) {
    console.error("Symbol not found:", ctSymbol);
    await logSignal(signal, null, 'ERROR', 'Symbol not found: ' + ctSymbol, latencyMs);
    return;
  }

  try {
    if (isEntry) {
      // Enforce one position per ticker per side
      const posRes = await connection.sendCommand('ProtoOAReconcileReq', {
        ctidTraderAccountId: ACCOUNT_ID,
      });
      const existing = (posRes.position || []).find(p =>
        String(p.tradeData?.symbolId) === String(symbolId) &&
        (isLong ? p.tradeData?.tradeSide === 'BUY' : p.tradeData?.tradeSide === 'SELL')
      );
      if (existing) {
        console.log("Position already open — skipping entry:", ctSymbol);
        await logSignal(signal, null, 'DUPLICATE_POSITION', null, latencyMs);
        return;
      }

      const volume   = getVolume(parseInt(signal.score));
      const stopLoss = Math.round(parseFloat(signal.atr) * 2 * 100000);

      console.log(`Order | ${ctSymbol} | ${isLong ? "BUY" : "SELL"} | ${volume} cents | SL: ${stopLoss} | latency: ${latencyMs}ms`);

      // Log as PENDING_FILL immediately
      const dbId = await logSignal(
        signal,
        { symbolId, volume, stopLoss },
        'PENDING_FILL',
        null,
        latencyMs
      );

      // ── Strategy A: Await sendCommand ─────────────────────────
      // The library may resolve the promise with execution data.
      // Log the raw result regardless — this is our one remaining
      // diagnostic window to understand what the library returns.
      let orderResult = null;
      let orderError  = null;
      try {
        orderResult = await connection.sendCommand('ProtoOANewOrderReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          symbolId,
          orderType:        'MARKET',
          tradeSide:        isLong ? 'BUY' : 'SELL',
          volume,
          relativeStopLoss: stopLoss,
          comment:          `HAWK|${signal.strategy_id}|S${signal.score}`,
        });

        // Log everything we can extract from the result
        console.log('Order result constructor:', orderResult?.constructor?.name);
        const symbols = orderResult ? Object.getOwnPropertySymbols(orderResult) : [];
        console.log('Order result symbols:', symbols.length);
        const proto = orderResult ? Object.getPrototypeOf(orderResult) : null;
        const protoMethods = proto ? Object.getOwnPropertyNames(proto).filter(k => k !== 'constructor').join(',') : 'none';
        console.log('Order result proto methods:', protoMethods);

        // Attempt all extraction paths
        const extracted = readObject(orderResult);
        console.log('Order result extracted:', JSON.stringify(extracted).substring(0, 500));

        // Check if we got real execution data from the result
        const execType   = extracted.executionType ?? null;
        const order      = extracted.order    || {};
        const position   = extracted.position || {};
        const deal       = extracted.deal     || {};
        const orderId    = order.orderId    ? String(order.orderId)    : (deal.dealId    ? String(deal.dealId)    : null);
        const positionId = position.positionId ? String(position.positionId) : null;
        const fillPrice  = deal.executionPrice != null ? (deal.executionPrice / 100000).toFixed(5) : null;
        const filledVol  = deal.filledVolume ?? null;

        const FILLED_EXEC   = new Set(['ORDER_FILLED', 'ORDER_PARTIAL_FILL', 2, 3]);
        const REJECTED_EXEC = new Set(['ORDER_REJECTED', 'ORDER_CANCEL_REJECTED', 6, 7]);
        const isFilled   = FILLED_EXEC.has(execType);
        const isRejected = REJECTED_EXEC.has(execType);

        if (isFilled || isRejected || orderId || positionId) {
          // Strategy A succeeded — execution data available from sendCommand result
          console.log(`Strategy A succeeded | execType:${execType} | orderId:${orderId} | posId:${positionId} | price:${fillPrice}`);
          await supabase.from('signal_log').update({
            status:        isFilled ? 'EXECUTED' : isRejected ? 'REJECTED' : 'EXECUTED',
            exec_type:     execType ? String(execType) : 'SENDCMD_CONFIRMED',
            order_id:      orderId,
            position_id:   positionId,
            fill_price:    fillPrice,
            filled_volume: filledVol,
            api_response:  JSON.stringify(extracted),
          }).eq('id', dbId);
          return; // Done — no reconcile needed
        }

        // Strategy A returned something but no useful execution fields
        console.log("Strategy A: order sent, no execution fields in result — falling back to reconcile");

      } catch(e) {
        orderError = e.message;
        console.error("Order send error:", e.message);
        await logError(signal, 'SEND_ERROR', e.message, null);
        await supabase.from('signal_log').update({
          status: 'ERROR', error_message: e.message,
        }).eq('id', dbId);
        return;
      }

      // ── Strategy B: Reconcile fallback ────────────────────────
      // sendCommand did not return usable execution data.
      // Wait 1.5s for cTrader to register the position, then reconcile.
      console.log(`Scheduling reconcile confirm | dbId:${dbId} | in 1500ms`);
      setTimeout(() => reconcileConfirm(dbId, symbolId, isLong), 1500);

    } else {
      // ── Exit signal ───────────────────────────────────────────
      const posRes = await connection.sendCommand('ProtoOAReconcileReq', {
        ctidTraderAccountId: ACCOUNT_ID,
      });
      const position = (posRes.position || []).find(p =>
        String(p.tradeData?.symbolId) === String(symbolId) &&
        (isLong ? p.tradeData?.tradeSide === 'BUY' : p.tradeData?.tradeSide === 'SELL')
      );
      if (!position) {
        console.log("No matching position for", signal.action, ctSymbol);
        await logSignal(signal, null, 'NO_POSITION', null, latencyMs);
        return;
      }
      const positionId = position.positionId ? String(position.positionId) : null;
      console.log("Closing position:", positionId, "| latency:", latencyMs + "ms");

      try {
        await connection.sendCommand('ProtoOAClosePositionReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          positionId:          position.positionId,
          volume:              position.tradeData.volume,
        });
        console.log("Close sent to cTrader");
      } catch(e) {
        console.error("Close send error:", e.message);
        await logError(signal, 'CLOSE_ERROR', e.message, null);
      }
      await logSignal(signal, { positionId }, 'CLOSED', null, latencyMs);
    }

  } catch(err) {
    console.error("Execution error:", err.message);
    await logSignal(signal, null, 'ERROR', err.message, latencyMs);
  }
}

// ── HTTP SERVER ───────────────────────────────────────────────────────
function startHttpServer() {
  const app = express();
  app.use(express.json());

  app.post('/signal', async (req, res) => {
    if (req.headers['x-internal-secret'] !== INTERNAL_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const signal = req.body;
    if (!signal || !signal.signal_id) {
      return res.status(400).json({ error: 'Invalid signal' });
    }
    res.status(200).json({ ok: true });
    setImmediate(() => executeSignal(signal));
  });

  app.get('/health', (req, res) => {
    res.json({
      status:  isConnected ? 'CONNECTED' : 'DISCONNECTED',
      mode:    IS_PAPER ? 'PAPER' : 'LIVE',
      uptime:  process.uptime(),
      version: '2.2',
    });
  });

  app.listen(PORT, () => {
    console.log(`HTTP server listening on port ${PORT}`);
  });
}

// ── MAIN ──────────────────────────────────────────────────────────────
async function main() {
  await refreshAccessToken();

  const TWENTY_DAYS_MS = 20 * 24 * 60 * 60 * 1000;
  setInterval(async () => {
    try { await refreshAccessToken(); }
    catch(e) { logAlert('TOKEN_REFRESH_FAILED', 'CRITICAL', e.message); }
  }, TWENTY_DAYS_MS);

  await washdownQueue();
  await connectToCTrader();
  startHttpServer();

  setInterval(async () => {
    const daysLeft = tokenExpiryTime
      ? Math.floor((tokenExpiryTime - Date.now()) / 86400000) : null;
    await logHealth(isConnected ? 'RUNNING' : 'DISCONNECTED', daysLeft);
    if (daysLeft !== null && daysLeft < 2) {
      await logAlert('TOKEN_EXPIRY_CRITICAL', 'CRITICAL',
        `cTrader access token expires in ${daysLeft} days. Immediate action required.`);
    }
  }, 60000);
}

main().catch(err => {
  console.error("Fatal startup error:", err.message);
  process.exit(1);
});
