// Hawk Execution Engine — index.js v2.1
// Resilient FAF Pipeline — Direct HTTP + Auto-reconnect + Token refresh + Full observability
"use strict";

const { CTraderConnection } = require("@reiryoku/ctrader-layer");
const { createClient }      = require("@supabase/supabase-js");
const express               = require("express");

console.log("=== HAWK ENGINE v2.1 STARTING ===");

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

// ── SUPABASE LOGGING ──────────────────────────────────────────────────
async function logSignal(signal, result, status, errorMsg = null, latencyMs = null) {
  try {
    await supabase.from('signal_log').insert({
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
    });
    console.log('Logged:', status, latencyMs ? `(${latencyMs}ms)` : '');
  } catch(e) { console.error('Supabase signal_log error:', e.message); }
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
      ticker:      signal ? signal.ticker : null,
      action:      signal ? signal.action : null,
      error_code:  errorCode,
      error_desc:  errorDesc,
      raw_event:   rawEvent,
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

    // TEMPORARY DIAGNOSTIC
    connection.on('ProtoOAExecutionEvent', (...args) => console.log('DIAG A:', args[0].toObject ? JSON.stringify(args[0].toObject()).substring(0, 500) : 'no toObject method'));
    connection.on('execution', (e) => console.log('DIAG B:', JSON.stringify(e).substring(0, 300)));
    connection.on('ProtoOAOrderErrorEvent', (e) => console.log('DIAG ERR:', JSON.stringify(e).substring(0, 300)));
    
    // ── D3: Execution confirmation deserialisation ────────────────
    connection.on('ProtoOAExecutionEvent', async (e) => {
      try {
        const execType  = e.executionType;
        const order     = e.order    || {};
        const position  = e.position || {};
        const deal      = e.deal     || {};

        if (!['ORDER_FILLED', 'ORDER_PARTIAL_FILL', 'POSITION_CLOSED',
              'POSITION_PARTIAL_CLOSE'].includes(execType)) return;

        const orderId    = order.orderId    || deal.dealId   || null;
        const positionId = position.positionId               || null;
        const fillPrice  = deal.executionPrice               || null;
        const filledVol  = deal.filledVolume                 || null;

        console.log(`Execution confirmed | type:${execType} | orderId:${orderId} | price:${fillPrice} | vol:${filledVol}`);

        if (orderId || positionId) {
          // Fetch the most recent unconfirmed EXECUTED row
          const { data: rows } = await supabase
            .from('signal_log')
            .select('id')
            .eq('status', 'EXECUTED')
            .is('order_id', null)
            .order('processed_at', { ascending: false })
            .limit(1);

          if (rows && rows.length > 0) {
            const { error } = await supabase
              .from('signal_log')
              .update({
                fill_price:    fillPrice,
                filled_volume: filledVol,
                order_id:      orderId,
                position_id:   positionId,
                exec_type:     execType,
              })
              .eq('id', rows[0].id);

            if (error) console.error('Execution update error:', error.message);
            else console.log('Execution detail logged | id:', rows[0].id);
          }
        }
      } catch(err) {
        console.error('Execution event error:', err.message);
      }
    });

    // ── D6: Order rejection capture ───────────────────────────────
    connection.on('ProtoOAOrderErrorEvent', async (e) => {
      const code = e.errorCode    || 'UNKNOWN';
      const desc = e.description  || '';
      console.error(`Order rejected | code:${code} | ${desc}`);
      await logError(null, code, desc, e);
      await logAlert('ORDER_REJECTED', 'CRITICAL', `Order rejected: ${code} — ${desc}`);
    });

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
    await logAlert('ENGINE_READY', 'INFO', `Engine v2.1 connected. Mode: ${IS_PAPER ? 'PAPER' : 'LIVE'}`);

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

// ── SIGNAL EXECUTION ─────────────────────────────────────────────────
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
      const posRes = await connection.sendCommand('ProtoOAReconcileReq', {
        ctidTraderAccountId: ACCOUNT_ID,
      });
      const existing = (posRes.position || []).find(p =>
        p.tradeData?.symbolId === symbolId &&
        (isLong ? p.tradeData?.tradeSide === 'BUY' : p.tradeData?.tradeSide === 'SELL')
      );
      if (existing) {
        console.log("Position already open — skipping entry:", ctSymbol);
        await logSignal(signal, null, 'DUPLICATE_POSITION', null, latencyMs);
        return;
      }

      const volume   = getVolume(parseInt(signal.score));
      const stopLoss = Math.round(parseFloat(signal.atr) * 2 * 100000);

      console.log(`Order | ${ctSymbol} | ${signal.action === "LONG" ? "BUY" : "SELL"} | ${volume} cents | SL: ${stopLoss} | latency: ${latencyMs}ms`);

      connection.sendCommand('ProtoOANewOrderReq', {
        ctidTraderAccountId: ACCOUNT_ID,
        symbolId,
        orderType:        'MARKET',
        tradeSide:        signal.action === 'LONG' ? 'BUY' : 'SELL',
        volume,
        relativeStopLoss: stopLoss,
        comment:          `HAWK|${signal.strategy_id}|S${signal.score}`,
      }).then(() => {
        console.log("Order sent successfully");
      }).catch(async (e) => {
        console.error("Order send error:", e.message);
        await logError(signal, 'SEND_ERROR', e.message, null);
      });

      await logSignal(signal, { symbolId, volume, stopLoss }, 'EXECUTED', null, latencyMs);

    } else {
      const posRes = await connection.sendCommand('ProtoOAReconcileReq', {
        ctidTraderAccountId: ACCOUNT_ID,
      });
      const position = (posRes.position || []).find(p =>
        p.tradeData?.symbolId === symbolId &&
        (isLong ? p.tradeData?.tradeSide === 'BUY' : p.tradeData?.tradeSide === 'SELL')
      );
      if (!position) {
        console.log("No matching position for", signal.action, ctSymbol);
        await logSignal(signal, null, 'NO_POSITION', null, latencyMs);
        return;
      }
      console.log("Closing position:", position.positionId, "| latency:", latencyMs + "ms");
      connection.sendCommand('ProtoOAClosePositionReq', {
        ctidTraderAccountId: ACCOUNT_ID,
        positionId:          position.positionId,
        volume:              position.tradeData.volume,
      }).then(() => {
        console.log("Close sent successfully");
      }).catch(async (e) => {
        console.error("Close send error:", e.message);
        await logError(signal, 'CLOSE_ERROR', e.message, null);
      });
      await logSignal(signal, { positionId: position.positionId }, 'CLOSED', null, latencyMs);
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
      version: '2.1',
    });
  });

  app.listen(PORT, () => {
    console.log(`HTTP server listening on port ${PORT}`);
  });
}

// ── MAIN ─────────────────────────────────────────────────────────────
async function main() {
  // 1. Refresh token once on startup
  await refreshAccessToken();

  // 2. Schedule token refresh every 20 days (safe within 32-bit integer limit)
  const TWENTY_DAYS_MS = 20 * 24 * 60 * 60 * 1000; // 1,728,000,000ms
  setInterval(async () => {
    try { await refreshAccessToken(); }
    catch(e) { logAlert('TOKEN_REFRESH_FAILED', 'CRITICAL', e.message); }
  }, TWENTY_DAYS_MS);

  // 3. Flush stale signals from previous session
  await washdownQueue();

  // 4. Connect to cTrader
  await connectToCTrader();

  // 5. Start HTTP server
  startHttpServer();

  // 6. Heartbeat every 60 seconds
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
