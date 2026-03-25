// ╔══════════════════════════════════════════════════════════════════╗
// ║  HAWK EXECUTION ENGINE  —  index.js  v2.7                       ║
// ║  Changes vs v2.6:                                                ║
// ║  - SPOTCRUDE removed from SYMBOL_MAP                             ║
// ║  - SPOTBRENT, AUS200, GER40, ETHUSD, XAGUSD, BTCUSD added       ║
// ║  - getVolume() updated with confirmed lot sizes (fallback only)  ║
// ║  - Engine now reads lot_size from payload if present (primary)   ║
// ╚══════════════════════════════════════════════════════════════════╝

'use strict';
const { createClient } = require('@supabase/supabase-js');
const net             = require('net');
const http            = require('http');

// ── ENVIRONMENT ──────────────────────────────────────────────────────────────
const CTRADER_CLIENT_ID    = process.env.CTRADER_CLIENT_ID;
const CTRADER_SECRET       = process.env.CTRADER_CLIENT_SECRET;
const CTRADER_REFRESH      = process.env.CTRADER_REFRESH_TOKEN;
const ACCOUNT_ID           = parseInt(process.env.CTRADER_ACCOUNT_ID || '46630089');
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const INTERNAL_SECRET      = process.env.INTERNAL_SECRET;
const IS_PAPER             = (process.env.IS_PAPER || 'true') !== 'false';
const PORT                 = parseInt(process.env.PORT || '3000');

const CT_HOST = IS_PAPER ? 'demo.ctraderapi.com' : 'live.ctraderapi.com';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── TOKEN MANAGEMENT ─────────────────────────────────────────────────────────
// We refresh automatically 30 seconds before expiry so execution is never
// interrupted. Every Railway redeploy triggers a fresh token refresh and
// consumes the current refresh token — check the logs for the new expiry.
let accessToken   = null;
let tokenExpiry   = 0;

async function getAccessToken() {
  if (accessToken && Date.now() < tokenExpiry - 30000) return accessToken;
  console.log('Refreshing cTrader access token...');
  const res = await fetch('https://connect.spotware.com/apps/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type:    'refresh_token',
      refresh_token: CTRADER_REFRESH,
      client_id:     CTRADER_CLIENT_ID,
      client_secret: CTRADER_SECRET,
    }).toString(),
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Token refresh failed: ' + JSON.stringify(data));
  accessToken  = data.access_token;
  tokenExpiry  = Date.now() + (data.expires_in * 1000);
  const days   = Math.floor(data.expires_in / 86400);
  console.log(`Token refreshed. Expires in ${days} days.`);
  return accessToken;
}

// ── SYMBOL MAP: TradingView ticker → cTrader symbol name ─────────────────────
// Verify each name in your Pepperstone paper account before going live.
// SPOTCRUDE has been removed. SPOTBRENT replaces it.
const SYMBOL_MAP = {
  'XAUUSD':    'XAUUSD',
  'BTCUSD':    'BTCUSD',
  'ETHUSD':    'ETHUSD',
  'XAGUSD':    'XAGUSD',
  'NAS100':    'NAS100',
  'GER40':     'GER40',
  'AUS200':    'AUS200',
  'SPOTBRENT': 'SPOTBRENT',
};

// ── VOLUME FALLBACK: score + ticker → cTrader units ──────────────────────────
// PRIMARY PATH: the engine reads lot_size directly from the signal payload.
//   lot_size is sent by the Pine Script alert as a dynamic variable.
//   cTrader units = lot_size × 100.
//
// FALLBACK PATH (legacy signals without lot_size field): this function is used.
//   Units = lots × 100. Scores 7/8/9 map to the confirmed lot sizes below.
//
// ⚠ NAS100 Score 7 is set to 0 units (0.0 lots) per the provided table.
//   A zero-volume order will be rejected by cTrader. Confirm this is correct
//   before deploying, or update to 100 (1.0 lot) if it was a data entry error.
//
function getVolume(score, ticker) {
  const s = parseInt(score);
  switch (ticker) {
    case 'XAUUSD':
      // 0.04 / 0.05 / 0.06 lots
      return s >= 9 ? 6 : s >= 8 ? 5 : 4;

    case 'BTCUSD':
      // 0.01 / 0.02 / 0.03 lots
      return s >= 9 ? 3 : s >= 8 ? 2 : 1;

    case 'ETHUSD':
      // 0.5 / 0.75 / 1.0 lots
      return s >= 9 ? 100 : s >= 8 ? 75 : 50;

    case 'XAGUSD':
      // 0.1 / 0.2 / 0.3 lots
      return s >= 9 ? 30 : s >= 8 ? 20 : 10;

    case 'NAS100':
      // 1.0 / 1.5 / 2.0 lots — Score 7 = 0 UNITS. Verify this is intentional.
      return s >= 9 ? 200 : s >= 8 ? 150 : 0;

    case 'GER40':
      // 0.5 / 1.0 / 1.5 lots
      return s >= 9 ? 150 : s >= 8 ? 100 : 50;

    case 'AUS200':
      // 1.0 / 1.5 / 2.0 lots
      return s >= 9 ? 200 : s >= 8 ? 150 : 100;

    case 'SPOTBRENT':
      // 1.0 / 1.5 / 2.0 lots
      return s >= 9 ? 200 : s >= 8 ? 150 : 100;

    default:
      console.warn(`[VOLUME WARNING] No fallback rule for ticker: ${ticker}. Defaulting to 1 unit.`);
      return 1;
  }
}

// ── RESOLVE VOLUME FROM SIGNAL ────────────────────────────────────────────────
// Primary path: use lot_size from the Pine Script payload.
// Fallback: use the hardcoded getVolume() lookup above.
function resolveVolume(signal) {
  if (signal.lot_size !== undefined && signal.lot_size !== null && signal.lot_size !== '') {
    const lots = parseFloat(signal.lot_size);
    if (!isNaN(lots) && lots > 0) {
      const units = Math.round(lots * 100);
      console.log(`[VOLUME] Using payload lot_size: ${lots} lots = ${units} units`);
      return units;
    }
  }
  // Fallback to hardcoded lookup
  const units = getVolume(signal.score, signal.ticker);
  console.log(`[VOLUME] Using fallback getVolume(): score=${signal.score} ticker=${signal.ticker} = ${units} units`);
  return units;
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
async function logSignal(signal, result, status, errorMsg = null, latencyMs = null) {
  try {
    await supabase.from('signal_log').insert({
      signal_id:     signal.signal_id || signal.timestamp,
      strategy_id:   signal.strategy_id,
      ticker:        signal.ticker,
      action:        signal.action,
      score:         parseInt(signal.score),
      atr:           parseFloat(signal.atr),
      close_price:   parseFloat(signal.close),
      status,
      error_message: errorMsg,
      api_response:  result ? JSON.stringify(result) : null,
      signal_time:   new Date(parseInt(signal.timestamp) || Date.now()).toISOString(),
      processed_at:  new Date().toISOString(),
      is_paper:      IS_PAPER,
      latency_ms:    latencyMs,
    });
  } catch (err) {
    console.error('[SUPABASE LOG ERROR]', err.message);
  }
}

async function logHealth(status, tokenDaysLeft = null) {
  try {
    await supabase.from('health_log').insert({
      status,
      token_days_left: tokenDaysLeft,
      checked_at:      new Date().toISOString(),
      is_paper:        IS_PAPER,
    });
  } catch (err) {
    console.error('[SUPABASE HEALTH ERROR]', err.message);
  }
}

async function logAlert(code, severity, message) {
  try {
    await supabase.from('alerts').insert({
      code, severity, message,
      created_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[SUPABASE ALERT ERROR]', err.message);
  }
}

// ── cTRADER PROTOBUF HELPERS ─────────────────────────────────────────────────
// Minimal Protobuf encoding for the messages we need.

function encodeVarint(value) {
  const bytes = [];
  while (value > 0x7F) {
    bytes.push((value & 0x7F) | 0x80);
    value >>>= 7;
  }
  bytes.push(value & 0x7F);
  return Buffer.from(bytes);
}

function encodeField(fieldNum, wireType, value) {
  const tag = (fieldNum << 3) | wireType;
  if (wireType === 0) {
    return Buffer.concat([encodeVarint(tag), encodeVarint(value)]);
  } else if (wireType === 2) {
    const buf = Buffer.isBuffer(value) ? value : Buffer.from(value);
    return Buffer.concat([encodeVarint(tag), encodeVarint(buf.length), buf]);
  }
  throw new Error('Unsupported wire type: ' + wireType);
}

function encodeLong(value) {
  // Encode a 64-bit int as two 32-bit parts for Protobuf varint
  const lo = value >>> 0;
  const hi = Math.floor(value / 0x100000000) >>> 0;
  const bytes = [];
  let combined = lo;
  for (let i = 0; i < 4; i++) {
    if (i === 3 && hi > 0) {
      bytes.push(((combined & 0x0F) | ((hi & 0x07) << 4)) | 0x80);
      combined = hi >> 3;
    } else {
      bytes.push((combined & 0x7F) | (combined > 0x7F || (i === 3 && hi > 0) ? 0x80 : 0));
      combined >>>= 7;
    }
  }
  // Simpler approach: just use BigInt if available
  return encodeVarint(value);
}

// Build ProtoOAApplicationAuthReq  (payloadType = 2100)
function buildAppAuthReq(clientId, clientSecret) {
  const body = Buffer.concat([
    encodeField(1, 2, clientId),
    encodeField(2, 2, clientSecret),
  ]);
  return (2100, body);
}

// Build ProtoOAAccountAuthReq  (payloadType = 2102)
function buildAccountAuthReq(accountId, accessToken) {
  const body = Buffer.concat([
    encodeField(1, 0, accountId),
    encodeField(2, 2, accessToken),
  ]);
  return (2102, body);
}

// Build ProtoOASymbolsListReq  (payloadType = 2115)
function buildSymbolsListReq(accountId) {
  const body = encodeField(1, 0, accountId);
  return (2115, body);
}

// Build ProtoOANewOrderReq  (payloadType = 2106)
function buildNewOrderReq(accountId, symbolId, tradeSide, volume, relativeStopLoss) {
  const MARKET_ORDER = 1;
  const body = Buffer.concat([
    encodeField(1, 0, accountId),
    encodeField(2, 0, symbolId),
    encodeField(3, 0, MARKET_ORDER),   // orderType = MARKET
    encodeField(4, 0, tradeSide),       // 1 = BUY, 2 = SELL
    encodeField(5, 0, volume),          // cTrader units
    encodeField(14, 0, relativeStopLoss), // relative SL in points/pips
  ]);
  return (2106, body);
}

// Build ProtoOAReconcileReq  (payloadType = 2124)
function buildReconcileReq(accountId) {
  const body = encodeField(1, 0, accountId);
  return (2124, body);
}

// Build ProtoOAClosePositionReq  (payloadType = 2139)
function buildClosePositionReq(accountId, positionId, volume) {
  const body = Buffer.concat([
    encodeField(1, 0, accountId),
    encodeField(2, 0, positionId),
    encodeField(3, 0, volume),
  ]);
  return (2139, body);
}

// Build ProtoMessage wrapper
function buildProtoMessage(payloadType, payload) {
  const payloadTypeField = encodeField(1, 0, payloadType);
  const payloadField     = encodeField(2, 2, payload);
  const message          = Buffer.concat([payloadTypeField, payloadField]);
  const lengthBuf        = Buffer.alloc(4);
  lengthBuf.writeUInt32BE(message.length, 0);
  return Buffer.concat([lengthBuf, message]);
}

// ── cTRADER WebSocket / TCP CONNECTION ────────────────────────────────────────
let socket         = null;
let isConnected    = false;
let symbolMap      = {};   // ticker → symbolId
let pendingResolve = null;
let pendingReject  = null;
let receiveBuffer  = Buffer.alloc(0);
let tokenExpiryTime = null;

function connectToCTrader() {
  return new Promise((resolve, reject) => {
    console.log(`Connecting to cTrader (${CT_HOST}:5035)...`);
    socket = new net.Socket();

    socket.connect(5035, CT_HOST, async () => {
      console.log('TCP connected. Authenticating application...');
      try {
        const token = await getAccessToken();
        tokenExpiryTime = tokenExpiry;

        // 1. App auth
        await sendAndWait(buildAppAuthReq(CTRADER_CLIENT_ID, CTRADER_SECRET), 2101);
        console.log('Application authenticated');

        // 2. Account auth
        await sendAndWait(buildAccountAuthReq(ACCOUNT_ID, token), 2103);
        console.log(`Account authenticated: ${ACCOUNT_ID}`);

        // 3. Load symbols
        await loadSymbols();
        console.log(`Symbols loaded: ${Object.keys(symbolMap).length}`);

        isConnected = true;
        console.log(`=== ENGINE READY | Mode: ${IS_PAPER ? 'PAPER' : 'LIVE'} ===`);
        await logHealth('RUNNING');
        resolve();
      } catch (err) {
        reject(err);
      }
    });

    socket.on('data', (chunk) => {
      receiveBuffer = Buffer.concat([receiveBuffer, chunk]);
      processReceiveBuffer();
    });

    socket.on('error', async (err) => {
      console.error('[TCP ERROR]', err.message);
      isConnected = false;
      await logHealth('DISCONNECTED');
      scheduleReconnect();
    });

    socket.on('close', async () => {
      console.warn('[TCP CLOSED] Scheduling reconnect...');
      isConnected = false;
      await logHealth('DISCONNECTED');
      scheduleReconnect();
    });
  });
}

let reconnectTimeout = null;
function scheduleReconnect() {
  if (reconnectTimeout) return;
  reconnectTimeout = setTimeout(async () => {
    reconnectTimeout = null;
    try {
      await connectToCTrader();
      console.log('[RECONNECTED]');
      await logHealth('RECONNECTED');
    } catch (err) {
      console.error('[RECONNECT FAILED]', err.message);
      scheduleReconnect();
    }
  }, 3000);
}

function processReceiveBuffer() {
  while (receiveBuffer.length >= 4) {
    const msgLen = receiveBuffer.readUInt32BE(0);
    if (receiveBuffer.length < 4 + msgLen) break;
    const msgBuf = receiveBuffer.slice(4, 4 + msgLen);
    receiveBuffer = receiveBuffer.slice(4 + msgLen);
    handleIncomingMessage(msgBuf);
  }
}

function handleIncomingMessage(buf) {
  // Extract payloadType (field 3, varint) and payload (field 5, bytes)
  let payloadType = null;
  let payload     = null;
  let pos = 0;
  while (pos < buf.length) {
    const tagByte  = buf[pos++];
    const fieldNum = tagByte >> 3;
    const wireType = tagByte & 0x07;
    if (wireType === 0) {
      let val = 0, shift = 0;
      while (true) {
        const b = buf[pos++];
        val |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      if (fieldNum === 3) payloadType = val;
    } else if (wireType === 2) {
      let len = 0, shift = 0;
      while (true) {
        const b = buf[pos++];
        len |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      const data = buf.slice(pos, pos + len);
      pos += len;
      if (fieldNum === 5) payload = data;
    } else {
      break; // unknown wire type — stop
    }
  }

  if (pendingResolve && payloadType !== null) {
    pendingResolve({ payloadType, payload });
    pendingResolve = null;
    pendingReject  = null;
  }
}

function sendAndWait(msg, expectedType) {
  return new Promise((resolve, reject) => {
    pendingResolve = (resp) => {
      if (resp.payloadType === expectedType || resp.payloadType === 50) {
        resolve(resp);
      } else {
        // Accept any response — let caller decide
        resolve(resp);
      }
    };
    pendingReject = reject;
    socket.write(msg);
    setTimeout(() => {
      if (pendingReject) {
        pendingReject(new Error(`Timeout waiting for payloadType ${expectedType}`));
        pendingResolve = null;
        pendingReject  = null;
      }
    }, 8000);
  });
}

async function loadSymbols() {
  const resp = await sendAndWait(buildSymbolsListReq(ACCOUNT_ID), 2116);
  if (!resp.payload) return;
  // Parse symbols list — field 2 repeated: {field1=symbolId, field3=name}
  const buf = resp.payload;
  let pos = 0;
  while (pos < buf.length) {
    if (pos >= buf.length) break;
    const tagByte  = buf[pos++];
    const fieldNum = tagByte >> 3;
    const wireType = tagByte & 0x07;
    if (wireType === 2) {
      let len = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        len |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      if (fieldNum === 2) {
        // Parse each symbol entry
        const symBuf = buf.slice(pos, pos + len);
        const sym    = parseSymbolEntry(symBuf);
        if (sym.id && sym.name) symbolMap[sym.name] = sym.id;
      }
      pos += len;
    } else if (wireType === 0) {
      while (pos < buf.length && (buf[pos++] & 0x80));
    } else break;
  }
}

function parseSymbolEntry(buf) {
  let id = null, name = null, pos = 0;
  while (pos < buf.length) {
    const tagByte  = buf[pos++];
    const fieldNum = tagByte >> 3;
    const wireType = tagByte & 0x07;
    if (wireType === 0) {
      let val = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        val |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      if (fieldNum === 1) id = val;
    } else if (wireType === 2) {
      let len = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        len |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      const str = buf.slice(pos, pos + len).toString('utf8');
      pos += len;
      if (fieldNum === 3) name = str;
    } else break;
  }
  return { id, name };
}

// ── UPSTASH QUEUE WASHDOWN (on startup) ──────────────────────────────────────
async function washdownQueue() {
  try {
    const res = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/llen/hawk:signals`, {
      headers: { Authorization: `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` },
    });
    const data = await res.json();
    const depth = data.result || 0;
    if (depth > 0) {
      console.warn(`[WASHDOWN] Flushing ${depth} stale signals from queue...`);
      await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/del/hawk:signals`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` },
      });
    } else {
      console.log('Pipeline washdown: queue was empty');
    }
  } catch (err) {
    console.warn('[WASHDOWN] Could not flush queue:', err.message);
  }
}

// ── SIGNAL PROCESSING ────────────────────────────────────────────────────────
async function processSignal(signal, receivedAt) {
  const startMs    = Date.now();
  const latencyMs  = startMs - receivedAt;

  // Stale signal check (> 5 seconds old)
  if (latencyMs > 5000) {
    console.warn(`[EXPIRED] Signal ${signal.ticker} ${signal.action} age=${latencyMs}ms — skipped`);
    await logSignal(signal, null, 'EXPIRED', `Signal age ${latencyMs}ms exceeds 5000ms limit`, latencyMs);
    return;
  }

  // Deduplication
  const sigId = signal.signal_id || signal.timestamp;
  if (isDuplicate(sigId)) {
    console.warn(`[DUPLICATE] ${signal.ticker} ${signal.action} — skipped`);
    return;
  }

  // Symbol lookup
  const ctSymbol = SYMBOL_MAP[signal.ticker];
  if (!ctSymbol) {
    console.error(`[SYMBOL ERROR] No mapping for ticker: ${signal.ticker}`);
    await logSignal(signal, null, 'FAILED', `No SYMBOL_MAP entry for: ${signal.ticker}`, latencyMs);
    return;
  }
  const symbolId = symbolMap[ctSymbol];
  if (!symbolId) {
    console.error(`[SYMBOL ERROR] ${ctSymbol} not found in cTrader symbol list`);
    await logSignal(signal, null, 'FAILED', `cTrader symbol not found: ${ctSymbol}`, latencyMs);
    return;
  }

  const action  = signal.action;
  const isEntry = action === 'LONG' || action === 'SHORT';
  const isExit  = action === 'LONG_EXIT' || action === 'SHORT_EXIT' ||
                  action === 'LONG_STOP'  || action === 'SHORT_STOP';

  console.log(`[SIGNAL] ${signal.ticker} ${action} score=${signal.score} age=${latencyMs}ms`);

  if (!isConnected) {
    console.error('[ENGINE] Not connected to cTrader — signal dropped');
    await logSignal(signal, null, 'FAILED', 'Engine not connected to cTrader', latencyMs);
    return;
  }

  try {
    if (isEntry) {
      await placeEntry(signal, symbolId, latencyMs);
    } else if (isExit) {
      await placeExit(signal, symbolId, latencyMs);
    } else {
      console.warn(`[UNKNOWN ACTION] ${action} — skipped`);
    }
  } catch (err) {
    console.error(`[EXECUTE ERROR] ${signal.ticker} ${action}:`, err.message);
    await logSignal(signal, null, 'FAILED', err.message, latencyMs);
  }
}

async function placeEntry(signal, symbolId, latencyMs) {
  const volume   = resolveVolume(signal);

  if (volume === 0) {
    console.warn(`[ZERO VOLUME] ${signal.ticker} score=${signal.score} — order skipped (0 units). Check lot size configuration.`);
    await logSignal(signal, null, 'SKIPPED', 'Zero volume — no order placed. Review lot size for this score/ticker combination.', latencyMs);
    return;
  }

  const isLong     = signal.action === 'LONG';
  const tradeSide  = isLong ? 1 : 2;
  const atr        = parseFloat(signal.atr) || 0;
  // relativeStopLoss in cTrader = points (price × 100 for most instruments)
  // ATR is already in price units. Multiply by 2 for 2× ATR stop.
  const stopPoints = Math.round(atr * 2 * 100);

  console.log(`[ORDER] Placing ${signal.action} | ${signal.ticker} | ${volume} units | SL: ${stopPoints} pts`);

  const orderMsg = buildNewOrderReq(ACCOUNT_ID, symbolId, tradeSide, volume, stopPoints);
  const resp     = await sendAndWait(orderMsg, 2108);

  // Confirm via reconcile 1.5s later
  await new Promise(r => setTimeout(r, 1500));
  const reconMsg  = buildReconcileReq(ACCOUNT_ID);
  const reconResp = await sendAndWait(reconMsg, 2125);

  const totalLatency = Date.now() - (Date.now() - latencyMs);
  console.log(`[EXECUTED] ${signal.ticker} ${signal.action} | ${volume} units`);
  await logSignal(signal, { resp: resp.payloadType }, 'EXECUTED', null, latencyMs);
}

async function placeExit(signal, symbolId, latencyMs) {
  // Reconcile to find open position for this symbol
  const reconMsg  = buildReconcileReq(ACCOUNT_ID);
  const reconResp = await sendAndWait(reconMsg, 2125);

  // Parse positions from reconcile response
  const position = findPosition(reconResp.payload, symbolId);
  if (!position) {
    console.warn(`[EXIT] No open position found for ${signal.ticker} — skipped`);
    await logSignal(signal, null, 'SKIPPED', `No open position for ${signal.ticker}`, latencyMs);
    return;
  }

  console.log(`[EXIT] Closing position ${position.id} | ${signal.ticker} | ${position.volume} units`);
  const closeMsg = buildClosePositionReq(ACCOUNT_ID, position.id, position.volume);
  const closeResp = await sendAndWait(closeMsg, 2140);

  console.log(`[CLOSED] ${signal.ticker} ${signal.action}`);
  await logSignal(signal, { resp: closeResp.payloadType }, 'EXECUTED', null, latencyMs);
}

function findPosition(buf, targetSymbolId) {
  if (!buf) return null;
  // ProtoOAReconcileRes: field 2 = position[] {field1=positionId, field2=tradeData{symbolId,volume}}
  // Simplified parse — extract first position matching symbolId
  let pos = 0;
  while (pos < buf.length) {
    if (pos >= buf.length) break;
    const tagByte  = buf[pos++];
    const fieldNum = tagByte >> 3;
    const wireType = tagByte & 0x07;
    if (wireType === 2) {
      let len = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        len |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      if (fieldNum === 2) {
        const posEntry = parsePositionEntry(buf.slice(pos, pos + len), targetSymbolId);
        if (posEntry) return posEntry;
      }
      pos += len;
    } else if (wireType === 0) {
      while (pos < buf.length && (buf[pos++] & 0x80));
    } else break;
  }
  return null;
}

function parsePositionEntry(buf, targetSymbolId) {
  let positionId = null, symbolId = null, volume = null;
  let pos = 0;
  while (pos < buf.length) {
    const tagByte  = buf[pos++];
    const fieldNum = tagByte >> 3;
    const wireType = tagByte & 0x07;
    if (wireType === 0) {
      let val = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        val |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      if (fieldNum === 1) positionId = val;
    } else if (wireType === 2) {
      let len = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        len |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      if (fieldNum === 2) {
        // tradeData
        const tradeData = parseTradeData(buf.slice(pos, pos + len));
        symbolId = tradeData.symbolId;
        volume   = tradeData.volume;
      }
      pos += len;
    } else break;
  }
  if (symbolId === targetSymbolId && positionId) return { id: positionId, volume: volume || 0 };
  return null;
}

function parseTradeData(buf) {
  let symbolId = null, volume = null, pos = 0;
  while (pos < buf.length) {
    const tagByte  = buf[pos++];
    const fieldNum = tagByte >> 3;
    const wireType = tagByte & 0x07;
    if (wireType === 0) {
      let val = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        val |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      if (fieldNum === 2) symbolId = val;
      if (fieldNum === 3) volume   = val;
    } else if (wireType === 2) {
      let len = 0, shift = 0;
      while (pos < buf.length) {
        const b = buf[pos++];
        len |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
      }
      pos += len;
    } else break;
  }
  return { symbolId, volume };
}

// ── HTTP SERVER (primary signal path from Cloudflare Worker) ─────────────────
function startHttpServer() {
  const server = http.createServer(async (req, res) => {
    if (req.method !== 'POST' || req.url !== '/signal') {
      res.writeHead(404); res.end(); return;
    }
    const secret = req.headers['x-internal-secret'];
    if (secret !== INTERNAL_SECRET) {
      res.writeHead(401); res.end('Unauthorized'); return;
    }
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const signal     = JSON.parse(body);
        const receivedAt = signal.received_at ? new Date(signal.received_at).getTime() : Date.now();
        res.writeHead(200); res.end('OK');
        await processSignal(signal, receivedAt);
      } catch (err) {
        res.writeHead(400); res.end('Bad Request');
        console.error('[HTTP PARSE ERROR]', err.message);
      }
    });
  });
  server.listen(PORT, () => {
    console.log(`HTTP server listening on port ${PORT}`);
  });
}

// ── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`╔════════════════════════════════════════════╗`);
  console.log(`║  HAWK Execution Engine v2.7 STARTED        ║`);
  console.log(`║  Mode: ${IS_PAPER ? 'PAPER (safe to test)    ' : 'LIVE  — REAL MONEY!    '}  ║`);
  console.log(`╚════════════════════════════════════════════╝`);

  await washdownQueue();
  await connectToCTrader();
  startHttpServer();

  // Heartbeat every 60 seconds
  setInterval(async () => {
    const daysLeft = tokenExpiryTime
      ? Math.floor((tokenExpiryTime - Date.now()) / 86400000)
      : null;
    await logHealth(isConnected ? 'RUNNING' : 'DISCONNECTED', daysLeft);
    if (daysLeft !== null && daysLeft < 7) {
      await logAlert('TOKEN_EXPIRY_WARNING', 'WARN',
        `cTrader access token expires in ${daysLeft} days.`);
    }
    if (daysLeft !== null && daysLeft < 2) {
      await logAlert('TOKEN_EXPIRY_CRITICAL', 'CRITICAL',
        `cTrader access token expires in ${daysLeft} days. Immediate action required.`);
    }
  }, 60000);
}

main().catch(err => {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
