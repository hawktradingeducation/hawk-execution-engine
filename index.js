'use strict';

const { CTraderConnection } = require('@reiryoku/ctrader-layer');
const { createClient }      = require('@supabase/supabase-js');
const express               = require('express');

console.log('=== HAWK ENGINE v2.13 STARTING ===');

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

console.log('IS_PAPER:', IS_PAPER, '| HOST:', HOST, '| ACCOUNT_ID:', ACCOUNT_ID);

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

let currentAccessToken = null;
let tokenExpiryTime    = null;

// ─── TOKEN ────────────────────────────────────────────────────────────────────

async function refreshAccessToken() {
  console.log('Refreshing cTrader access token...');
  const res = await fetch('https://connect.spotware.com/apps/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type:    'refresh_token',
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
  console.log('Access token refreshed. Expires in', daysLeft, 'days');
  await logHealth('RUNNING', daysLeft);
  if (daysLeft < 7) {
    await logAlert('TOKEN_EXPIRY_WARNING', 'WARN',
      'cTrader access token expires in ' + daysLeft + ' days.');
  }
  return currentAccessToken;
}

// ─── SYMBOL MAP ───────────────────────────────────────────────────────────────

const SYMBOL_MAP = {
  'XAUUSD':    'XAUUSD',
  'BTCUSD':    'BTCUSD',
  'ETHUSD':    'ETHUSD',
  'XAGUSD':    'XAGUSD',
  'NAS100':    'NAS100',
  'GER40':     'GER40',
  'AUS200':    'AUS200',
  'SPOTBRENT': 'SpotBrent',
};

let symbolIdMap      = {};
let symbolIdToTicker = {};

// ─── PENDING ORDER REGISTRY ───────────────────────────────────────────────────
// Maps symbolId:tradeSide → { dbId, registeredAt }
// Allows the execution event listener to match broker events back to
// the originating signal_log row without orderId storage.

var pendingOrders = {};

function registerPending(symbolId, tradeSide, dbId) {
  var key = String(symbolId) + ':' + tradeSide;
  pendingOrders[key] = { dbId: dbId, registeredAt: Date.now() };
}

function resolvePending(symbolId, tradeSide) {
  var key = String(symbolId) + ':' + tradeSide;
  var entry = pendingOrders[key];
  if (entry) {
    delete pendingOrders[key];
    return entry.dbId;
  }
  return null;
}

setInterval(function() {
  var now = Date.now();
  Object.keys(pendingOrders).forEach(function(key) {
    if (now - pendingOrders[key].registeredAt > 60000) {
      console.warn('[PENDING] Pruning stale pending entry:', key);
      delete pendingOrders[key];
    }
  });
}, 30000);

// ─── VOLUME ───────────────────────────────────────────────────────────────────

function getVolume(score, ticker) {
  const s = parseInt(score);
  switch (ticker) {
    case 'XAUUSD':    return s >= 9 ? 6   : s >= 8 ? 5   : 4;
    case 'BTCUSD':    return s >= 9 ? 3   : s >= 8 ? 2   : 1;
    case 'ETHUSD':    return s >= 9 ? 100 : s >= 8 ? 75  : 50;
    case 'XAGUSD':    return s >= 9 ? 30  : s >= 8 ? 20  : 10;
    case 'NAS100':    return s >= 9 ? 200 : s >= 8 ? 150 : 100;
    case 'GER40':     return s >= 9 ? 150 : s >= 8 ? 100 : 50;
    case 'AUS200':    return s >= 9 ? 200 : s >= 8 ? 150 : 100;
    case 'SPOTBRENT': return s >= 9 ? 200 : s >= 8 ? 150 : 100;
    default:
      console.warn('[VOLUME] No rule for', ticker, '— defaulting to 1');
      return 1;
  }
}

function resolveVolume(signal) {
  if (signal.lot_size !== undefined && signal.lot_size !== null && signal.lot_size !== '') {
    const lots = parseFloat(signal.lot_size);
    if (!isNaN(lots) && lots > 0) {
      const units = Math.round(lots * 100);
      console.log('[VOLUME] payload lot_size:', lots, 'lots =', units, 'units');
      return units;
    }
  }
  const units = getVolume(signal.score, signal.ticker);
  console.log('[VOLUME] fallback: score=' + signal.score + ' ticker=' + signal.ticker + ' = ' + units + ' units');
  return units;
}

// ─── SIGNAL HELPERS ───────────────────────────────────────────────────────────

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

// ─── SUPABASE LOGGING ─────────────────────────────────────────────────────────

async function logSignal(signal, result, status, errorMsg, latencyMs) {
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
      error_message: errorMsg || null,
      api_response:  result ? JSON.stringify(result) : null,
      signal_time:   new Date(parseInt(signal.timestamp)).toISOString(),
      processed_at:  new Date().toISOString(),
      is_paper:      IS_PAPER,
      latency_ms:    latencyMs || null,
    }).select('id').single();
    if (error) throw error;
    console.log('Logged:', status, latencyMs ? '(' + latencyMs + 'ms)' : '', '| dbId:', data && data.id);
    return data && data.id || null;
  } catch (e) {
    console.error('Supabase signal_log error:', e.message);
    return null;
  }
}

async function logExecutionEvent(execType, symbolId, tradeSide, executionPrice,
                                  executedVolume, orderId, positionId, errorCode,
                                  rawEvent, signalLogId) {
  try {
    await supabase.from('execution_events').insert({
      received_at:          new Date().toISOString(),
      ctid_trader_account:  ACCOUNT_ID,
      execution_type:       execType       || null,
      order_id:             orderId        || null,
      position_id:          positionId     || null,
      symbol_id:            symbolId       || null,
      trade_side:           tradeSide      || null,
      executed_volume:      executedVolume || null,
      execution_price:      executionPrice || null,
      error_code:           errorCode      || null,
      raw_event:            rawEvent       ? JSON.parse(JSON.stringify(rawEvent)) : null,
      signal_log_id:        signalLogId    || null,
      is_paper:             IS_PAPER,
    });
  } catch (e) {
    console.error('Supabase execution_events error:', e.message);
  }
}

async function logHealth(status, tokenDaysLeft) {
  try {
    await supabase.from('health_log').insert({
      status,
      token_days_left: tokenDaysLeft || null,
      checked_at:      new Date().toISOString(),
      is_paper:        IS_PAPER,
    });
  } catch (e) {
    console.error('Supabase health_log error:', e.message);
  }
}

async function logAlert(alertType, severity, message) {
  console.log('ALERT [' + severity + '] ' + alertType + ': ' + message);
  try {
    await supabase.from('alerts').insert({
      code:       alertType,
      severity,
      message,
      created_at: new Date().toISOString(),
    });
  } catch (e) {
    console.error('Supabase alerts error:', e.message);
  }
}

// ─── SYMBOL SCHEDULE QUERY ────────────────────────────────────────────────────
// Queries ProtoOASymbolByIdReq for all 8 tracked instruments on startup.
// Converts cTrader session format (seconds from Monday midnight UTC) into
// human-readable strings and persists to symbol_schedules table.

var DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

function secondsToHuman(secs) {
  var dayIndex = Math.floor(secs / 86400);
  var rem      = secs % 86400;
  var hh       = String(Math.floor(rem / 3600)).padStart(2, '0');
  var mm       = String(Math.floor((rem % 3600) / 60)).padStart(2, '0');
  return (DAYS[dayIndex % 7] || 'Day' + dayIndex) + ' ' + hh + ':' + mm + ' UTC';
}

async function querySymbolSchedules() {
  console.log('=== QUERYING SYMBOL SCHEDULES ===');
  var ids = Object.keys(symbolIdToTicker).map(Number).filter(Boolean);
  if (ids.length === 0) { console.warn('No symbolIds — skipping schedule query'); return; }

  try {
    var res     = await connection.sendCommand('ProtoOASymbolByIdReq', {
      ctidTraderAccountId: ACCOUNT_ID,
      symbolId:            ids,
    });
    var symbols = res.symbol || [];

    for (var s of symbols) {
      var ticker   = symbolIdToTicker[String(s.symbolId)] || 'UNKNOWN';
      var sessions = [];
      var rawSchedule = null;
      console.log('[SCHEDULE RAW]', ticker, JSON.stringify(s.schedule));
      
      try {
        rawSchedule = s.schedule || null;
        var intervals = (s.schedule && s.schedule.intervals) || [];
        intervals.forEach(function(iv) {
          sessions.push(secondsToHuman(iv.startSecond) + ' → ' + secondsToHuman(iv.endSecond));
        });
      } catch (e) {
        console.warn('Schedule parse error for', ticker, ':', e.message);
      }

      var humanStr = sessions.length > 0 ? sessions.join(' | ') : 'NO SCHEDULE DATA RETURNED';
      console.log('[SCHEDULE]', ticker, '(symbolId:' + s.symbolId + ')', humanStr);

      try {
        await supabase.from('symbol_schedules').insert({
          queried_at:     new Date().toISOString(),
          ticker:         ticker,
          symbol_id:      s.symbolId,
          symbol_name:    s.symbolName || null,
          sessions_raw:   rawSchedule  || null,
          sessions_human: humanStr,
          is_paper:       IS_PAPER,
        });
      } catch (e) {
        console.error('symbol_schedules insert error for', ticker, ':', e.message);
      }
    }
    console.log('=== SYMBOL SCHEDULES COMPLETE ===');
  } catch (err) {
    var msg = (err && err.message) ? err.message : JSON.stringify(err);
    console.error('Symbol schedule query failed:', msg);
    await logAlert('SCHEDULE_QUERY_FAILED', 'WARN', msg);
  }
}

// ─── EXECUTION EVENT LISTENER ─────────────────────────────────────────────────
// Primary order confirmation mechanism. Listens for ProtoOAExecutionEvent
// pushed by cTrader for every fill, rejection, cancellation, or expiry.
// Updates signal_log with definitive status and fill_price / rejection_reason.
// reconcileConfirm() is retained as a 2000ms fallback only.

function attachExecutionEventListener() {
  connection.on('ProtoOAExecutionEvent', async function(event) {
    try {
      var execType       = event.executionType                         || null;
      var order          = event.order                                 || {};
      var tradeData      = order.tradeData                             || {};
      var symbolId       = tradeData.symbolId                          || null;
      var tradeSide      = tradeData.tradeSide                         || null;
      var orderId        = order.orderId                               || null;
      var positionId     = order.positionId                            || null;
      var executionPrice = order.executionPrice !== undefined
        ? order.executionPrice / 100000 : null;
      var executedVolume = tradeData.volume                            || null;
      var errorCode      = event.errorCode                             || null;
      var ticker         = symbolId ? (symbolIdToTicker[String(symbolId)] || String(symbolId)) : 'UNKNOWN';

      console.log('[EXEC EVENT]', execType,
        '| ticker:', ticker, '| side:', tradeSide,
        '| price:', executionPrice, '| errorCode:', errorCode || 'none');

      var dbId = (tradeSide && symbolId) ? resolvePending(symbolId, tradeSide) : null;

      await logExecutionEvent(
        execType, symbolId, tradeSide, executionPrice,
        executedVolume, orderId, positionId, errorCode,
        event, dbId
      );

      if (!dbId) {
        console.log('[EXEC EVENT] No pending order match for', ticker, tradeSide,
          '— audit record written');
        return;
      }

      if (execType === 'ORDER_FILLED' || execType === 'ORDER_PARTIALLY_FILLED') {
        console.log('[EXEC EVENT] FILLED | dbId:' + dbId
          + ' | positionId:' + positionId + ' | fillPrice:' + executionPrice);
        await supabase.from('signal_log').update({
          status:       'EXECUTED',
          position_id:  positionId ? String(positionId) : null,
          fill_price:   executionPrice,
          api_response: JSON.stringify({
            positionId:     positionId,
            executionPrice: executionPrice,
            executedVolume: executedVolume,
            source:         'execution_event',
          }),
        }).eq('id', dbId);

      } else if (execType === 'ORDER_REJECTED'  ||
                 execType === 'ORDER_CANCELLED'  ||
                 execType === 'ORDER_EXPIRED') {
        console.error('[EXEC EVENT] ' + execType + ' | dbId:' + dbId
          + ' | ticker:' + ticker + ' | errorCode:' + errorCode);
        await supabase.from('signal_log').update({
          status:           'REJECTED',
          rejection_reason: errorCode || execType,
          error_message:    'Order ' + execType + ' by broker. Code: ' + (errorCode || 'none'),
        }).eq('id', dbId);
        await logAlert('ORDER_REJECTED', 'WARN',
          ticker + ' ' + tradeSide + ' ' + execType
          + '. Code: ' + (errorCode || 'none') + ' | dbId:' + dbId);

      } else {
        console.log('[EXEC EVENT] Type:', execType, '— audit record written, no signal_log update');
      }

    } catch (err) {
      console.error('[EXEC EVENT] Handler error:', (err && err.message) ? err.message : err);
    }
  });

console.log('ProtoOAExecutionEvent listener attached — supported events:', Object.keys(connection._events || {}).join(', '));
}

// ─── CONNECTION ───────────────────────────────────────────────────────────────

let connection   = null;
let isConnected  = false;
let reconnecting = false;

async function connectToCTrader() {
  if (reconnecting) return;
  reconnecting = true;
  isConnected  = false;

  try {
    console.log('Connecting to cTrader...');
    connection = new CTraderConnection({ host: HOST, port: 5035 });

    connection.on('close', function() {
      console.warn('cTrader connection closed — scheduling reconnect');
      isConnected  = false;
      reconnecting = false;
      logAlert('WEBSOCKET_CLOSED', 'WARN', 'cTrader connection closed. Reconnecting...');
      setTimeout(connectToCTrader, 3000);
    });
    
    console.log('Library prototype methods:', 
    Object.getOwnPropertyNames(Object.getPrototypeOf(connection))
      .filter(m => m !== 'constructor').join(', '));
    connection.on('error', function(err) {
      console.error('cTrader connection error:', err.message);
      isConnected = false;
    });

    await connection.open();
    console.log('Connected to cTrader');

    // Attach listener immediately after connection — before auth — so no events are missed
    attachExecutionEventListener();

    await connection.sendCommand('ProtoOAApplicationAuthReq', {
      clientId:     CLIENT_ID,
      clientSecret: CLIENT_SECRET,
    });
    console.log('Application authenticated');

    await connection.sendCommand('ProtoOAAccountAuthReq', {
      ctidTraderAccountId: ACCOUNT_ID,
      accessToken:         currentAccessToken,
    });
    console.log('Account authenticated:', ACCOUNT_ID);

    const symRes = await connection.sendCommand('ProtoOASymbolsListReq', {
      ctidTraderAccountId:    ACCOUNT_ID,
      includeArchivedSymbols: false,
    });
    symbolIdMap      = {};
    symbolIdToTicker = {};
    (symRes.symbol || []).forEach(function(s) {
      symbolIdMap[s.symbolName] = s.symbolId;
    });
    console.log('Symbols loaded:', Object.keys(symbolIdMap).length);

    Object.keys(SYMBOL_MAP).forEach(function(tv) {
      var ct = SYMBOL_MAP[tv];
      var id = symbolIdMap[ct];
      console.log(' ', tv, '->', ct, '-> symbolId:', id || 'NOT FOUND');
      if (id) symbolIdToTicker[String(id)] = tv;
    });

    setInterval(function() { connection.sendHeartbeat(); }, 25000);

    isConnected  = true;
    reconnecting = false;
    console.log('=== ENGINE READY | Mode:', IS_PAPER ? 'PAPER' : 'LIVE', '===');
    await logAlert('ENGINE_READY', 'INFO',
      'Engine v2.13 connected. Mode: ' + (IS_PAPER ? 'PAPER' : 'LIVE'));

    // Non-blocking — runs after ENGINE READY
    querySymbolSchedules().catch(function(e) {
      console.error('Symbol schedule query error:', e.message);
    });

  } catch (err) {
    var msg = (err && err.message) ? err.message : JSON.stringify(err);
    console.error('cTrader connection failed:', msg);
    reconnecting = false;
    await logAlert('CONNECTION_FAILED', 'CRITICAL', msg);
    setTimeout(connectToCTrader, 5000);
  }
}

// ─── QUEUE WASHDOWN ───────────────────────────────────────────────────────────

async function washdownQueue() {
  try {
    var flushed = 0;
    while (true) {
      var res  = await fetch(UPSTASH_URL + '/rpop/hawk:signals',
        { headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN } });
      var data = await res.json();
      if (!data.result) break;
      flushed++;
    }
    if (flushed > 0) {
      console.log('Pipeline washdown: flushed', flushed, 'stale signals');
      await logAlert('PIPELINE_RESET', 'WARN',
        'Startup washdown flushed ' + flushed + ' stale signals.');
    } else {
      console.log('Pipeline washdown: queue was empty');
    }
  } catch (e) {
    console.error('Washdown error:', e.message);
  }
}

// ─── RECONCILE FALLBACK ───────────────────────────────────────────────────────
// Fires 2000ms after order submission. If the execution event has already
// resolved the row, this exits immediately. Otherwise polls once as a safety net.

async function reconcileConfirm(dbId, symbolId, isLong) {
  try {
    var { data: row } = await supabase
      .from('signal_log')
      .select('status')
      .eq('id', dbId)
      .single();

    if (row && (row.status === 'EXECUTED' || row.status === 'REJECTED')) {
      console.log('[RECONCILE] dbId:' + dbId + ' already resolved by execution event ('
        + row.status + ') — skipping');
      return;
    }

    console.log('[RECONCILE FALLBACK] Polling for dbId:' + dbId);
    var posRes    = await connection.sendCommand('ProtoOAReconcileReq', {
      ctidTraderAccountId: ACCOUNT_ID,
    });
    var tradeSide = isLong ? 'BUY' : 'SELL';
    var position  = (posRes.position || []).find(function(p) {
      return String(p.tradeData && p.tradeData.symbolId) === String(symbolId) &&
             p.tradeData && p.tradeData.tradeSide === tradeSide;
    });

    if (!position) {
      console.warn('[RECONCILE FALLBACK] No position found for dbId:' + dbId
        + ' — execution event pending');
      await supabase.from('signal_log')
        .update({ status: 'PENDING_EXECUTION_EVENT' })
        .eq('id', dbId);
      return;
    }

    var positionId = position.positionId ? String(position.positionId) : null;
    console.log('[RECONCILE FALLBACK] Confirmed | positionId:' + positionId);
    await supabase.from('signal_log').update({
      status:       'EXECUTED',
      position_id:  positionId,
      api_response: JSON.stringify({
        positionId: positionId, tradeSide: tradeSide, source: 'reconcile_fallback',
      }),
    }).eq('id', dbId);

  } catch (err) {
    console.error('[RECONCILE FALLBACK] Error:', err.message);
    await logAlert('RECONCILE_ERROR', 'CRITICAL',
      'Reconcile fallback failed for dbId:' + dbId + ' — ' + err.message);
  }
}

async function reconcileExitConfirm(dbId, positionId, symbolId, isLong, attempt) {
  attempt = attempt || 1;
  try {
    var posRes    = await connection.sendCommand('ProtoOAReconcileReq', {
      ctidTraderAccountId: ACCOUNT_ID,
    });
    var tradeSide = isLong ? 'BUY' : 'SELL';
    var stillOpen = (posRes.position || []).find(function(p) {
      return String(p.tradeData && p.tradeData.symbolId) === String(symbolId) &&
             p.tradeData && p.tradeData.tradeSide === tradeSide;
    });
    if (stillOpen && attempt < 3) {
      setTimeout(function() {
        reconcileExitConfirm(dbId, positionId, symbolId, isLong, attempt + 1);
      }, 1000);
      return;
    }
    if (stillOpen) {
      await logAlert('EXIT_UNCONFIRMED', 'WARN',
        'Position still open after ' + attempt + ' checks. dbId:' + dbId);
      return;
    }
    console.log('Exit confirmed | positionId:' + positionId);
    await supabase.from('signal_log').update({
      status:       'CLOSED',
      position_id:  positionId,
      api_response: JSON.stringify({ positionId: positionId, tradeSide: tradeSide }),
    }).eq('id', dbId);
  } catch (err) {
    console.error('Exit reconcile error:', err.message);
    await logAlert('EXIT_RECONCILE_ERROR', 'CRITICAL',
      'Exit reconcile failed for dbId:' + dbId + ' — ' + err.message);
  }
}

// ─── SIGNAL EXECUTION ─────────────────────────────────────────────────────────

async function executeSignal(signal) {
  var latencyMs = getLatencyMs(signal);

  if (isExpired(signal)) {
    console.warn('Signal EXPIRED:', signal.signal_id, '| age:', latencyMs + 'ms');
    await logSignal(signal, null, 'EXPIRED', 'Signal age exceeded 5000ms', latencyMs);
    return;
  }

  if (isDuplicate(signal.signal_id)) {
    console.log('Duplicate signal ignored:', signal.signal_id);
    return;
  }

  if (!isConnected) {
    console.warn('Engine not connected — signal dropped');
    await logSignal(signal, null, 'ERROR', 'Engine not connected', latencyMs);
    return;
  }

  var action  = signal.action;
  var isEntry = action === 'LONG' || action === 'SHORT';
  var isExit  = action === 'LONG_EXIT'      || action === 'SHORT_EXIT'    ||
                action === 'LONG_STOP'      || action === 'SHORT_STOP'    ||
                action === 'LONG_MKT_CLOSE' || action === 'SHORT_MKT_CLOSE';
  var isLong  = action === 'LONG'           || action === 'LONG_EXIT'     ||
                action === 'LONG_STOP'      || action === 'LONG_MKT_CLOSE';

  var ctSymbol  = SYMBOL_MAP[signal.ticker] || signal.ticker;
  var symbolId  = symbolIdMap[ctSymbol];
  var tradeSide = isLong ? 'BUY' : 'SELL';

  if (!symbolId) {
    console.error('Symbol not found:', ctSymbol);
    await logSignal(signal, null, 'ERROR', 'Symbol not found: ' + ctSymbol, latencyMs);
    return;
  }

  try {
    if (isEntry) {
      var posRes   = await connection.sendCommand('ProtoOAReconcileReq', {
        ctidTraderAccountId: ACCOUNT_ID,
      });
      var existing = (posRes.position || []).find(function(p) {
        return String(p.tradeData && p.tradeData.symbolId) === String(symbolId) &&
               p.tradeData && p.tradeData.tradeSide === tradeSide;
      });
      if (existing) {
        console.log('Position already open — skipping entry:', ctSymbol);
        await logSignal(signal, null, 'DUPLICATE_POSITION', null, latencyMs);
        return;
      }

      var volume = resolveVolume(signal);
      if (volume === 0) {
        console.warn('[ZERO VOLUME]', signal.ticker, 'score=' + signal.score, '— order skipped');
        await logSignal(signal, null, 'SKIPPED',
          'Zero volume — review lot size configuration', latencyMs);
        return;
      }

      var stopLoss = Math.round(parseFloat(signal.atr) * 2 * 100000);
      console.log('Order |', ctSymbol, '|', tradeSide, '|', volume,
        'units | SL:', stopLoss, '| latency:', latencyMs + 'ms');

      var dbId = await logSignal(
        signal,
        { symbolId: symbolId, volume: volume, stopLoss: stopLoss },
        'PENDING_FILL', null, latencyMs
      );

      if (dbId) registerPending(symbolId, tradeSide, dbId);

      try {
        await connection.sendCommand('ProtoOANewOrderReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          symbolId:            symbolId,
          orderType:           'MARKET',
          tradeSide:           tradeSide,
          volume:              volume,
          relativeStopLoss:    stopLoss,
          comment:             'HAWK|' + signal.strategy_id + '|S' + signal.score,
        });
        console.log('Order sent to cTrader');
      } catch (e) {
        console.error('Order send error:', e.message);
        if (dbId) resolvePending(symbolId, tradeSide);
        await supabase.from('signal_log')
          .update({ status: 'ERROR', error_message: e.message })
          .eq('id', dbId);
        return;
      }

      // Reconcile fallback at 2000ms — exits immediately if execution event arrived first
      setTimeout(function() { reconcileConfirm(dbId, symbolId, isLong); }, 2000);

    } else if (isExit) {
      var posRes2  = await connection.sendCommand('ProtoOAReconcileReq', {
        ctidTraderAccountId: ACCOUNT_ID,
      });
      var position = (posRes2.position || []).find(function(p) {
        return String(p.tradeData && p.tradeData.symbolId) === String(symbolId) &&
               p.tradeData && p.tradeData.tradeSide === (isLong ? 'BUY' : 'SELL');
      });
      if (!position) {
        console.log('No matching position for', action, ctSymbol);
        await logSignal(signal, null, 'NO_POSITION', null, latencyMs);
        return;
      }

      var positionId = position.positionId ? String(position.positionId) : null;
      console.log('Closing position:', positionId, '| latency:', latencyMs + 'ms');

      var dbId2 = await logSignal(
        signal, { positionId: positionId }, 'PENDING_CLOSE', null, latencyMs
      );

      try {
        await connection.sendCommand('ProtoOAClosePositionReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          positionId:          position.positionId,
          volume:              position.tradeData.volume,
        });
        console.log('Close sent to cTrader');
      } catch (e) {
        console.error('Close send error:', e.message);
        await supabase.from('signal_log')
          .update({ status: 'ERROR', error_message: e.message })
          .eq('id', dbId2);
        return;
      }

      setTimeout(function() {
        reconcileExitConfirm(dbId2, positionId, symbolId, isLong);
      }, 1500);

    } else {
      console.warn('[UNKNOWN ACTION]', action, '— skipped');
    }

  } catch (err) {
    console.error('Execution error:', err.message);
    await logSignal(signal, null, 'ERROR', err.message, latencyMs);
  }
}

// ─── HTTP SERVER ──────────────────────────────────────────────────────────────

function startHttpServer() {
  var app = express();
  app.use(express.json());

  app.post('/signal', async function(req, res) {
    if (req.headers['x-internal-secret'] !== INTERNAL_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    var signal = req.body;
    if (!signal || !signal.signal_id) {
      return res.status(400).json({ error: 'Invalid signal' });
    }
    res.status(200).json({ ok: true });
    setImmediate(function() { executeSignal(signal); });
  });

  app.get('/health', function(req, res) {
    res.json({
      status:        isConnected ? 'CONNECTED' : 'DISCONNECTED',
      mode:          IS_PAPER ? 'PAPER' : 'LIVE',
      uptime:        process.uptime(),
      version:       '2.13',
      pendingOrders: Object.keys(pendingOrders).length,
    });
  });

  app.listen(PORT, function() {
    console.log('HTTP server listening on port', PORT);
  });
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main() {
  await refreshAccessToken();

  setInterval(async function() {
    try { await refreshAccessToken(); }
    catch (e) { logAlert('TOKEN_REFRESH_FAILED', 'CRITICAL', e.message); }
  }, 20 * 24 * 60 * 60 * 1000);

  await washdownQueue();
  await connectToCTrader();
  startHttpServer();

  setInterval(async function() {
    var daysLeft = tokenExpiryTime
      ? Math.floor((tokenExpiryTime - Date.now()) / 86400000)
      : null;
    await logHealth(isConnected ? 'RUNNING' : 'DISCONNECTED', daysLeft);
    if (daysLeft !== null && daysLeft < 2) {
      await logAlert('TOKEN_EXPIRY_CRITICAL', 'CRITICAL',
        'cTrader access token expires in ' + daysLeft + ' days. Immediate action required.');
    }
  }, 60000);
}

main().catch(function(err) {
  console.error('Fatal startup error:', err.message);
  process.exit(1);
});
