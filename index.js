'use strict';

const { CTraderConnection } = require('@reiryoku/ctrader-layer');
const { createClient }      = require('@supabase/supabase-js');
const express               = require('express');

console.log('=== HAWK ENGINE v2.23 STARTING ===');

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

// ─── STOP DISTANCE POINT MULTIPLIERS ─────────────────────────────────────────
// Converts stop_distance (price units from Pine Script payload) into
// cTrader relativeStopLoss integer (points).
// Formula: relativeStopLoss = stop_distance × multiplier
//
// XAUUSD  — 2 decimal places → ×100
// XAGUSD  — 3 decimal places → ×1,000
// BTCUSD  — 2 decimal places → ×100
// ETHUSD  — 2 decimal places → ×100
// NAS100  — 1 decimal place  → ×10
// GER40   — 1 decimal place  → ×10
// AUS200  — 1 decimal place  → ×10
// SPOTBRENT — 3 decimal places → ×1,000

const STOP_POINT_MULTIPLIER = {
  'XAUUSD':    100,
  'XAGUSD':    1000,
  'BTCUSD':    100,
  'ETHUSD':    100,
  'NAS100':    10,
  'GER40':     10,
  'AUS200':    10,
  'SPOTBRENT': 1000,
};

// ─── PENDING ORDER REGISTRY ───────────────────────────────────────────────────

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
      // Per-instrument lot size map — units = lots × lotSize
      // Derived empirically from Pepperstone cTrader paper account:
      // XAUUSD/SPOTBRENT: 1 lot = 10,000 units (confirmed: 100 units = 0.01 lots)
      // ETHUSD/NAS100/GER40/AUS200: 1 lot = 100 units (confirmed: 100 units = 1 lot)
      // BTCUSD/XAGUSD: 1 lot = 100 units (inferred — 100 units caused margin rejection)
      // LOT_SIZE = units per lot, derived empirically from Pepperstone paper account.
      // Rule of thumb: 3 decimal place instruments (XAUUSD, XAGUSD, SPOTBRENT) = 10,000
      //                2 decimal place / index instruments = 100
      const LOT_SIZE = {
        'XAUUSD':    10000,
        'XAGUSD':    10000,
        'SPOTBRENT': 10000,
        'ETHUSD':    100,
        'NAS100':    100,
        'GER40':     100,
        'AUS200':    100,
        'BTCUSD':    100,
      };
      var lotSize = LOT_SIZE[signal.ticker] || 10000;
      const units = Math.round(lots * lotSize);
      console.log('[VOLUME] payload lot_size:', lots, 'lots | lotSize:', lotSize,
        '| units:', units, '| ticker:', signal.ticker);
      return units;
    }
  }
  const units = getVolume(signal.score, signal.ticker);
  console.log('[VOLUME] fallback: score=' + signal.score + ' ticker=' + signal.ticker + ' = ' + units + ' units');
  return units;
}

// ─── STOP LOSS CALCULATION ────────────────────────────────────────────────────
// Primary: use stop_distance from v5.0.8 payload (Kijun-based structural stop).
// Fallback: atr × 2 (legacy behaviour — fires only if stop_distance absent).

function resolveStopLoss(signal) {
  var ticker     = signal.ticker;
  var multiplier = STOP_POINT_MULTIPLIER[ticker] || 100;

  if (signal.stop_distance !== undefined &&
      signal.stop_distance !== null &&
      signal.stop_distance !== '') {
    var dist = parseFloat(signal.stop_distance);
    if (!isNaN(dist) && dist > 0) {
      var stopPoints = Math.round(dist * multiplier);
      console.log('[STOP] payload stop_distance:', dist,
        '| multiplier:', multiplier,
        '| cTrader points:', stopPoints,
        '| ticker:', ticker);
      return stopPoints;
    }
  }

  var atr        = parseFloat(signal.atr) || 0;
  var stopPoints = Math.round(atr * 2 * multiplier);
  console.log('[STOP] FALLBACK atr×2:', atr * 2,
    '| multiplier:', multiplier,
    '| cTrader points:', stopPoints,
    '| ticker:', ticker);
  return stopPoints;
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
        var intervals = Array.isArray(s.schedule) ? s.schedule : [];
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

// ─── DEAL LIST DIAGNOSTIC ─────────────────────────────────────────────────────
// Queries cTrader for deals in the last 30 seconds.
// Called after every order submission to confirm whether cTrader recorded
// any execution. If deals appear, the order executed and the execution event
// listener is not working. If deals are empty, cTrader rejected the order.

async function queryRecentDeals(ctSymbol, dbId) {
  try {
    var toTs   = Date.now();
    var fromTs = toTs - 30000;
    var res    = await connection.sendCommand('ProtoOADealListReq', {
      ctidTraderAccountId: ACCOUNT_ID,
      fromTimestamp:       fromTs,
      toTimestamp:         toTs,
      maxRows:             10,
    });
    var deals = res.deal || [];
    if (deals.length === 0) {
      console.warn('[DEAL LIST] No deals found in last 30s for', ctSymbol,
        '— order was likely rejected by cTrader | dbId:', dbId);
      await supabase.from('signal_log')
        .update({
          status:        'REJECTED',
          error_message: 'No deal recorded by cTrader within 30s — order rejected silently',
        })
        .eq('id', dbId);
      await logAlert('ORDER_SILENT_REJECT', 'WARN',
        ctSymbol + ' order sent but no deal recorded by cTrader. dbId:' + dbId
        + ' — check stop loss distance and minimum stop requirements.');
    } else {
      console.log('[DEAL LIST] Deals found after order:', JSON.stringify(deals));
      var deal = deals[0];
      var fillPrice = deal.executionPrice ? deal.executionPrice / 100000 : null;
      console.log('[DEAL LIST] CONFIRMED EXECUTION | dealId:', deal.dealId,
        '| fillPrice:', fillPrice,
        '| status:', deal.dealStatus,
        '| dbId:', dbId);
      await supabase.from('signal_log')
        .update({
          status:       'EXECUTED',
          fill_price:   fillPrice,
          api_response: JSON.stringify({
            dealId:         deal.dealId,
            executionPrice: fillPrice,
            dealStatus:     deal.dealStatus,
            source:         'deal_list_query',
          }),
        })
        .eq('id', dbId);
    }
  } catch (err) {
    console.error('[DEAL LIST] Query error:', err.message);
  }
}

// ─── EXECUTION EVENT LISTENER ─────────────────────────────────────────────────

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

  console.log('ProtoOAExecutionEvent listener attached');
}

// ─── CONNECTION ───────────────────────────────────────────────────────────────

let connection   = null;
let isConnected  = false;
let reconnecting = false;
let symbolIdMap      = {};
let symbolIdToTicker = {};

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

    connection.on('error', function(err) {
      console.error('cTrader connection error:', err.message);
      isConnected = false;
    });

    await connection.open();
    console.log('Connected to cTrader');

    attachExecutionEventListener();

    await connection.sendCommand('ProtoOAApplicationAuthReq', {
      clientId:     CLIENT_ID,
      clientSecret: CLIENT_SECRET,
    });
    console.log('Application authenticated');

    // ── ACCOUNT BALANCE DIAGNOSTIC ───────────────────────────────────────────
    // Queries the paper account details including balance.
    // A zero balance would silently reject all orders.
    try {
      var traderRes = await connection.sendCommand('ProtoOATraderReq', {
        ctidTraderAccountId: ACCOUNT_ID,
      });
      var trader = traderRes.trader || {};
      var balance     = trader.balance     !== undefined ? (trader.balance / 100)     : 'unknown';
      var equity      = trader.equity      !== undefined ? (trader.equity / 100)      : 'unknown';
      var freeMargin  = trader.freeMargin  !== undefined ? (trader.freeMargin / 100)  : 'unknown';
      var currency    = trader.depositAsset ? (trader.depositAsset.name || 'unknown') : 'unknown';
      var leverageVal = trader.leverageInCents !== undefined ? (trader.leverageInCents / 100) : 'unknown';
      console.log('[ACCOUNT BALANCE]',
        'balance:', balance,
        '| equity:', equity,
        '| freeMargin:', freeMargin,
        '| currency:', currency,
        '| leverage:', leverageVal,
        '| brokerName:', trader.brokerName || 'unknown',
        '| accountType:', trader.accountType || 'unknown',
        '| isLive:', trader.isLive);
      if (balance !== 'unknown' && balance <= 0) {
        console.error('[ACCOUNT BALANCE] WARNING: Balance is zero or negative — orders will be rejected!');
        await logAlert('ZERO_BALANCE', 'CRITICAL',
          'Paper account balance is ' + balance + '. Orders will be rejected. Please reset the paper account.');
      }
    } catch (balErr) {
      console.warn('[ACCOUNT BALANCE] Query failed:', balErr.message);
    }
    // ────────────────────────────────────────────────────────────────────────

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
      'Engine v2.23 connected. Mode: ' + (IS_PAPER ? 'PAPER' : 'LIVE'));

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

async function reconcileConfirm(dbId, symbolId, isLong) {
  try {
    var { data: row } = await supabase
      .from('signal_log')
      .select('status')
      .eq('id', dbId)
      .single();

    if (row && (row.status === 'EXECUTED' || row.status === 'REJECTED')) {
      console.log('[RECONCILE] dbId:' + dbId + ' already resolved ('
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

      var volume   = resolveVolume(signal);
      var stopLoss = resolveStopLoss(signal);

      if (volume === 0) {
        console.warn('[ZERO VOLUME]', signal.ticker, 'score=' + signal.score, '— order skipped');
        await logSignal(signal, null, 'SKIPPED',
          'Zero volume — review lot size configuration', latencyMs);
        return;
      }

      console.log('Order |', ctSymbol, '|', tradeSide, '|', volume,
        'units | SL:', stopLoss, 'pts | latency:', latencyMs + 'ms');

      var dbId = await logSignal(
        signal,
        { symbolId: symbolId, volume: volume, stopLoss: stopLoss },
        'PENDING_FILL', null, latencyMs
      );

      if (dbId) registerPending(symbolId, tradeSide, dbId);

      try {
        // v2.20 DIAGNOSTIC — bare minimum order: no stop loss, no comment
        // Cleanest possible test to confirm if order placement works at all.
        // Stop loss and comment will be restored once execution is confirmed.
        var orderRes = await connection.sendCommand('ProtoOANewOrderReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          symbolId:            symbolId,
          orderType:           'MARKET',
          tradeSide:           tradeSide,
          volume:              volume,
        });

        // Log full response — executionType tells us if this was accepted or rejected
        var orderResKeys = orderRes ? Object.keys(orderRes) : [];
        console.log('[ORDER RESPONSE] keys:', orderResKeys.join(',') || 'EMPTY',
          '| executionType:', (orderRes && orderRes.executionType) || 'none',
          '| errorCode:', (orderRes && orderRes.errorCode) || 'none',
          '| full:', JSON.stringify(orderRes));

      } catch (e) {
        console.error('[ORDER ERROR] cTrader rejected order:', e.message,
          '| ticker:', ctSymbol,
          '| side:', tradeSide,
          '| volume:', volume,
          '| stopLoss:', stopLoss,
          '| errorCode:', (e.errorCode || 'none'),
          '| errorDescription:', (e.description || 'none'));
        if (dbId) resolvePending(symbolId, tradeSide);
        await supabase.from('signal_log')
          .update({
            status:        'ERROR',
            error_message: e.message + (e.errorCode ? ' | code: ' + e.errorCode : ''),
          })
          .eq('id', dbId);
        await logAlert('ORDER_REJECTED', 'WARN',
          ctSymbol + ' ' + tradeSide + ' rejected by cTrader.'
          + ' Error: ' + e.message
          + (e.errorCode ? ' | Code: ' + e.errorCode : '')
          + ' | volume: ' + volume
          + ' | stopLoss: ' + stopLoss + 'pts'
          + ' | dbId: ' + dbId);
        return;
      }

      // Reconcile fallback at 2000ms
      setTimeout(function() { reconcileConfirm(dbId, symbolId, isLong); }, 2000);

      // Deal list diagnostic at 4000ms — confirms whether cTrader recorded an execution
      setTimeout(function() { queryRecentDeals(ctSymbol, dbId); }, 4000);

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
      version:       '2.23',
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
