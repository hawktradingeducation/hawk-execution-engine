'use strict';

const { CTraderConnection } = require('@reiryoku/ctrader-layer');
const { createClient }      = require('@supabase/supabase-js');
const express               = require('express');

console.log('=== HAWK ENGINE v2.32 STARTING ===');

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
  await logAlert('TOKEN_REFRESH_SUCCEEDED', 'INFO',
    'Access token refreshed successfully. Expires in ' + daysLeft + ' days.');
  await logHealth('RUNNING', daysLeft);
  if (daysLeft < 7) {
    await logAlert('TOKEN_EXPIRY_WARNING', 'WARN',
      'cTrader access token expires in ' + daysLeft + ' days.');
  }
  return currentAccessToken;
}

// ─── SYMBOL MAP ───────────────────────────────────────────────────────────────
// v2.31: GBPJPY added.
// NOTE: cTrader symbol string 'GBPJPY' assumed — verify in Pepperstone
// cTrader symbol list on first deployment. If not found, check for 'GBP/JPY'.

const SYMBOL_MAP = {
  'XAUUSD':    'XAUUSD',
  'BTCUSD':    'BTCUSD',
  'ETHUSD':    'ETHUSD',
  'XAGUSD':    'XAGUSD',
  'NAS100':    'NAS100',
  'GER40':     'GER40',
  'AUS200':    'AUS200',
  'SPOTBRENT': 'SpotBrent',
  'GBPJPY':    'GBPJPY',       // v2.31 — verify exact cTrader symbol string
};

// --- STOP DISTANCE POINT MULTIPLIERS -----------------------------------------
// Converts stop_distance (price units from Pine Script payload) into
// cTrader relativeStopLoss integer.
// Formula: relativeStopLoss = stop_distance x multiplier
//
// Empirically confirmed: 1 cTrader relativeStopLoss unit = 1e-05 price units
// for all instruments regardless of decimal places.
// Therefore: multiplier = 1 / 1e-05 = 100,000 universally.

const STOP_POINT_MULTIPLIER = {
  'XAUUSD':    100000,
  'XAGUSD':    100000,
  'BTCUSD':    100000,
  'ETHUSD':    100000,
  'NAS100':    100000,
  'GER40':     100000,
  'AUS200':    100000,
  'SPOTBRENT': 100000,
  'GBPJPY':    100000,         // v2.31
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
// v2.31: GBPJPY added.
// Fallback only — fires if lot_size absent from payload (should never occur
// on v6.0.0+ Pine Scripts). Values in cTrader units (lots × LOT_SIZE).
// GBPJPY: 1 standard FX lot = 100,000 units. 0.01/0.02/0.04 lots =
// 1,000/2,000/4,000 units. *** EMPIRICALLY UNVERIFIED — confirm on first
// paper trade that cTrader shows 0.01 lots, not some other size. ***

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
    case 'GBPJPY':    return s >= 9 ? 400000 : s >= 8 ? 200000 : 100000;  // v2.32 — 0.04/0.02/0.01 lots
    default:
      console.warn('[VOLUME] No rule for', ticker, '— defaulting to 1');
      return 1;
  }
}

// Minimum volumes enforced by cTrader (units, not lots).
// NAS100/GER40/AUS200: minVolume=10, stepVolume=10 (= 0.1 lots minimum).
// GBPJPY: standard FX minVolume = 1,000 (= 0.01 lots). No clamping needed.
const MIN_VOLUME = {
  'NAS100': 10,
  'GER40':  10,
  'AUS200': 10,
};

function resolveVolume(signal) {
  if (signal.lot_size !== undefined && signal.lot_size !== null && signal.lot_size !== '') {
    const lots = parseFloat(signal.lot_size);
    if (!isNaN(lots) && lots > 0) {
      // LOT_SIZE empirical values — units per lot in cTrader Open API.
      // GBPJPY: empirically confirmed 23 Apr 2026.
      //   lot_size=1.00 × 100,000 = 100,000 units → 0.01 lots in cTrader.
      //   Therefore 0.01 lots = 100,000 units → 1 lot = 10,000,000 units.
      //   LOT_SIZE = 10,000,000 ensures Pine payload 0.01/0.02/0.04 →
      //   0.01/0.02/0.04 lots displayed in cTrader.
      const LOT_SIZE = {
        'XAUUSD':    10000,
        'XAGUSD':    500000,
        'SPOTBRENT': 10000,
        'ETHUSD':    100,
        'NAS100':    100,
        'GER40':     100,
        'AUS200':    100,
        'BTCUSD':    100,
        'GBPJPY':    10000000,  // v2.32 — empirically confirmed
      };
      let lotSize = LOT_SIZE[signal.ticker] || 10000;
      let units   = Math.round(lots * lotSize);
      let minVol  = MIN_VOLUME[signal.ticker] || 0;
      if (minVol > 0 && units < minVol) {
        console.warn('[VOLUME] Calculated units', units, '< minVolume', minVol,
          'for', signal.ticker, '— clamping to minVolume. lot_size sent:', lots);
        units = minVol;
      }
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
      alert_type: alertType,
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
      console.log('[SYMBOL SPEC]', ticker,
        '| minStopLoss:', s.minStopLossDistance,
        '| minVolume:', s.minVolume,
        '| maxVolume:', s.maxVolume,
        '| lotSize:', s.lotSize,
        '| digits:', s.digits,
        '| pipPosition:', s.pipPosition,
        '| stepVolume:', s.stepVolume
      );

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

async function queryRecentDeals(ctSymbol, dbId, symbolId) {
  try {
    var toTs   = Date.now();
    var fromTs = toTs - 30000;
    var res    = await connection.sendCommand('ProtoOADealListReq', {
      ctidTraderAccountId: ACCOUNT_ID,
      fromTimestamp:       fromTs,
      toTimestamp:         toTs,
      maxRows:             10,
    });
    var allDeals = res.deal || [];
    var deals = symbolId
      ? allDeals.filter(function(d) { return String(d.symbolId) === String(symbolId); })
      : allDeals;
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
      var fillPrice = deal.executionPrice ? deal.executionPrice : null;
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
      var raw = typeof event.toObject === 'function' ? event.toObject() :
                typeof event.toJSON   === 'function' ? event.toJSON()   :
                Object.assign({}, event);
console.log('[EXEC TYPE]', event.type);
      console.log('[EXEC DESC]', JSON.stringify(
        typeof event.descriptor.toObject === 'function' ? event.descriptor.toObject() :
        typeof event.descriptor.toJSON   === 'function' ? event.descriptor.toJSON()   :
        event.descriptor
      ));
    } catch(e) { console.log('[EXEC RAW ERROR]', e.message); }
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

// ─── STARTUP: CLOSE ALL OPEN POSITIONS ───────────────────────────────────────

async function closeAllOpenPositions() {
  console.log('[STARTUP] Checking for open positions to close...');
  try {
    var posRes    = await connection.sendCommand('ProtoOAReconcileReq', {
      ctidTraderAccountId: ACCOUNT_ID,
    });
    var positions = posRes.position || [];

    if (positions.length === 0) {
      console.log('[STARTUP] No open positions found.');
      await logAlert('STARTUP_POSITIONS_CLOSED', 'INFO',
        'Startup check: no open positions found.');
      return;
    }

    console.log('[STARTUP] Found ' + positions.length + ' open position(s). Closing all...');
    var closed = [];
    var failed = [];

    for (var p of positions) {
      var symbolId   = p.tradeData && p.tradeData.symbolId ? String(p.tradeData.symbolId) : 'UNKNOWN';
      var ticker     = symbolIdToTicker[symbolId] || symbolId;
      var positionId = p.positionId ? String(p.positionId) : null;
      var volume     = p.tradeData && p.tradeData.volume ? p.tradeData.volume : null;
      var tradeSide  = p.tradeData && p.tradeData.tradeSide ? p.tradeData.tradeSide : null;

      console.log('[STARTUP] Closing position | ticker:', ticker,
        '| positionId:', positionId,
        '| side:', tradeSide,
        '| volume:', volume);

      try {
        await connection.sendCommand('ProtoOAClosePositionReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          positionId:          p.positionId,
          volume:              volume,
        });

        await supabase.from('signal_log').insert({
          signal_id:     'STARTUP_CLOSE_' + positionId,
          strategy_id:   'SYSTEM',
          ticker:        ticker,
          action:        'STARTUP_CLOSE',
          score:         null,
          atr:           null,
          close_price:   null,
          status:        'CLOSED',
          error_message: null,
          api_response:  JSON.stringify({ positionId: positionId, tradeSide: tradeSide, source: 'startup_close' }),
          signal_time:   new Date().toISOString(),
          processed_at:  new Date().toISOString(),
          is_paper:      IS_PAPER,
          latency_ms:    null,
        });

        closed.push(ticker);
        console.log('[STARTUP] Closed:', ticker, '| positionId:', positionId);

      } catch (e) {
        var errMsg = (e && e.message) ? e.message : String(e);
        console.error('[STARTUP] Failed to close position | ticker:', ticker,
          '| positionId:', positionId, '| error:', errMsg);

        await supabase.from('signal_log').insert({
          signal_id:     'STARTUP_CLOSE_FAIL_' + positionId,
          strategy_id:   'SYSTEM',
          ticker:        ticker,
          action:        'STARTUP_CLOSE',
          score:         null,
          atr:           null,
          close_price:   null,
          status:        'ERROR',
          error_message: 'Startup close failed: ' + errMsg,
          api_response:  null,
          signal_time:   new Date().toISOString(),
          processed_at:  new Date().toISOString(),
          is_paper:      IS_PAPER,
          latency_ms:    null,
        });

        failed.push(ticker);
      }
    }

    var summary  = 'Startup close: ' + closed.length + ' closed'
      + (closed.length > 0 ? ' (' + closed.join(', ') + ')' : '')
      + (failed.length > 0 ? ' | ' + failed.length + ' FAILED (' + failed.join(', ') + ')' : '');
    var severity = failed.length > 0 ? 'CRITICAL' : 'WARN';
    await logAlert('STARTUP_POSITIONS_CLOSED', severity, summary);
    console.log('[STARTUP]', summary);

  } catch (err) {
    var msg = (err && err.message) ? err.message : String(err);
    console.error('[STARTUP] closeAllOpenPositions error:', msg);
    await logAlert('STARTUP_CLOSE_ERROR', 'CRITICAL',
      'Startup position close query failed: ' + msg);
  }
}

// ─── WATCHDOG ────────────────────────────────────────────────────────────────

async function runWatchdog() {
  if (!isConnected || reconnecting) return;

  try {
    var timeoutMs = 8000;
    var watchdogPromise = connection.sendCommand('ProtoOAReconcileReq', {
      ctidTraderAccountId: ACCOUNT_ID,
    });
    var timeoutPromise = new Promise(function(_, reject) {
      setTimeout(function() { reject(new Error('Watchdog timeout after 8s')); }, timeoutMs);
    });

    await Promise.race([watchdogPromise, timeoutPromise]);

    lastWatchdogOk   = new Date().toISOString();
    watchdogFailures = 0;
    console.log('[WATCHDOG] OK | connection verified |', lastWatchdogOk);

  } catch (err) {
    watchdogFailures++;
    var msg = (err && err.message) ? err.message : String(err);
    console.error('[WATCHDOG] FAIL #' + watchdogFailures + ' |', msg);
    await logAlert('WATCHDOG_FAIL', 'WARN',
      'Watchdog failure #' + watchdogFailures + ': ' + msg);

    if (watchdogFailures >= 2) {
      console.error('[WATCHDOG] 2 consecutive failures — forcing reconnect');
      await logAlert('WATCHDOG_RECONNECT', 'CRITICAL',
        'Watchdog forced reconnect after ' + watchdogFailures + ' failures. isConnected was: ' + isConnected);
      isConnected      = false;
      reconnecting     = false;
      watchdogFailures = 0;
      connectToCTrader();
    }
  }
}

// ─── CONNECTION ───────────────────────────────────────────────────────────────

let connection   = null;
let isConnected  = false;
let reconnecting = false;
let symbolIdMap      = {};
let symbolIdToTicker = {};

let lastWatchdogOk   = null;
let watchdogFailures = 0;

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
      'Engine v2.32 connected. Mode: ' + (IS_PAPER ? 'PAPER' : 'LIVE'));

    await closeAllOpenPositions();

    setInterval(runWatchdog, 10 * 60 * 1000);

    querySymbolSchedules().catch(function(e) {
      console.error('Symbol schedule query error:', e.message);
    });

    var startupElapsedMs = Date.now() - (global.engineStartMs || Date.now());
    await logAlert('STARTUP_COMPLETE', 'INFO',
      'Engine v2.32 startup complete in ' + startupElapsedMs + 'ms. Mode: '
      + (IS_PAPER ? 'PAPER' : 'LIVE'));

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

  if (latencyMs !== null && latencyMs > 3000) {
    await logAlert('LATENCY_CRITICAL', 'CRITICAL',
      'Signal latency ' + latencyMs + 'ms exceeds 3000ms threshold.'
      + ' ticker: ' + (signal.ticker || 'UNKNOWN')
      + ' | signal_id: ' + (signal.signal_id || 'UNKNOWN'));
  } else if (latencyMs !== null && latencyMs > 1500) {
    await logAlert('LATENCY_WARN', 'WARN',
      'Signal latency ' + latencyMs + 'ms exceeds 1500ms threshold.'
      + ' ticker: ' + (signal.ticker || 'UNKNOWN')
      + ' | signal_id: ' + (signal.signal_id || 'UNKNOWN'));
  } else if (latencyMs !== null && latencyMs > 500) {
    await logAlert('LATENCY_ADVISORY', 'INFO',
      'Signal latency ' + latencyMs + 'ms exceeds 500ms advisory threshold.'
      + ' ticker: ' + (signal.ticker || 'UNKNOWN')
      + ' | signal_id: ' + (signal.signal_id || 'UNKNOWN'));
  }

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
        var orderRes = await connection.sendCommand('ProtoOANewOrderReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          symbolId:            symbolId,
          orderType:           'MARKET',
          tradeSide:           tradeSide,
          volume:              volume,
          relativeStopLoss:    stopLoss,
          comment:             'HAWK|' + signal.strategy_id + '|S' + signal.score,
        });
        console.log('[ORDER] Sent to cTrader | ticker:', ctSymbol,
          '| side:', tradeSide, '| volume:', volume, '| stopLoss:', stopLoss, 'pts');

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

      setTimeout(function() { reconcileConfirm(dbId, symbolId, isLong); }, 2000);
      setTimeout(function() { queryRecentDeals(ctSymbol, dbId, symbolId); }, 4000);

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
    var uptimeSecs  = process.uptime();
    var watchdogAge = lastWatchdogOk
      ? Math.round((Date.now() - new Date(lastWatchdogOk).getTime()) / 1000)
      : null;
    res.json({
      status:           isConnected ? 'CONNECTED' : 'DISCONNECTED',
      mode:             IS_PAPER ? 'PAPER' : 'LIVE',
      uptime:           uptimeSecs,
      version:          '2.32',
      pendingOrders:    Object.keys(pendingOrders).length,
      lastWatchdogOk:   lastWatchdogOk,
      watchdogAgeS:     watchdogAge,
      watchdogFailures: watchdogFailures,
    });
  });

  app.listen(PORT, function() {
    console.log('HTTP server listening on port', PORT);
  });
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main() {
  global.engineStartMs = Date.now();

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
