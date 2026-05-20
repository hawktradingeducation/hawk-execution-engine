'use strict';

// ─── CHANGE LOG ───────────────────────────────────────────────────────────────
// v2.42.0 — v8.3 PAYLOAD ALIGNMENT + R_REV ACTION HANDLING
//
// 1. LONG_R_REV / SHORT_R_REV action handling added.
//    R reversal of open C position. Closes existing position then opens new
//    position in opposite direction. Both legs logged to signal_log separately.
//    Previously fell through to [UNKNOWN ACTION] warn and was silently dropped.
//
// 2. logSignal() — 7 new v8.3 enrichment fields added:
//    utc_hour, dow_utc, ar_char, kijun_dist_atr, stop_dist_atr,
//    backstop_level, utc_hour_exit.
//    All fields are nullable — engine operates normally if Pine omits them.
//
// 3. Version strings updated to v2.42.0 throughout.
//
// Supabase schema changes required BEFORE deploying this engine:
//    Run hawk_btc_v83_epoch.sql (new epoch clean start + 7 new columns).
//    Do NOT deploy against old schema — logSignal() insert will fail on
//    columns that do not yet exist.
// ─────────────────────────────────────────────────────────────────────────────

// ─── PREVIOUS: v2.41.0 — v7.1.5 PAYLOAD ALIGNMENT + FIXED BACKSTOP ──────────
// 1. logSignal() field mapping overhaul (entry_type, cloud_dist_atr, etc.)
// 2. ProtoOANewOrderReq: removed trailingStopLoss:true. Fixed backstop only.
// 3. Order comment: HAWK|<strategy_id>|<entry_type><score>
// 4. BACKSTOP_HIT CRITICAL alert reinstated for LONG_STOP / SHORT_STOP.
// 5. Version strings corrected throughout.
// ─────────────────────────────────────────────────────────────────────────────

// ─── CHANGE v2.38.1 #1 of 3 ───────────────────────────────────────────────────
// Added cors require. No other change on this line.
const cors                  = require('cors');
// ─────────────────────────────────────────────────────────────────────────────
const { CTraderConnection } = require('@reiryoku/ctrader-layer');
const { createClient }      = require('@supabase/supabase-js');
const express               = require('express');

console.log('=== HAWK ENGINE v2.42.0 STARTING ===');

const UPSTASH_URL       = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN     = process.env.UPSTASH_REDIS_REST_TOKEN;
const SUPABASE_URL      = process.env.SUPABASE_URL;
const SUPABASE_KEY      = process.env.SUPABASE_SERVICE_KEY;
const CLIENT_ID         = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET     = process.env.CTRADER_CLIENT_SECRET;
const REFRESH_TOKEN     = process.env.CTRADER_REFRESH_TOKEN;
const ACCOUNT_ID        = parseInt(process.env.CTRADER_ACCOUNT_ID);
const IS_PAPER          = process.env.IS_PAPER === 'true';
const INTERNAL_SECRET   = process.env.INTERNAL_SECRET;
const PORT              = parseInt(process.env.PORT) || 3000;
const HOST              = IS_PAPER ? 'demo.ctraderapi.com' : 'live.ctraderapi.com';
// NOTE: NTFY_CRITICAL_URL and NTFY_OPS_URL removed in v2.40.0.
// sendNtfy() was dead code — all push notifications route via Supabase Edge Function.
// Remove both variables from Railway environment if still present.

console.log('IS_PAPER:', IS_PAPER, '| HOST:', HOST, '| ACCOUNT_ID:', ACCOUNT_ID);

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

let currentAccessToken = null;
let tokenExpiryTime    = null;

// v2.39.0 state variables
let lastAdvisoryDay         = null;  // date string — prevents >1 advisory per calendar day
let lastCriticalSentAt      = null;  // ms timestamp — gates hourly critical token alerts
const knownPositionIds      = new Set(); // position IDs seen since start — mismatch detection
const expiredSignalTimestamps = [];  // recent EXPIRED timestamps — spike detection

// TOKEN
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
  // v2.40.0: consolidated into TOKEN_EXPIRY_ADVISORY at 7-day threshold (replaces TOKEN_EXPIRY_WARNING)
  if (daysLeft < 7) {
    await logAlert('TOKEN_EXPIRY_ADVISORY', 'WARN',
      'cTrader access token expires in ' + daysLeft + ' day(s). Refresh required — see Railway token procedure.');
  }
  return currentAccessToken;
}

// SYMBOL MAP
const SYMBOL_MAP = {
  'XAUUSD':    'XAUUSD',
  'BTCUSD':    'BTCUSD',
  'ETHUSD':    'ETHUSD',
  'XAGUSD':    'XAGUSD',
  'NAS100':    'NAS100',
  'GER40':     'GER40',
  'AUS200':    'AUS200',
  'SPOTBRENT': 'SpotBrent',
  'GBPJPY':    'GBPJPY',
};

// STOP DISTANCE POINT MULTIPLIERS
const STOP_POINT_MULTIPLIER = {
  'XAUUSD': 100000, 'XAGUSD': 100000, 'BTCUSD': 100000, 'ETHUSD': 100000,
  'NAS100': 100000, 'GER40':  100000, 'AUS200': 100000, 'SPOTBRENT': 100000,
  'GBPJPY': 100000,
};

// PENDING ORDER REGISTRY
var pendingOrders = {};
function registerPending(symbolId, tradeSide, dbId) {
  var key = String(symbolId) + ':' + tradeSide;
  pendingOrders[key] = { dbId: dbId, registeredAt: Date.now() };
}
function resolvePending(symbolId, tradeSide) {
  var key = String(symbolId) + ':' + tradeSide;
  var entry = pendingOrders[key];
  if (entry) { delete pendingOrders[key]; return entry.dbId; }
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

// VOLUME
function getVolume(score, ticker) {
  const s = parseInt(score);
  switch (ticker) {
    case 'XAUUSD':    return s >= 9 ? 6      : s >= 8 ? 5      : 4;
    case 'BTCUSD':    return s >= 9 ? 3      : s >= 8 ? 2      : 1;
    case 'ETHUSD':    return s >= 9 ? 100    : s >= 8 ? 75     : 50;
    case 'XAGUSD':    return s >= 9 ? 30     : s >= 8 ? 20     : 10;
    case 'NAS100':    return s >= 9 ? 200    : s >= 8 ? 150    : 100;
    case 'GER40':     return s >= 9 ? 150    : s >= 8 ? 100    : 50;
    case 'AUS200':    return s >= 9 ? 200    : s >= 8 ? 150    : 100;
    case 'SPOTBRENT': return s >= 9 ? 200    : s >= 8 ? 150    : 100;
    case 'GBPJPY':    return s >= 9 ? 400000 : s >= 8 ? 200000 : 100000;
    default: console.warn('[VOLUME] No rule for', ticker, '— defaulting to 1'); return 1;
  }
}
const MIN_VOLUME = { 'NAS100': 10, 'GER40': 10, 'AUS200': 10 };
function resolveVolume(signal) {
  if (signal.lot_size !== undefined && signal.lot_size !== null && signal.lot_size !== '') {
    const lots = parseFloat(signal.lot_size);
    if (!isNaN(lots) && lots > 0) {
      const LOT_SIZE = {
        'XAUUSD': 10000, 'XAGUSD': 500000, 'SPOTBRENT': 10000, 'ETHUSD': 100,
        'NAS100': 100,   'GER40':  100,     'AUS200':    100,   'BTCUSD': 100,
        'GBPJPY': 10000000,
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

// STOP LOSS CALCULATION
function resolveStopLoss(signal) {
  var ticker     = signal.ticker;
  var multiplier = STOP_POINT_MULTIPLIER[ticker] || 100;
  if (signal.stop_distance !== undefined && signal.stop_distance !== null && signal.stop_distance !== '') {
    var dist = parseFloat(signal.stop_distance);
    if (!isNaN(dist) && dist > 0) {
      var stopPoints = Math.round(dist * multiplier);
      console.log('[STOP] payload stop_distance:', dist, '| multiplier:', multiplier,
        '| cTrader points:', stopPoints, '| ticker:', ticker);
      return stopPoints;
    }
  }
  var atr        = parseFloat(signal.atr) || 0;
  var stopPoints = Math.round(atr * 2 * multiplier);
  console.log('[STOP] FALLBACK atr×2:', atr * 2, '| multiplier:', multiplier,
    '| cTrader points:', stopPoints, '| ticker:', ticker);
  return stopPoints;
}

// SIGNAL HELPERS
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
  for (const [id, ts] of seenSignals) { if (now - ts > 10000) seenSignals.delete(id); }
  if (seenSignals.has(signalId)) return true;
  seenSignals.set(signalId, now);
  return false;
}

// SUPABASE LOGGING
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
      // ── v7.1.5 PAYLOAD FIELDS ────────────────────────────────────────────────
      // entry_type: MR or C (new in v7.1.0)
      entry_type:          signal.entry_type             || null,
      // Retained enrichment fields (unchanged from v7.0.0)
      atr_36:              signal.atr_36              != null ? parseFloat(signal.atr_36)              : null,
      rvol:                signal.rvol                != null ? parseFloat(signal.rvol)                : null,
      kijun_slope_pct:     signal.kijun_slope_pct     != null ? parseFloat(signal.kijun_slope_pct)    : null,
      ha_body_atr:         signal.ha_body_atr         != null ? parseFloat(signal.ha_body_atr)        : null,
      cloud_dist_atr:      signal.cloud_dist_atr      != null ? parseFloat(signal.cloud_dist_atr)     : null,
      tk_spread_atr:       signal.tk_spread_atr       != null ? parseFloat(signal.tk_spread_atr)      : null,
      tenkan_slope_atr:    signal.tenkan_slope_atr    != null ? parseFloat(signal.tenkan_slope_atr)   : null,
      session:             signal.session              || null,
      was_resurrection:    signal.was_resurrection != null
                             ? String(signal.was_resurrection) === 'true' : null,
      // Renamed from ema_mod (v7.1.1: exit mechanism changed to Chandelier)
      chandelier_mod:      signal.chandelier_mod      != null ? parseInt(signal.chandelier_mod)       : null,
      // New in v7.1.5: Chandelier buffer at entry in ATR units
      chandelier_dist_atr: signal.chandelier_dist_atr != null ? parseFloat(signal.chandelier_dist_atr) : null,
      // New in v7.1.5: real bar direction at entry confirmation (1=bullish, -1=bearish)
      real_bar_dir:        signal.real_bar_dir        != null ? parseInt(signal.real_bar_dir)         : null,
      // Renamed from slowk_mod (v7.1.0: Slow Kijun removed)
      d6_mod:              signal.d6_mod              != null ? parseInt(signal.d6_mod)               : null,
      fwd_cloud:           signal.fwd_cloud           != null ? parseInt(signal.fwd_cloud)            : null,
      chikou_vs_hist:      signal.chikou_vs_hist      != null ? parseInt(signal.chikou_vs_hist)       : null,
      // New in v7.1.0: MR architecture fields
      tk_cross_bars_ago:   signal.tk_cross_bars_ago   != null ? parseInt(signal.tk_cross_bars_ago)    : null,
      mr_dims_passed:      signal.mr_dims_passed      != null ? parseInt(signal.mr_dims_passed)       : null,
      // Replaces c_dims_passed (now a meaningful count regardless of CD gate ON/OFF)
      cd_gates_passed:     signal.cd_gates_passed     != null ? parseInt(signal.cd_gates_passed)      : null,
      wsf_active:          signal.wsf_active          != null ? parseInt(signal.wsf_active)           : null,
      atr_ratio:           signal.atr_ratio           != null ? parseFloat(signal.atr_ratio)          : null,
      cloud_thickness_atr: signal.cloud_thickness_atr != null ? parseFloat(signal.cloud_thickness_atr) : null,
      bars_since_flat:     signal.bars_since_flat     != null ? parseInt(signal.bars_since_flat)      : null,
      trend_aligned:       signal.trend_aligned       != null ? parseInt(signal.trend_aligned)        : null,
      ghost_bars_active:   signal.ghost_bars_active   != null ? parseInt(signal.ghost_bars_active)    : null,
      // ── v8.3 NEW FIELDS ──────────────────────────────────────────────────────
      utc_hour:            signal.utc_hour            != null ? parseInt(signal.utc_hour)             : null,
      dow_utc:             signal.dow_utc             != null ? parseInt(signal.dow_utc)              : null,
      ar_char:             signal.ar_char             || null,
      kijun_dist_atr:      signal.kijun_dist_atr      != null ? parseFloat(signal.kijun_dist_atr)    : null,
      stop_dist_atr:       signal.stop_dist_atr       != null ? parseFloat(signal.stop_dist_atr)     : null,
      backstop_level:      signal.backstop_level      != null ? parseFloat(signal.backstop_level)    : null,
      utc_hour_exit:       signal.utc_hour_exit       != null ? parseInt(signal.utc_hour_exit)       : null,
      // ── REMOVED from v6.0.3 (not in v7.1.5 payload) ─────────────────────────
      // ema_mod, slowk_mod → replaced by chandelier_mod and d6_mod above
      // d1, d2, d3_raw, d4, d5, d5_inv, d6, d7 → D-gate architecture retired
      // cloud_clearance_atr → replaced by cloud_dist_atr above
      // c_dims_passed → replaced by cd_gates_passed above
    }).select('id').single();
    if (error) throw error;
    console.log('Logged:', status, latencyMs ? '(' + latencyMs + 'ms)' : '', '| dbId:', data && data.id);
    return data && data.id || null;
  } catch (e) { console.error('Supabase signal_log error:', e.message); return null; }
}

async function logExecutionEvent(execType, symbolId, tradeSide, executionPrice,
                                  executedVolume, orderId, positionId, errorCode,
                                  rawEvent, signalLogId) {
  try {
    await supabase.from('execution_events').insert({
      received_at:         new Date().toISOString(),
      ctid_trader_account: ACCOUNT_ID,
      execution_type:      execType       || null,
      order_id:            orderId        || null,
      position_id:         positionId     || null,
      symbol_id:           symbolId       || null,
      trade_side:          tradeSide      || null,
      executed_volume:     executedVolume || null,
      execution_price:     executionPrice || null,
      error_code:          errorCode      || null,
      raw_event:           rawEvent ? JSON.parse(JSON.stringify(rawEvent)) : null,
      signal_log_id:       signalLogId    || null,
      is_paper:            IS_PAPER,
    });
  } catch (e) { console.error('Supabase execution_events error:', e.message); }
}

async function logHealth(status, tokenDaysLeft) {
  // FIXED v2.38.1: corrected column names to match actual health_log schema.
  // Previous: checked_at + token_days_left + is_paper — none of these columns exist.
  // Actual columns are: logged_at, token_expiry_days.
  try {
    await supabase.from('health_log').insert({
      status,
      token_expiry_days: tokenDaysLeft || null,
      logged_at:         new Date().toISOString(),
    });
  } catch (e) { console.error('Supabase health_log error:', e.message); }
}

// ACTIVE POSITIONS SYNC — C1 v2.38.2
// Runs every 60s inside the health interval.
// Upserts all open cTrader positions to active_positions, then deletes any
// rows whose position_id is no longer returned by reconcile (i.e. closed).
// If the reconcile call itself fails, the function returns without deleting
// anything — safety-first: never clear the table on a failed read.
async function syncActivePositions() {
  if (!isConnected) return;
  try {
    var posRes    = await connection.sendCommand('ProtoOAReconcileReq',
                     { ctidTraderAccountId: ACCOUNT_ID });
    var positions = posRes.position || [];
    var openIds   = positions.map(function(p) { return String(p.positionId); });

    if (positions.length > 0) {
      var rows = positions.map(function(p) {
        var symbolId = p.tradeData && p.tradeData.symbolId
                       ? String(p.tradeData.symbolId) : null;
        var openMs   = p.tradeData && p.tradeData.openTime
                       ? parseInt(p.tradeData.openTime) : null;
        return {
          position_id:    String(p.positionId),
          ticker:         symbolId ? (symbolIdToTicker[symbolId] || symbolId) : 'UNKNOWN',
          trade_side:     (p.tradeData && p.tradeData.tradeSide) || null,
          volume:         (p.tradeData && p.tradeData.volume)    || null,
          entry_price:    p.price       != null ? p.price       : null,
          stop_loss:      p.stopLoss    != null ? p.stopLoss    : null,
          take_profit:    p.takeProfit  != null ? p.takeProfit  : null,
          swap:           p.swap        != null ? p.swap        : null,
          open_time:      openMs        != null ? new Date(openMs).toISOString() : null,
          unrealised_pnl: null, // not available from reconcile; requires spot subscription
          is_paper:       IS_PAPER,
          updated_at:     new Date().toISOString(),
        };
      });
      var { error: upsertErr } = await supabase.from('active_positions')
        .upsert(rows, { onConflict: 'position_id' });
      if (upsertErr) throw new Error('Upsert failed: ' + upsertErr.message);
    }

    // Delete positions no longer open — only runs if reconcile succeeded
    if (openIds.length > 0) {
      await supabase.from('active_positions')
        .delete()
        .eq('is_paper', IS_PAPER)
        .not('position_id', 'in', '(' + openIds.map(function(id) {
          return '"' + id + '"';
        }).join(',') + ')');
    } else {
      // Reconcile returned zero positions — clear all rows for this mode
      await supabase.from('active_positions').delete().eq('is_paper', IS_PAPER);
    }

    console.log('[SYNC] active_positions | open:', positions.length,
      openIds.length > 0 ? '(' + openIds.join(', ') + ')' : '(none)');

    // v2.39.0: position state mismatch detection — fires once per new unknown position_id
    for (var i = 0; i < openIds.length; i++) {
      var pid = openIds[i];
      if (!knownPositionIds.has(pid)) {
        try {
          var mPos     = positions.find(function(p) { return String(p.positionId) === pid; });
          var mSymId   = mPos && mPos.tradeData ? String(mPos.tradeData.symbolId) : null;
          var mTicker  = mSymId ? (symbolIdToTicker[mSymId] || 'UNKNOWN') : 'UNKNOWN';
          var slCheck  = await supabase.from('signal_log')
            .select('id').eq('position_id', pid).eq('is_paper', IS_PAPER).limit(1);
          if (!slCheck.data || slCheck.data.length === 0) {
            await logAlert('POSITION_STATE_MISMATCH', 'WARN',
              'cTrader position ' + pid + ' (' + mTicker + ') has no engine signal_log record. ' +
              'May have been opened outside the pipeline or engine restarted during a trade.');
          }
        } catch (mismatchErr) {
          console.error('[SYNC] Mismatch check error:', mismatchErr.message);
        }
        knownPositionIds.add(pid);
      }
    }
    for (var knownPid of Array.from(knownPositionIds)) {
      if (!openIds.includes(knownPid)) knownPositionIds.delete(knownPid);
    }
  } catch (err) {
    var msg = (err && err.message) ? err.message : String(err);
    console.error('[SYNC] active_positions error — table not modified:', msg);
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
  } catch (e) { console.error('Supabase alerts error:', e.message); }
}

// SYMBOL SCHEDULE QUERY
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
      ctidTraderAccountId: ACCOUNT_ID, symbolId: ids,
    });
    var symbols = res.symbol || [];
    for (var s of symbols) {
      var ticker      = symbolIdToTicker[String(s.symbolId)] || 'UNKNOWN';
      var sessions    = [];
      var rawSchedule = null;
      console.log('[SCHEDULE RAW]', ticker, JSON.stringify(s.schedule));
      console.log('[SYMBOL SPEC]', ticker,
        '| minStopLoss:', s.minStopLossDistance, '| minVolume:', s.minVolume,
        '| maxVolume:', s.maxVolume, '| lotSize:', s.lotSize,
        '| digits:', s.digits, '| pipPosition:', s.pipPosition, '| stepVolume:', s.stepVolume);
      try {
        rawSchedule = s.schedule || null;
        var intervals = Array.isArray(s.schedule) ? s.schedule : [];
        intervals.forEach(function(iv) {
          sessions.push(secondsToHuman(iv.startSecond) + ' -> ' + secondsToHuman(iv.endSecond));
        });
      } catch (e) { console.warn('Schedule parse error for', ticker, ':', e.message); }
      var humanStr = sessions.length > 0 ? sessions.join(' | ') : 'NO SCHEDULE DATA RETURNED';
      console.log('[SCHEDULE]', ticker, '(symbolId:' + s.symbolId + ')', humanStr);
      try {
        await supabase.from('symbol_schedules').insert({
          queried_at: new Date().toISOString(), ticker, symbol_id: s.symbolId,
          symbol_name: s.symbolName || null, sessions_raw: rawSchedule || null,
          sessions_human: humanStr, is_paper: IS_PAPER,
        });
      } catch (e) { console.error('symbol_schedules insert error for', ticker, ':', e.message); }
    }
    console.log('=== SYMBOL SCHEDULES COMPLETE ===');
  } catch (err) {
    var msg = (err && err.message) ? err.message : JSON.stringify(err);
    console.error('Symbol schedule query failed:', msg);
    await logAlert('SCHEDULE_QUERY_FAILED', 'WARN',
      'Symbol schedule query failed — session data not updated. Engine continues operating but schedule display may be stale. Error: ' + msg);
  }
}

// DEAL LIST DIAGNOSTIC
async function queryRecentDeals(ctSymbol, dbId, symbolId, signal) {
  try {
    var toTs   = Date.now();
    var fromTs = toTs - 30000;
    var res    = await connection.sendCommand('ProtoOADealListReq', {
      ctidTraderAccountId: ACCOUNT_ID, fromTimestamp: fromTs,
      toTimestamp: toTs, maxRows: 10,
    });
    var allDeals = res.deal || [];
    var deals    = symbolId
      ? allDeals.filter(function(d) { return String(d.symbolId) === String(symbolId); })
      : allDeals;
    if (deals.length === 0) {
      console.warn('[DEAL LIST] No deals found in last 30s for', ctSymbol,
        '— order was likely rejected by cTrader | dbId:', dbId);
      await supabase.from('signal_log').update({
        status: 'REJECTED',
        error_message: 'No deal recorded by cTrader within 30s — order rejected silently',
      }).eq('id', dbId);
      await logAlert('ORDER_SILENT_REJECT', 'WARN',
        ctSymbol + ' order sent but no deal recorded by cTrader within 30s. dbId:' + dbId
        + ' | action:' + (signal && signal.action ? signal.action : 'UNKNOWN')
        + ' | atr:' + (signal && signal.atr ? signal.atr : 'UNKNOWN')
        + '. Investigate via Supabase signal_log and cTrader deal history.');
    } else {
      var deal          = deals[0];
      var fillPrice     = deal.executionPrice ? deal.executionPrice : null;
      var tvClose       = signal && signal.close ? parseFloat(signal.close) : null;
      var marginRate    = deal.marginRate  ? parseFloat(deal.marginRate) : null;
      var commissionRaw = deal.commission  ? parseFloat(deal.commission) : 0;
      var tradeSide     = deal.tradeSide;
      var entrySlippage = null;
      if (fillPrice != null && tvClose != null) {
        entrySlippage = tradeSide === 'BUY'
          ? parseFloat((fillPrice - tvClose).toFixed(5))
          : parseFloat((tvClose - fillPrice).toFixed(5));
      }
      var spreadAtEntry = null;
      if (fillPrice != null && marginRate != null && marginRate >= 10) {
        spreadAtEntry = parseFloat(Math.abs(fillPrice - marginRate).toFixed(5));
      }
      console.log('[DEAL LIST] Deals found after order:', JSON.stringify(deals));
      console.log('[DEAL LIST] CONFIRMED EXECUTION | dealId:', deal.dealId,
        '| fillPrice:', fillPrice, '| slippage:', entrySlippage,
        '| spread:', spreadAtEntry, '| commission:', commissionRaw,
        '| status:', deal.dealStatus, '| dbId:', dbId);
      await supabase.from('signal_log').update({
        status: 'EXECUTED', fill_price: fillPrice,
        entry_slippage: entrySlippage, spread_at_entry: spreadAtEntry,
        commission_usd: commissionRaw,
        api_response: JSON.stringify({
          dealId: deal.dealId, executionPrice: fillPrice,
          dealStatus: deal.dealStatus, source: 'deal_list_query',
        }),
      }).eq('id', dbId);
    }
  } catch (err) { console.error('[DEAL LIST] Query error:', err.message); }
}

// EXECUTION EVENT LISTENER
function attachExecutionEventListener() {
  connection.on('ProtoOAExecutionEvent', async function(event) {
    try {
      var desc           = event.descriptor;
      var execType       = desc.executionType || null;
      var order          = desc.order         || {};
      var tradeData      = order.tradeData    || {};
      var symbolId       = tradeData.symbolId || null;
      var tradeSide      = tradeData.tradeSide || null;
      var orderId        = order.orderId       || null;
      var orderType      = order.orderType     || null;
      var closingOrder   = order.closingOrder === true;
      var positionId     = (desc.position && desc.position.positionId) || null;
      var deal           = desc.deal           || null;
      var executionPrice = deal && deal.executionPrice ? deal.executionPrice
                         : order.executionPrice        ? order.executionPrice : null;
      var executedVolume = tradeData.volume    || null;
      var errorCode      = desc.errorCode      || null;
      var isServer       = desc.isServerEvent === true;
      var ticker         = symbolId
        ? (symbolIdToTicker[String(symbolId)] || String(symbolId)) : 'UNKNOWN';

      console.log('[EXEC EVENT]', execType, '| ticker:', ticker, '| side:', tradeSide,
        '| price:', executionPrice, '| orderType:', orderType,
        '| isServer:', isServer, '| errorCode:', errorCode || 'none');

      if (execType === 'ORDER_FILLED' && orderType === 'STOP_LOSS_TAKE_PROFIT' && isServer) {
        var slPositionId = positionId ? String(positionId) : null;
        console.log('[SL FILL DETECTED] ticker:', ticker,
          '| positionId:', slPositionId, '| fillPrice:', executionPrice);
        await logExecutionEvent(execType, symbolId, tradeSide, executionPrice,
          executedVolume, orderId, positionId, errorCode, desc, null);
        try {
          var { data: slRow } = await supabase.from('signal_log')
            .select('id, score').eq('position_id', slPositionId).eq('status', 'EXECUTED').single();
          if (slRow) {
            await supabase.from('signal_log').update({
              status: 'STOPPED', fill_price: executionPrice,
            }).eq('id', slRow.id);
            await logAlert('SL_FILL', 'WARN',
              ticker + ' SL fill | positionId:' + slPositionId
              + ' | fillPrice:' + executionPrice + ' | dbId:' + slRow.id);
          } else {
            console.warn('[SL FILL] No matching signal_log for positionId:', slPositionId);
            await logAlert('SL_FILL_UNMATCHED', 'WARN',
              ticker + ' SL fill — no signal_log match. positionId:' + slPositionId
              + ' fillPrice:' + executionPrice);
          }
        } catch (slErr) { console.error('[SL FILL] Lookup error:', slErr.message); }
        return;
      }

      var dbId = null;
      if (execType === 'ORDER_FILLED' && orderType === 'MARKET' && !closingOrder
          && symbolId && tradeSide) {
        dbId = resolvePending(symbolId, tradeSide);
      }
      await logExecutionEvent(execType, symbolId, tradeSide, executionPrice,
        executedVolume, orderId, positionId, errorCode, desc, dbId);
      if (!dbId) {
        console.log('[EXEC EVENT] No pending order match for', ticker, tradeSide,
          '— audit record written');
        return;
      }
      if (execType === 'ORDER_FILLED') {
        await supabase.from('signal_log').update({
          status: 'EXECUTED', position_id: positionId ? String(positionId) : null,
          fill_price: executionPrice,
          api_response: JSON.stringify({ positionId, executionPrice, executedVolume,
            source: 'execution_event' }),
        }).eq('id', dbId);
      } else if (execType === 'ORDER_REJECTED' || execType === 'ORDER_CANCELLED'
                 || execType === 'ORDER_EXPIRED') {
        await supabase.from('signal_log').update({
          status: 'REJECTED', rejection_reason: errorCode || execType,
          error_message: 'Order ' + execType + ' by broker. Code: ' + (errorCode || 'none'),
        }).eq('id', dbId);
        await logAlert('ORDER_REJECTED', 'WARN', ticker + ' ' + tradeSide + ' ' + execType
          + '. Code: ' + (errorCode || 'none') + ' | dbId:' + dbId);
      }
    } catch (err) {
      console.error('[EXEC EVENT] Handler error:', (err && err.message) ? err.message : err);
    }
  });
  console.log('ProtoOAExecutionEvent listener attached');
}

// STARTUP: CLOSE ALL OPEN POSITIONS
async function closeAllOpenPositions() {
  console.log('[STARTUP] Checking for open positions to close...');
  try {
    var posRes    = await connection.sendCommand('ProtoOAReconcileReq', { ctidTraderAccountId: ACCOUNT_ID });
    var positions = posRes.position || [];
    if (positions.length === 0) {
      console.log('[STARTUP] No open positions found.');
      await logAlert('STARTUP_POSITIONS_CLOSED', 'INFO', 'Startup check: no open positions found.');
      return;
    }
    console.log('[STARTUP] Found ' + positions.length + ' open position(s). Closing all...');
    var closed = [], failed = [];
    for (var p of positions) {
      var symbolId   = p.tradeData && p.tradeData.symbolId ? String(p.tradeData.symbolId) : 'UNKNOWN';
      var ticker     = symbolIdToTicker[symbolId] || symbolId;
      var positionId = p.positionId ? String(p.positionId) : null;
      var volume     = p.tradeData && p.tradeData.volume    ? p.tradeData.volume    : null;
      var tradeSide  = p.tradeData && p.tradeData.tradeSide ? p.tradeData.tradeSide : null;
      try {
        await connection.sendCommand('ProtoOAClosePositionReq', {
          ctidTraderAccountId: ACCOUNT_ID, positionId: p.positionId, volume,
        });
        await supabase.from('signal_log').insert({
          signal_id: 'STARTUP_CLOSE_' + positionId, strategy_id: 'SYSTEM',
          ticker, action: 'STARTUP_CLOSE', score: null, atr: null, close_price: null,
          status: 'CLOSED', error_message: null,
          api_response: JSON.stringify({ positionId, tradeSide, source: 'startup_close' }),
          signal_time: new Date().toISOString(), processed_at: new Date().toISOString(),
          is_paper: IS_PAPER, latency_ms: null,
        });
        closed.push(ticker);
      } catch (e) {
        var errMsg = (e && e.message) ? e.message : String(e);
        console.error('[STARTUP] Failed to close | ticker:', ticker, '| error:', errMsg);
        await supabase.from('signal_log').insert({
          signal_id: 'STARTUP_CLOSE_FAIL_' + positionId, strategy_id: 'SYSTEM',
          ticker, action: 'STARTUP_CLOSE', score: null, atr: null, close_price: null,
          status: 'ERROR', error_message: 'Startup close failed: ' + errMsg,
          api_response: null, signal_time: new Date().toISOString(),
          processed_at: new Date().toISOString(), is_paper: IS_PAPER, latency_ms: null,
        });
        failed.push(ticker);
      }
    }
    var summary  = 'Startup close: ' + closed.length + ' closed'
      + (closed.length > 0 ? ' (' + closed.join(', ') + ')' : '')
      + (failed.length > 0 ? ' | ' + failed.length + ' FAILED (' + failed.join(', ') + ')' : '');
    var severity = failed.length > 0 ? 'CRITICAL' : 'WARN';
    await logAlert('STARTUP_POSITIONS_CLOSED', severity, summary);
  } catch (err) {
    var msg = (err && err.message) ? err.message : String(err);
    console.error('[STARTUP] closeAllOpenPositions error:', msg);
    await logAlert('STARTUP_CLOSE_ERROR', 'CRITICAL', 'Startup position close query failed: ' + msg);
  }
}

// WATCHDOG
async function runWatchdog() {
  if (!isConnected || reconnecting) return;
  try {
    await Promise.race([
      connection.sendCommand('ProtoOAReconcileReq', { ctidTraderAccountId: ACCOUNT_ID }),
      new Promise(function(_, reject) { setTimeout(function() {
        reject(new Error('Watchdog timeout after 8s')); }, 8000); }),
    ]);
    lastWatchdogOk   = new Date().toISOString();
    watchdogFailures = 0;
    console.log('[WATCHDOG] OK | connection verified |', lastWatchdogOk);
  } catch (err) {
    watchdogFailures++;
    var msg = (err && err.message) ? err.message : String(err);
    console.error('[WATCHDOG] FAIL #' + watchdogFailures + ' |', msg);
    await logAlert('WATCHDOG_FAIL', 'WARN', 'Watchdog failure #' + watchdogFailures + ': ' + msg);
    if (watchdogFailures >= 2) {
      await logAlert('WATCHDOG_RECONNECT', 'CRITICAL',
        'Watchdog forced reconnect after ' + watchdogFailures + ' failures. isConnected was: ' + isConnected);
      isConnected = false; reconnecting = false; watchdogFailures = 0;
      connectToCTrader();
    }
  }
}

// CONNECTION
let connection = null, isConnected = false, reconnecting = false;
let symbolIdMap = {}, symbolIdToTicker = {};
let lastWatchdogOk = null, watchdogFailures = 0;

async function connectToCTrader() {
  if (reconnecting) return;
  reconnecting = true; isConnected = false;
  try {
    console.log('Connecting to cTrader...');
    connection = new CTraderConnection({ host: HOST, port: 5035 });
    connection.on('close', function() {
      console.warn('cTrader connection closed — scheduling reconnect');
      isConnected = false; reconnecting = false;
      logAlert('WEBSOCKET_CLOSED', 'WARN', 'cTrader WebSocket connection closed unexpectedly. Reconnect scheduled automatically. If this persists, check Railway logs and cTrader API status.');
      setTimeout(connectToCTrader, 3000);
    });
    connection.on('error', function(err) {
      console.error('cTrader connection error:', err.message); isConnected = false;
    });
    await connection.open();
    console.log('Connected to cTrader');
    attachExecutionEventListener();
    await connection.sendCommand('ProtoOAApplicationAuthReq', { clientId: CLIENT_ID, clientSecret: CLIENT_SECRET });
    console.log('Application authenticated');
    await connection.sendCommand('ProtoOAAccountAuthReq', { ctidTraderAccountId: ACCOUNT_ID, accessToken: currentAccessToken });
    console.log('Account authenticated:', ACCOUNT_ID);
    const symRes = await connection.sendCommand('ProtoOASymbolsListReq', {
      ctidTraderAccountId: ACCOUNT_ID, includeArchivedSymbols: false,
    });
    symbolIdMap = {}; symbolIdToTicker = {};
    (symRes.symbol || []).forEach(function(s) { symbolIdMap[s.symbolName] = s.symbolId; });
    console.log('Symbols loaded:', Object.keys(symbolIdMap).length);
    Object.keys(SYMBOL_MAP).forEach(function(tv) {
      var ct = SYMBOL_MAP[tv], id = symbolIdMap[ct];
      console.log(' ', tv, '->', ct, '-> symbolId:', id || 'NOT FOUND');
      if (id) symbolIdToTicker[String(id)] = tv;
    });
    setInterval(function() { connection.sendHeartbeat(); }, 25000);
    isConnected = true; reconnecting = false;
    console.log('=== ENGINE READY | Mode:', IS_PAPER ? 'PAPER' : 'LIVE', '===');
    await logAlert('ENGINE_READY', 'INFO', 'Engine v2.42.0 connected. Mode: ' + (IS_PAPER ? 'PAPER' : 'LIVE'));
    await closeAllOpenPositions();
    setInterval(runWatchdog, 10 * 60 * 1000);
    querySymbolSchedules().catch(function(e) { console.error('Symbol schedule query error:', e.message); });
    var startupElapsedMs = Date.now() - (global.engineStartMs || Date.now());
    await logAlert('STARTUP_COMPLETE', 'INFO',
      'Engine v2.42.0 startup complete in ' + startupElapsedMs + 'ms. Mode: ' + (IS_PAPER ? 'PAPER' : 'LIVE'));
  } catch (err) {
    var msg = (err && err.message) ? err.message : JSON.stringify(err);
    console.error('cTrader connection failed:', msg);
    reconnecting = false;
    await logAlert('CONNECTION_FAILED', 'CRITICAL', msg);
    setTimeout(connectToCTrader, 5000);
  }
}

// QUEUE WASHDOWN
async function washdownQueue() {
  try {
    var flushed = 0;
    while (true) {
      var res  = await fetch(UPSTASH_URL + '/rpop/hawk:signals', { headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN } });
      var data = await res.json();
      if (!data.result) break;
      flushed++;
    }
    if (flushed > 0) {
      console.log('Pipeline washdown: flushed', flushed, 'stale signals');
      await logAlert('PIPELINE_RESET', 'WARN', 'Startup washdown flushed ' + flushed + ' stale signals.');
    } else { console.log('Pipeline washdown: queue was empty'); }
  } catch (e) { console.error('Washdown error:', e.message); }
}

// LIVE REDIS POLLER
var redisPollerActive = false;

function startRedisPoller() {
  if (redisPollerActive) return;
  redisPollerActive = true;
  console.log('[REDIS POLL] Live poller started (1s interval)');
  setInterval(async function() {
    if (!isConnected) return;
    try {
      var res  = await fetch(UPSTASH_URL + '/rpop/hawk:signals', {
        headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN },
      });
      var data = await res.json();
      if (!data.result) return;
      var signal;
      try {
        signal = JSON.parse(data.result);
      } catch (e) {
        console.error('[REDIS POLL] Parse error:', e.message);
        return;
      }
      if (isDuplicate(signal.signal_id)) {
        console.log('[REDIS POLL] Duplicate', signal.signal_id, '— discarded');
        return;
      }
      var ageMs = signal.received_at
        ? Date.now() - new Date(signal.received_at).getTime()
        : null;
      if (isExpired(signal)) {
        console.log('[REDIS POLL] EXPIRED', signal.signal_id, signal.ticker,
          ageMs != null ? '(' + ageMs + 'ms old)' : '');
        await logSignal(signal, null, 'EXPIRED', 'Redis fallback signal exceeded TTL', ageMs);
        return;
      }
      console.log('[REDIS POLL] Executing fallback signal', signal.signal_id,
        signal.ticker, ageMs != null ? '(' + ageMs + 'ms)' : '');
      await logAlert('REDIS_FALLBACK_EXECUTED', 'WARN',
        'Redis fallback executed | ticker: ' + signal.ticker
        + ' | signal_id: ' + signal.signal_id
        + ' | age: ' + ageMs + 'ms');
      setImmediate(function() { executeSignal(signal); });
    } catch (e) {
      console.error('[REDIS POLL] Error:', e.message);
    }
  }, 1000);
}

// RECONCILE FALLBACK
async function reconcileConfirm(dbId, symbolId, isLong) {
  try {
    var { data: row } = await supabase.from('signal_log').select('status').eq('id', dbId).single();
    if (row && (row.status === 'EXECUTED' || row.status === 'REJECTED')) {
      console.log('[RECONCILE] dbId:' + dbId + ' already resolved (' + row.status + ') — skipping'); return;
    }
    var posRes    = await connection.sendCommand('ProtoOAReconcileReq', { ctidTraderAccountId: ACCOUNT_ID });
    var tradeSide = isLong ? 'BUY' : 'SELL';
    var position  = (posRes.position || []).find(function(p) {
      return String(p.tradeData && p.tradeData.symbolId) === String(symbolId)
        && p.tradeData && p.tradeData.tradeSide === tradeSide;
    });
    if (!position) {
      await supabase.from('signal_log').update({ status: 'PENDING_EXECUTION_EVENT' }).eq('id', dbId); return;
    }
    var positionId = position.positionId ? String(position.positionId) : null;
    await supabase.from('signal_log').update({
      status: 'EXECUTED', position_id: positionId,
      api_response: JSON.stringify({ positionId, tradeSide, source: 'reconcile_fallback' }),
    }).eq('id', dbId);
  } catch (err) {
    console.error('[RECONCILE FALLBACK] Error:', err.message);
    await logAlert('RECONCILE_ERROR', 'CRITICAL', 'Reconcile fallback failed for dbId:' + dbId + ' — ' + err.message);
  }
}

async function reconcileExitConfirm(dbId, positionId, symbolId, isLong, attempt) {
  attempt = attempt || 1;
  try {
    var posRes    = await connection.sendCommand('ProtoOAReconcileReq', { ctidTraderAccountId: ACCOUNT_ID });
    var tradeSide = isLong ? 'BUY' : 'SELL';
    var stillOpen = (posRes.position || []).find(function(p) {
      return String(p.tradeData && p.tradeData.symbolId) === String(symbolId)
        && p.tradeData && p.tradeData.tradeSide === tradeSide;
    });
    if (stillOpen && attempt < 3) {
      setTimeout(function() { reconcileExitConfirm(dbId, positionId, symbolId, isLong, attempt + 1); }, 1000); return;
    }
    if (stillOpen) {
      var tickerLabel = symbolId ? (symbolIdToTicker[String(symbolId)] || String(symbolId)) : 'UNKNOWN';
      await logAlert('EXIT_UNCONFIRMED', 'WARN',
        'Position still open after ' + attempt + ' reconcile checks. ticker:' + tickerLabel
        + ' | positionId:' + (positionId || 'UNKNOWN') + ' | dbId:' + dbId
        + '. Manual check required.'); return;
    }
    await supabase.from('signal_log').update({
      status: 'CLOSED', position_id: positionId,
      api_response: JSON.stringify({ positionId, tradeSide }),
    }).eq('id', dbId);
  } catch (err) {
    console.error('Exit reconcile error:', err.message);
    await logAlert('EXIT_RECONCILE_ERROR', 'CRITICAL', 'Exit reconcile failed for dbId:' + dbId + ' — ' + err.message);
  }
}

// SIGNAL EXECUTION
async function executeSignal(signal) {
  var latencyMs = getLatencyMs(signal);
  if (latencyMs !== null && latencyMs > 3000) {
    await logAlert('LATENCY_CRITICAL', 'CRITICAL', 'Signal latency ' + latencyMs + 'ms exceeds 3000ms. ticker: '
      + (signal.ticker || 'UNKNOWN') + ' | signal_id: ' + (signal.signal_id || 'UNKNOWN'));
  } else if (latencyMs !== null && latencyMs > 1500) {
    await logAlert('LATENCY_WARN', 'WARN', 'Signal latency ' + latencyMs + 'ms exceeds 1500ms. ticker: '
      + (signal.ticker || 'UNKNOWN') + ' | signal_id: ' + (signal.signal_id || 'UNKNOWN'));
  } else if (latencyMs !== null && latencyMs > 500) {
    await logAlert('LATENCY_ADVISORY', 'INFO', 'Signal latency ' + latencyMs + 'ms exceeds 500ms advisory. ticker: '
      + (signal.ticker || 'UNKNOWN') + ' | signal_id: ' + (signal.signal_id || 'UNKNOWN'));
  }
  if (isExpired(signal)) {
    console.warn('Signal EXPIRED:', signal.signal_id, '| age:', latencyMs + 'ms');
    await logSignal(signal, null, 'EXPIRED', 'Signal age exceeded 5000ms', latencyMs);
    // v2.39.0: expired signal spike detection
    var expNow = Date.now();
    expiredSignalTimestamps.push(expNow);
    while (expiredSignalTimestamps.length > 0 && expiredSignalTimestamps[0] < expNow - 300000) {
      expiredSignalTimestamps.shift();
    }
    if (expiredSignalTimestamps.length > 3) {
      var spkCount = expiredSignalTimestamps.length;
      expiredSignalTimestamps.length = 0;
      await logAlert('EXPIRED_SIGNALS_SPIKE', 'WARN',
        spkCount + ' signals expired in 5 minutes. Railway latency elevated or engine under load.');
    }
    return;
  }
  if (isDuplicate(signal.signal_id)) { console.log('Duplicate signal ignored:', signal.signal_id); return; }
  if (!isConnected) {
    console.warn('Engine not connected — signal dropped');
    await logAlert('ENGINE_NOT_CONNECTED', 'CRITICAL',
      'Signal received but engine not connected to cTrader — signal dropped. ticker:'
      + (signal.ticker || 'UNKNOWN') + ' | action:' + (signal.action || 'UNKNOWN')
      + ' | signal_id:' + (signal.signal_id || 'UNKNOWN') + '. Check Railway logs and cTrader connection.');
    await logSignal(signal, null, 'ERROR', 'Engine not connected', latencyMs); return;
  }
  var action     = signal.action;
  var isEntry    = action === 'LONG' || action === 'SHORT';
  var isReversal = action === 'LONG_R_REV' || action === 'SHORT_R_REV';
  var isExit     = action === 'LONG_EXIT'      || action === 'SHORT_EXIT'    ||
                   action === 'LONG_STOP'      || action === 'SHORT_STOP'    ||
                   action === 'LONG_MKT_CLOSE' || action === 'SHORT_MKT_CLOSE';
  // R_REV: isLong reflects the NEW direction being opened (the reversal target)
  var isLong     = action === 'LONG' || action === 'LONG_R_REV' ||
                   action === 'LONG_EXIT' || action === 'LONG_STOP' || action === 'LONG_MKT_CLOSE';
  var ctSymbol  = SYMBOL_MAP[signal.ticker] || signal.ticker;
  var symbolId  = symbolIdMap[ctSymbol];
  var tradeSide = isLong ? 'BUY' : 'SELL';
  if (!symbolId) {
    console.error('Symbol not found:', ctSymbol);
    await logSignal(signal, null, 'ERROR', 'Symbol not found: ' + ctSymbol, latencyMs); return;
  }
  try {
    if (isEntry) {
      var posRes   = await connection.sendCommand('ProtoOAReconcileReq', { ctidTraderAccountId: ACCOUNT_ID });
      var existing = (posRes.position || []).find(function(p) {
        return String(p.tradeData && p.tradeData.symbolId) === String(symbolId)
          && p.tradeData && p.tradeData.tradeSide === tradeSide;
      });
      if (existing) {
        console.log('Position already open — skipping entry:', ctSymbol);
        await logSignal(signal, null, 'DUPLICATE_POSITION', null, latencyMs); return;
      }
      var volume   = resolveVolume(signal);
      var stopLoss = resolveStopLoss(signal);
      if (volume === 0) {
        await logSignal(signal, null, 'SKIPPED', 'Zero volume — review lot size configuration', latencyMs); return;
      }
      console.log('Order |', ctSymbol, '|', tradeSide, '|', volume, 'units | SL:', stopLoss, 'pts | latency:', latencyMs + 'ms');
      var dbId = await logSignal(signal, { symbolId, volume, stopLoss }, 'PENDING_FILL', null, latencyMs);
      if (dbId) registerPending(symbolId, tradeSide, dbId);
      try {
        await connection.sendCommand('ProtoOANewOrderReq', {
          ctidTraderAccountId: ACCOUNT_ID, symbolId, orderType: 'MARKET',
          tradeSide, volume,
          relativeStopLoss: stopLoss,
          // v2.41.0: trailingStopLoss removed. Backstop is fixed (kijun ± N×ATR at entry).
          // cTrader holds a static stop level. Exits handled by Pine Chandelier + HA condition.
          comment: 'HAWK|' + signal.strategy_id + '|' + (signal.entry_type || 'C') + (signal.score || '9'),
        });
        console.log('[ORDER] Sent to cTrader | ticker:', ctSymbol, '| side:', tradeSide,
          '| volume:', volume, '| stopLoss:', stopLoss, 'pts');
      } catch (e) {
        console.error('[ORDER ERROR] cTrader rejected order:', e.message, '| ticker:', ctSymbol,
          '| errorCode:', (e.errorCode || 'none'));
        if (dbId) resolvePending(symbolId, tradeSide);
        await supabase.from('signal_log').update({
          status: 'ERROR', error_message: e.message + (e.errorCode ? ' | code: ' + e.errorCode : ''),
        }).eq('id', dbId);
        await logAlert('ORDER_REJECTED', 'WARN', ctSymbol + ' ' + tradeSide + ' rejected by cTrader.'
          + ' Error: ' + e.message + (e.errorCode ? ' | Code: ' + e.errorCode : '')
          + ' | volume: ' + volume + ' | stopLoss: ' + stopLoss + 'pts | dbId: ' + dbId);
        return;
      }
      setTimeout(function() { reconcileConfirm(dbId, symbolId, isLong); }, 2000);
      setTimeout(function() { queryRecentDeals(ctSymbol, dbId, symbolId, signal); }, 4000);
    } else if (isReversal) {
      // R_REV: close the existing C position in the OPPOSITE direction, then open R in the new direction.
      // isLong reflects the NEW direction. The existing position is in the opposite side.
      var existingSide = isLong ? 'SELL' : 'BUY';
      var posResRev    = await connection.sendCommand('ProtoOAReconcileReq', { ctidTraderAccountId: ACCOUNT_ID });
      var revPosition  = (posResRev.position || []).find(function(p) {
        return String(p.tradeData && p.tradeData.symbolId) === String(symbolId)
          && p.tradeData && p.tradeData.tradeSide === existingSide;
      });
      if (!revPosition) {
        console.log('[R_REV] No existing', existingSide, 'position to reverse for', ctSymbol, '— skipping');
        await logSignal(signal, null, 'NO_POSITION', 'R_REV: no existing ' + existingSide + ' position to close', latencyMs);
        return;
      }
      var revPositionId = String(revPosition.positionId);
      console.log('[R_REV] Closing', existingSide, 'positionId:', revPositionId, 'then opening', isLong ? 'LONG' : 'SHORT');
      // Log the close leg
      var revCloseDbId = await logSignal(signal, { positionId: revPositionId }, 'PENDING_CLOSE', null, latencyMs);
      try {
        await connection.sendCommand('ProtoOAClosePositionReq', {
          ctidTraderAccountId: ACCOUNT_ID,
          positionId:          revPosition.positionId,
          volume:              revPosition.tradeData.volume,
        });
        console.log('[R_REV] Close sent for positionId:', revPositionId);
        await logAlert('R_REV_CLOSE', 'INFO',
          'R_REV close sent | ticker: ' + ctSymbol + ' | closed: ' + existingSide
          + ' positionId: ' + revPositionId + ' | opening: ' + (isLong ? 'LONG' : 'SHORT'));
      } catch (closeErr) {
        console.error('[R_REV] Close error:', closeErr.message);
        await supabase.from('signal_log').update({ status: 'ERROR', error_message: closeErr.message }).eq('id', revCloseDbId);
        await logAlert('R_REV_CLOSE_FAILED', 'CRITICAL',
          'R_REV close FAILED | ticker: ' + ctSymbol + ' | positionId: ' + revPositionId
          + ' | error: ' + closeErr.message + ' | New entry NOT sent — manual intervention required.');
        return;
      }
      // Brief pause to allow cTrader to process the close before opening the new position
      await new Promise(function(resolve) { setTimeout(resolve, 500); });
      // Open the new R position
      var revVolume   = resolveVolume(signal);
      var revStopLoss = resolveStopLoss(signal);
      if (revVolume === 0) {
        await logSignal(signal, null, 'SKIPPED', 'R_REV: zero volume on new entry — review lot size', latencyMs);
        return;
      }
      var revEntryDbId = await logSignal(signal, { symbolId, volume: revVolume, stopLoss: revStopLoss }, 'PENDING_FILL', null, latencyMs);
      var revTradeSide = isLong ? 'BUY' : 'SELL';
      if (revEntryDbId) registerPending(symbolId, revTradeSide, revEntryDbId);
      try {
        await connection.sendCommand('ProtoOANewOrderReq', {
          ctidTraderAccountId: ACCOUNT_ID, symbolId, orderType: 'MARKET',
          tradeSide: revTradeSide, volume: revVolume,
          relativeStopLoss: revStopLoss,
          comment: 'HAWK|' + signal.strategy_id + '|R_REV' + (signal.score || '7'),
        });
        console.log('[R_REV] New entry sent | ticker:', ctSymbol, '| side:', revTradeSide, '| volume:', revVolume);
      } catch (entryErr) {
        console.error('[R_REV] New entry error:', entryErr.message);
        if (revEntryDbId) resolvePending(symbolId, revTradeSide);
        await supabase.from('signal_log').update({
          status: 'ERROR', error_message: 'R_REV entry failed after close: ' + entryErr.message,
        }).eq('id', revEntryDbId);
        await logAlert('R_REV_ENTRY_FAILED', 'CRITICAL',
          'R_REV entry FAILED after successful close | ticker: ' + ctSymbol
          + ' | error: ' + entryErr.message + ' | Position now FLAT — manual check required.');
      }
      setTimeout(function() { reconcileConfirm(revEntryDbId, symbolId, isLong); }, 2000);
      setTimeout(function() { queryRecentDeals(ctSymbol, revEntryDbId, symbolId, signal); }, 4000);
    } else if (isExit) {
      // v2.41.0: fire CRITICAL alert on backstop hit actions (fixed stop, not trailing)
      if (action === 'LONG_STOP' || action === 'SHORT_STOP') {
        await logAlert('BACKSTOP_HIT', 'CRITICAL',
          'Backstop stop loss hit | ticker: ' + (signal.ticker || 'UNKNOWN')
          + ' | action: ' + action
          + ' | entry_type: ' + (signal.entry_type || 'UNKNOWN')
          + ' | close: ' + (signal.close || 'UNKNOWN')
          + ' | atr: ' + (signal.atr || 'UNKNOWN')
          + ' | signal_id: ' + (signal.signal_id || 'UNKNOWN'));
      }
      var posRes2  = await connection.sendCommand('ProtoOAReconcileReq', { ctidTraderAccountId: ACCOUNT_ID });
      var position = (posRes2.position || []).find(function(p) {
        return String(p.tradeData && p.tradeData.symbolId) === String(symbolId)
          && p.tradeData && p.tradeData.tradeSide === (isLong ? 'BUY' : 'SELL');
      });
      if (!position) {
        console.log('No matching position for', action, ctSymbol);
        await logSignal(signal, null, 'NO_POSITION', null, latencyMs); return;
      }
      var positionId = position.positionId ? String(position.positionId) : null;
      var dbId2 = await logSignal(signal, { positionId }, 'PENDING_CLOSE', null, latencyMs);
      try {
        await connection.sendCommand('ProtoOAClosePositionReq', {
          ctidTraderAccountId: ACCOUNT_ID, positionId: position.positionId,
          volume: position.tradeData.volume,
        });
        console.log('Close sent to cTrader');
      } catch (e) {
        console.error('Close send error:', e.message);
        await supabase.from('signal_log').update({ status: 'ERROR', error_message: e.message }).eq('id', dbId2); return;
      }
      setTimeout(function() { reconcileExitConfirm(dbId2, positionId, symbolId, isLong); }, 1500);
    } else { console.warn('[UNKNOWN ACTION]', action, '— skipped'); }
  } catch (err) {
    console.error('Execution error:', err.message);
    await logSignal(signal, null, 'ERROR', err.message, latencyMs);
  }
}

// HTTP SERVER
function startHttpServer() {
  var app = express();
  app.use(express.json());

  // ─── CHANGE v2.38.1 #2 of 3 ─────────────────────────────────────────────────
  // CORS middleware — permits GET requests from the dashboard only.
  // Applies to /health only (POST /signal uses a different verb and is unaffected).
  // No security surface area changed — /health was already public and unauthenticated.
  app.use(cors({
    origin:  'https://hawk-dashboard.pages.dev',
    methods: ['GET'],
  }));
  // ─────────────────────────────────────────────────────────────────────────────

  app.post('/signal', async function(req, res) {
    if (req.headers['x-internal-secret'] !== INTERNAL_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    var signal = req.body;
    if (!signal || !signal.signal_id) return res.status(400).json({ error: 'Invalid signal' });
    res.status(200).json({ ok: true });
    setImmediate(function() { executeSignal(signal); });
  });

  app.get('/health', function(req, res) {
    var watchdogAge = lastWatchdogOk
      ? Math.round((Date.now() - new Date(lastWatchdogOk).getTime()) / 1000) : null;
    // ─── CHANGE v2.38.1 #3 of 3 — version bump in /health response ──────────────
    res.json({
      status:          isConnected ? 'CONNECTED' : 'DISCONNECTED',
      mode:            IS_PAPER ? 'PAPER' : 'LIVE',
      uptime:          process.uptime(),
      version:         '2.41.0',
      pendingOrders:   Object.keys(pendingOrders).length,
      lastWatchdogOk,
      watchdogAgeS:    watchdogAge,
      watchdogFailures,
    });
  });

  app.listen(PORT, function() { console.log('HTTP server listening on port', PORT); });
}

// MAIN
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
  startRedisPoller();
  setInterval(async function() {
    var daysLeft = tokenExpiryTime ? Math.floor((tokenExpiryTime - Date.now()) / 86400000) : null;
    await logHealth(isConnected ? 'RUNNING' : 'DISCONNECTED', daysLeft);
    if (daysLeft !== null) {
      // v2.40.0: graduated token expiry alerts — consolidated TOKEN_EXPIRY_WARNING into TOKEN_EXPIRY_ADVISORY
      if (daysLeft <= 7) {
        var today = new Date().toDateString();
        if (lastAdvisoryDay !== today) {
          lastAdvisoryDay = today;
          await logAlert('TOKEN_EXPIRY_ADVISORY', 'WARN',
            'cTrader access token expires in ' + daysLeft + ' day(s). Refresh required — see Railway token procedure.');
        }
      }
      if (daysLeft <= 1) {
        var now = Date.now();
        if (!lastCriticalSentAt || (now - lastCriticalSentAt) > 3600000) {
          lastCriticalSentAt = now;
          await logAlert('TOKEN_EXPIRY_CRITICAL', 'CRITICAL',
            'URGENT: cTrader token expires in ' + daysLeft + ' day(s). Engine will stop executing on expiry. Refresh immediately via Railway token procedure.');
        }
      }
    }
    await syncActivePositions();
  }, 60000);
}

main().catch(function(err) { console.error('Fatal startup error:', err.message); process.exit(1); });
