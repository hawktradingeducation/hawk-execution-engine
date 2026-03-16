// Hawk Execution Engine v1.0
// Dequeues signals from Upstash, executes on Pepperstone cTrader, logs to Supabase

const { createClient } = require('@supabase/supabase-js');

// ── CONFIG FROM ENVIRONMENT VARIABLES ───────────────────────────────────────
const UPSTASH_URL   = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const CTRADER_CLIENT_ID     = process.env.CTRADER_CLIENT_ID;
const CTRADER_CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;
const CTRADER_ACCOUNT_ID    = process.env.CTRADER_ACCOUNT_ID;  // numeric cTrader account ID
const CTRADER_REFRESH_TOKEN = process.env.CTRADER_REFRESH_TOKEN;
const IS_PAPER = process.env.IS_PAPER === 'true';  // 'true' for paper, 'false' for live

const CTRADER_API = IS_PAPER
  ? 'https://demo.ctraderapi.com'  // Paper/demo endpoint
  : 'https://live.ctraderapi.com'; // Live endpoint — only switch when ready

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── TOKEN MANAGEMENT ─────────────────────────────────────────────────────────
let accessToken = null;
let tokenExpiry = 0;

async function getAccessToken() {
  if (accessToken && Date.now() < tokenExpiry - 30000) return accessToken;
  const res = await fetch('https://connect.spotware.com/apps/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: CTRADER_REFRESH_TOKEN,
      client_id: CTRADER_CLIENT_ID,
      client_secret: CTRADER_CLIENT_SECRET
    }).toString()
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Token refresh failed: ' + JSON.stringify(data));
  accessToken = data.access_token;
  tokenExpiry = Date.now() + (data.expires_in * 1000);
  console.log('Access token refreshed, expires in', data.expires_in, 's');
  return accessToken;
}

// ── SYMBOL MAP: TradingView symbol -> cTrader symbol name ─────────────────────
const SYMBOL_MAP = {
  'XAUUSD': 'XAUUSD',
  'BTCUSD': 'BTCUSD',
  'NAS100': 'NAS100',
  'USOIL':  'USOIL.cmd',  // Verify exact symbol name in your cTrader account
};

// ── LOT SIZING: score -> volume in cTrader units (1 unit = 0.01 lots for XAUUSD) ──
function getLotSize(score, ticker) {
  // Adjust units per instrument as appropriate
  const base = ticker === 'BTCUSD' ? 1 : 100; // 1 unit BTC, 1 lot (100 units) gold
  if (score >= 8) return base * 2;
  if (score >= 7) return base * 1.5;
  return base * 1;
}

// ── DEDUPLICATION CACHE ───────────────────────────────────────────────────────
const seenSignals = new Map();
const DEDUP_TTL_MS = 10000; // 10 seconds — signals older than this are safe to ignore

function isDuplicate(signalId) {
  const now = Date.now();
  // Clean old entries
  for (const [id, ts] of seenSignals) {
    if (now - ts > DEDUP_TTL_MS) seenSignals.delete(id);
  }
  if (seenSignals.has(signalId)) return true;
  seenSignals.set(signalId, now);
  return false;
}

// ── CTRADER API: GET SYMBOL ID ────────────────────────────────────────────────
async function getSymbolId(symbolName) {
  const token = await getAccessToken();
  const res = await fetch(`${CTRADER_API}/v2/webserv/traders/${CTRADER_ACCOUNT_ID}/symbols`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const data = await res.json();
  const symbol = data.symbol?.find(s => s.symbolName === symbolName);
  if (!symbol) throw new Error(`Symbol not found: ${symbolName}`);
  return symbol.symbolId;
}

// ── CTRADER API: PLACE ORDER ──────────────────────────────────────────────────
async function placeOrder(signal) {
  const token = await getAccessToken();
  const ctSymbol = SYMBOL_MAP[signal.ticker];
  if (!ctSymbol) throw new Error(`No symbol mapping for: ${signal.ticker}`);

  const symbolId = await getSymbolId(ctSymbol);
  const isLong = signal.action === 'LONG';
  const volume = getLotSize(signal.score, signal.ticker);
  const atr = parseFloat(signal.atr);
  const stopLossPips = Math.round(atr * 2 * 100); // 2x ATR converted to pips

  const orderBody = {
    symbolId,
    volume,
    orderType: 'MARKET',
    tradeSide: isLong ? 'BUY' : 'SELL',
    stopLoss: stopLossPips,  // relative pips from entry
    comment: `HAWK|${signal.strategy_id}|${signal.signal_id}|S${signal.score}`
  };

  const res = await fetch(
    `${CTRADER_API}/v2/webserv/traders/${CTRADER_ACCOUNT_ID}/orders`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(orderBody)
    }
  );

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Order placement failed: ${err}`);
  }
  return await res.json();
}

// ── CTRADER API: CLOSE POSITION ───────────────────────────────────────────────
async function closePosition(signal) {
  const token = await getAccessToken();
  // Get open positions for this account and symbol
  const res = await fetch(
    `${CTRADER_API}/v2/webserv/traders/${CTRADER_ACCOUNT_ID}/positions`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const data = await res.json();
  const ctSymbol = SYMBOL_MAP[signal.ticker];
  const isLongExit = signal.action === 'LONG_EXIT' || signal.action === 'LONG_STOP';

  const position = data.position?.find(p =>
    p.symbolName === ctSymbol &&
    (isLongExit ? p.tradeSide === 'BUY' : p.tradeSide === 'SELL')
  );

  if (!position) {
    console.warn('No matching position to close for', signal.strategy_id, signal.action);
    return { closed: false, reason: 'No matching position' };
  }

  const closeRes = await fetch(
    `${CTRADER_API}/v2/webserv/traders/${CTRADER_ACCOUNT_ID}/positions/${position.positionId}/close`,
    {
      method: 'DELETE',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ volume: position.volume })
    }
  );
  if (!closeRes.ok) throw new Error('Close failed: ' + await closeRes.text());
  return await closeRes.json();
}

// ── SUPABASE LOGGING ──────────────────────────────────────────────────────────
async function logToSupabase(signal, result, status, errorMsg = null) {
  await supabase.from('signal_log').insert({
    signal_id:    signal.signal_id,
    strategy_id:  signal.strategy_id,
    ticker:       signal.ticker,
    action:       signal.action,
    score:        signal.score,
    atr:          parseFloat(signal.atr),
    close_price:  parseFloat(signal.close),
    status,
    error_message: errorMsg,
    api_response:  result ? JSON.stringify(result) : null,
    signal_time:   new Date(parseInt(signal.timestamp)).toISOString(),
    processed_at:  new Date().toISOString(),
    is_paper:      IS_PAPER
  });
}

// ── SIGNAL PROCESSOR ─────────────────────────────────────────────────────────
async function processSignal(signal) {
  console.log(`Processing: ${signal.strategy_id} | ${signal.action} | ID: ${signal.signal_id}`);

  if (isDuplicate(signal.signal_id)) {
    console.warn('Duplicate signal ignored:', signal.signal_id);
    await logToSupabase(signal, null, 'DUPLICATE');
    return;
  }

  try {
    let result;
    if (signal.action === 'LONG' || signal.action === 'SHORT') {
      result = await placeOrder(signal);
      await logToSupabase(signal, result, 'EXECUTED');
    } else {
      result = await closePosition(signal);
      const status = result.closed === false ? 'NO_POSITION' : 'CLOSED';
      await logToSupabase(signal, result, status);
    }
    console.log(`Success: ${signal.action} on ${signal.ticker}`);
  } catch (err) {
    console.error('Execution error:', err.message);
    await logToSupabase(signal, null, 'ERROR', err.message);
  }
}

// ── MAIN QUEUE LOOP ───────────────────────────────────────────────────────────
async function pollQueue() {
  console.log(`Hawk Execution Engine started | Paper: ${IS_PAPER} | ${new Date().toISOString()}`);

  while (true) {
    try {
      // BRPOP with 5-second timeout — blocks until signal available
      const res = await fetch(
        `${UPSTASH_URL}/brpop/hawk:signals/5`,
        { headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` } }
      );
      const data = await res.json();

      // null result means timeout (no signal) — loop back
      if (!data.result) continue;

      // Upstash BRPOP returns [key, value]
      const rawSignal = Array.isArray(data.result) ? data.result[1] : data.result;
      const signal = typeof rawSignal === 'string' ? JSON.parse(rawSignal) : rawSignal;

      await processSignal(signal);

    } catch (err) {
      console.error('Queue poll error:', err.message);
      await new Promise(r => setTimeout(r, 2000)); // 2s backoff on error
    }
  }
}

pollQueue();
