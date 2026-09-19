export const config = {
  runtime: 'nodejs',
  maxDuration: 300,
  api: { bodyParser: false },
};

const UPSTREAM_TIMEOUT   = 290_000;
const CONNECT_TIMEOUT    = 15_000;
const FIRST_BYTE_TIMEOUT = 180_000;
const INTER_BYTE_TIMEOUT = 45_000;
const MAX_RETRIES        = 3;
const MAX_BACKOFF_MS     = 8_000;
const CIRCUIT_THRESHOLD  = 3;
const CIRCUIT_COOLDOWN   = 60_000;

const circuitStates = new Map();

function uid() {
  return Math.random().toString(36).slice(2, 10);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function getCircuit(hostname) {
  const now = Date.now();
  const s = circuitStates.get(hostname);
  if (!s) return { open: false };
  if (s.failures >= CIRCUIT_THRESHOLD && (now - s.lastFail) < CIRCUIT_COOLDOWN) {
    return { open: true, until: s.lastFail + CIRCUIT_COOLDOWN };
  }
  if ((now - s.lastFail) >= CIRCUIT_COOLDOWN) circuitStates.delete(hostname);
  return { open: false };
}

function recordFail(hostname) {
  const e = circuitStates.get(hostname) || { failures: 0, lastFail: 0 };
  e.failures = Math.min(e.failures + 1, CIRCUIT_THRESHOLD);
  e.lastFail = Date.now();
  circuitStates.set(hostname, e);
}

function recordSuccess(hostname) {
  circuitStates.delete(hostname);
}

export default async function handler(req, res) {
  const reqId = req.headers['x-request-id'] || uid();
  const start = Date.now();

  if (req.url === '/health') {
    return res.status(200).json({ status: 'ok', uptime: process.uptime(), circuits: Object.fromEntries(circuitStates), version: '2.4' });
  }

  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-relay-target, x-relay-path, x-request-id');
    return res.status(204).end();
  }

  const target = req.headers['x-relay-target'];
  const relayPath = req.headers['x-relay-path'] || '/';
  if (!target) return res.status(400).json({ error: 'Missing x-relay-target', reqId });

  const targetUrl = target.replace(/\/$/, '') + relayPath;
  const hostname = new URL(targetUrl).hostname;

  const circ = getCircuit(hostname);
  if (circ.open) {
    return res.status(503).json({ error: 'Service Unavailable', message: `${hostname} circuit open`, retryAfter: Math.ceil((circ.until - Date.now()) / 1000), reqId });
  }

  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (!['x-relay-target','x-relay-path','host','content-length','connection'].includes(kl)) headers[k] = v;
  }
  headers['x-request-id'] = reqId;

  let body;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const raw = Buffer.concat(chunks).toString();
    if (raw) { JSON.parse(raw); body = raw; }
  }

  let clientGone = false;
  req.on('close', () => { clientGone = true; });
  req.on('error', () => { clientGone = true; });

  let lastErr;
  let lastStatus;

  for (let attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
    if (clientGone) {
      console.log(`[${reqId}] ⚡ CLIENT_GONE before attempt ${attempt}`);
      return;
    }

    const ctrl = new AbortController();
    let totalTimer, connectTimer, stallTimer;
    let isConnected = false;
    let firstByteReceived = false;

    const cleanup = () => {
      clearTimeout(totalTimer);
      clearTimeout(connectTimer);
      clearTimeout(stallTimer);
    };

    const resetStall = () => {
      clearTimeout(stallTimer);
      const t = firstByteReceived ? INTER_BYTE_TIMEOUT : FIRST_BYTE_TIMEOUT;
      stallTimer = setTimeout(() => {
        const phase = firstByteReceived ? 'inter-byte' : 'first-byte';
        console.log(`[${reqId}] ⏱ STALL_TIMEOUT [${phase}] attempt ${attempt}`);
        ctrl.abort();
      }, t);
    };

    try {
      totalTimer = setTimeout(() => ctrl.abort(), UPSTREAM_TIMEOUT);
      connectTimer = setTimeout(() => { if (!isConnected) ctrl.abort(); }, CONNECT_TIMEOUT);

      console.log(`[${reqId}] 🔄 ATTEMPT ${attempt}/${MAX_RETRIES + 1} → ${targetUrl}`);

      const response = await fetch(targetUrl, { method: req.method, headers, body, signal: ctrl.signal });

      isConnected = true;
      clearTimeout(connectTimer);

      const cl = response.headers.get('content-length');
      if (cl && parseInt(cl) > 50 * 1024 * 1024) {
        cleanup();
        return res.status(413).json({ error: 'Response too large', reqId });
      }

      lastStatus = response.status;

      if (response.status === 429) {
        cleanup();
        const ra = response.headers.get('retry-after') || '60';
        res.setHeader('Retry-After', ra);
        res.status(429);
        response.headers.forEach((v, k) => { try { res.setHeader(k, v); } catch {} });
        return res.send(await response.text());
      }

      if (response.status >= 502 && response.status <= 504) {
        recordFail(hostname);
        cleanup();
        if (attempt <= MAX_RETRIES) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
          console.log(`[${reqId}] ⚠ UPSTREAM_${response.status} → backoff ${backoff}ms`);
          await sleep(backoff);
          continue;
        }
      }

      if (response.status >= 200 && response.status < 300) recordSuccess(hostname);

      if (response.status >= 400 && response.status < 500) {
        cleanup();
        res.status(response.status);
        response.headers.forEach((v, k) => { try { res.setHeader(k, v); } catch {} });
        return res.send(await response.text());
      }

      res.status(response.status);
      response.headers.forEach((v, k) => {
        if (k.toLowerCase() === 'content-length') return;
        try { res.setHeader(k, v); } catch {}
      });
      res.setHeader('Cache-Control', 'no-cache, no-transform');
      res.setHeader('X-Accel-Buffering', 'no');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('x-proxy-req-id', reqId);

      if (response.body) {
        const { pipeline } = require('node:stream');
        const { Readable } = require('node:stream');
        const upstream = Readable.fromWeb(response.body);

        resetStall();
        upstream.on('data', () => {
          if (!firstByteReceived) {
            firstByteReceived = true;
            console.log(`[${reqId}] 📥 FIRST_BYTE after ${Date.now() - start}ms`);
          }
          resetStall();
        });

        await new Promise((resolve, reject) => {
          pipeline(upstream, res, (err) => {
            cleanup();
            if (!err) return resolve();
            if (err.code === 'ERR_STREAM_PREMATURE_CLOSE') {
              console.log(`[${reqId}] ⚡ CLIENT_CLOSED_STREAM`);
              return resolve();
            }
            if (err.name === 'AbortError' || err.message?.includes('abort')) return reject(err);
            console.log(`[${reqId}] Pipeline error: ${err.message}`);
            reject(err);
          });
        });

        console.log(`[${reqId}] ✅ DONE ${response.status} in ${Date.now() - start}ms`);
        return;
      }

      cleanup();
      res.end();
      return;

    } catch (err) {
      cleanup();
      lastErr = err;
      recordFail(hostname);

      const retryable = err.name === 'AbortError' || ['ECONNRESET','ETIMEDOUT','ECONNREFUSED','EPIPE','ENOTFOUND'].includes(err.code) || (err.message && err.message.includes('fetch failed'));
      if (retryable && attempt <= MAX_RETRIES) {
        const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
        const reason = err.name === 'AbortError' ? 'timeout' : (err.code || 'error');
        console.log(`[${reqId}] ⚠ ${reason} → backoff ${backoff}ms → retry ${attempt + 1}`);
        await sleep(backoff);
        continue;
      }
      break;
    }
  }

  console.log(`[${reqId}] ❌ FAILED after ${MAX_RETRIES + 1} attempts`);
  if (!res.headersSent) {
    const code = lastStatus || (lastErr?.name === 'AbortError' ? 504 : 502);
    res.status(code).json({ error: code === 504 ? 'Gateway Timeout' : 'Bad Gateway', message: lastErr?.message || 'Upstream failed', attempts: MAX_RETRIES + 1, reqId });
  }
}
