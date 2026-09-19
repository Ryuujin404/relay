import { pipeline, Readable } from 'node:stream';

export const config = {
  runtime: 'nodejs',
  maxDuration: 300,
  api: { bodyParser: false },
};

const UPSTREAM_TIMEOUT   = 290_000;
const CONNECT_TIMEOUT    = 15_000;
const INTER_BYTE_TIMEOUT = 45_000;
const FIRST_DATA_WAIT_MS = 15_000;  // turun dari 60s → 15s biar tidak stuck lama
const MAX_RETRIES        = 3;
const MAX_BACKOFF_MS     = 8_000;
const CIRCUIT_THRESHOLD  = 3;
const CIRCUIT_COOLDOWN   = 60_000;
const MAX_RESPONSE_SIZE  = 50 * 1024 * 1024;

// Strip seminimal mungkin — hanya yang WAJIB di-strip untuk HTTP correctness
// Jangan strip origin/referer/user-agent karena OpenCode mungkin pakai itu untuk validasi
const STRIP_HEADERS = new Set([
  'x-relay-target',
  'x-relay-path',
  'host',           // wajib: host diganti sesuai upstream
  'content-length', // wajib: akan di-set ulang oleh fetch
  'connection',     // wajib: hop-by-hop header
  'transfer-encoding', // wajib: hop-by-hop header
]);

const circuitStates = new Map();

function uid() { return Math.random().toString(36).slice(2, 10); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function getCircuit(hostname) {
  const now = Date.now();
  const s = circuitStates.get(hostname);
  if (!s) return { open: false };
  if (s.failures >= CIRCUIT_THRESHOLD && (now - s.lastFail) < CIRCUIT_COOLDOWN)
    return { open: true, until: s.lastFail + CIRCUIT_COOLDOWN };
  if ((now - s.lastFail) >= CIRCUIT_COOLDOWN) circuitStates.delete(hostname);
  return { open: false };
}

function recordFail(hostname) {
  const e = circuitStates.get(hostname) || { failures: 0, lastFail: 0 };
  e.failures = Math.min(e.failures + 1, CIRCUIT_THRESHOLD);
  e.lastFail = Date.now();
  circuitStates.set(hostname, e);
}

function recordSuccess(hostname) { circuitStates.delete(hostname); }

function applyCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-relay-target, x-relay-path, x-request-id');
}

function waitFirstChunk(upstream) {
  return new Promise((resolve, reject) => {
    let settled = false;

    const done = (fn, val) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      upstream.removeListener('data', onData);
      upstream.removeListener('end', onEnd);
      upstream.removeListener('error', onError);
      fn(val);
    };

    const timer = setTimeout(
      () => done(reject, Object.assign(new Error('FIRST_DATA_TIMEOUT'), { code: 'FIRST_DATA_TIMEOUT' })),
      FIRST_DATA_WAIT_MS
    );

    const onData  = (chunk) => { upstream.pause(); done(resolve, chunk); };
    const onEnd   = ()      => done(reject, Object.assign(new Error('STREAM_EMPTY'), { code: 'STREAM_EMPTY' }));
    const onError = (err)   => done(reject, err);

    upstream.once('data', onData);
    upstream.once('end', onEnd);
    upstream.once('error', onError);
  });
}

// Coba parse model name dari body JSON (untuk logging/debug)
function tryGetModel(bodyStr) {
  try { return JSON.parse(bodyStr)?.model || 'unknown'; }
  catch { return 'unknown'; }
}

export default async function handler(req, res) {
  const reqId = req.headers['x-request-id'] || uid();
  const start = Date.now();

  applyCors(res);

  if (req.method === 'OPTIONS') return res.status(204).end();

  if (req.url?.split('?')[0] === '/health') {
    return res.status(200).json({
      status: 'ok', uptime: process.uptime(),
      circuits: Object.fromEntries(circuitStates),
      version: '2.7',
    });
  }

  const target    = req.headers['x-relay-target'];
  const relayPath = req.headers['x-relay-path'] || '/';
  if (!target) return res.status(400).json({ error: 'Missing x-relay-target', reqId });

  let targetUrl, hostname;
  try {
    targetUrl = target.replace(/\/$/, '') + relayPath;
    hostname  = new URL(targetUrl).hostname;
  } catch {
    return res.status(400).json({ error: 'Invalid x-relay-target URL', reqId });
  }

  const initCirc = getCircuit(hostname);
  if (initCirc.open) {
    return res.status(503).json({
      error: 'Service Unavailable',
      message: `${hostname} circuit open`,
      retryAfter: Math.ceil((initCirc.until - Date.now()) / 1000), reqId,
    });
  }

  // Build headers — strip minimal, pertahankan sisanya termasuk origin/referer/user-agent
  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (!STRIP_HEADERS.has(k.toLowerCase())) headers[k] = v;
  }
  headers['x-request-id'] = reqId;
  // Set host sesuai upstream
  headers['host'] = hostname;

  let body, bodyStr;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    bodyStr = Buffer.concat(chunks).toString();
    if (bodyStr) {
      try { JSON.parse(bodyStr); body = bodyStr; }
      catch { return res.status(400).json({ error: 'Invalid JSON body', reqId }); }
    }
  }

  const model = tryGetModel(bodyStr);
  const isResponsesEndpoint = relayPath.includes('/responses');

  let clientGone = false;
  req.on('close', () => { clientGone = true; });
  req.on('error', () => { clientGone = true; });

  let lastErr, lastStatus;
  let streamingStarted = false;
  let totalAttempts = 0;

  // Kalau endpoint /responses dapat 403 FreeTierError, coba fallback ke /chat/completions
  let currentUrl = targetUrl;
  let triedFallback = false;

  for (let attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
    totalAttempts = attempt;

    if (clientGone) {
      console.log(`[${reqId}] ⚡ CLIENT_GONE before attempt ${attempt}`);
      return;
    }

    if (attempt > 1) {
      const circCheck = getCircuit(hostname);
      if (circCheck.open) {
        console.log(`[${reqId}] 🔴 CIRCUIT_OPEN on attempt ${attempt}, stop retrying`);
        lastErr = new Error(`Circuit open for ${hostname}`);
        break;
      }
    }

    const ctrl = new AbortController();
    let totalTimer, connectTimer, stallTimer;
    let isConnected = false;

    const cleanup = () => {
      clearTimeout(totalTimer);
      clearTimeout(connectTimer);
      clearTimeout(stallTimer);
    };

    const resetStall = () => {
      clearTimeout(stallTimer);
      stallTimer = setTimeout(() => {
        console.log(`[${reqId}] ⏱ INTER_BYTE_STALL attempt ${attempt}`);
        ctrl.abort();
      }, INTER_BYTE_TIMEOUT);
    };

    try {
      totalTimer   = setTimeout(() => ctrl.abort(), UPSTREAM_TIMEOUT);
      connectTimer = setTimeout(() => { if (!isConnected) ctrl.abort(); }, CONNECT_TIMEOUT);

      console.log(`[${reqId}] 🔄 ATTEMPT ${attempt}/${MAX_RETRIES + 1} → ${currentUrl} [model:${model}]`);

      const response = await fetch(currentUrl, {
        method: req.method, headers, body, signal: ctrl.signal,
      });

      isConnected = true;
      clearTimeout(connectTimer);

      const cl = response.headers.get('content-length');
      if (cl && parseInt(cl) > MAX_RESPONSE_SIZE) {
        cleanup();
        return res.status(413).json({ error: 'Response too large', reqId });
      }

      lastStatus = response.status;

      // ── 403 FreeTierError: coba fallback /responses → /chat/completions ──
      if (response.status === 403 && isResponsesEndpoint && !triedFallback) {
        let errBody = '';
        try { errBody = await response.text(); } catch {}

        if (errBody.includes('FreeTierError') || errBody.includes('free tier')) {
          triedFallback = true;
          cleanup();

          // Ganti URL: /responses → /chat/completions
          const baseTarget = target.replace(/\/$/, '');
          currentUrl = baseTarget + '/chat/completions';
          console.log(`[${reqId}] ⚠ 403 FreeTierError on /responses → fallback to /chat/completions [model:${model}]`);

          // Lanjut ke attempt berikutnya tanpa increment retry counter
          attempt--; // jangan hitung ini sebagai retry
          continue;
        }
      }

      // ── 429: forward rate limit ──
      if (response.status === 429) {
        cleanup();
        const ra = response.headers.get('retry-after') || '60';
        res.setHeader('Retry-After', ra);
        res.setHeader('x-proxy-req-id', reqId);
        res.status(429);
        response.headers.forEach((v, k) => { try { res.setHeader(k, v); } catch {} });
        return res.send(await response.text());
      }

      // ── 502-504: retry with backoff ──
      if (response.status >= 502 && response.status <= 504) {
        recordFail(hostname);
        cleanup();
        if (attempt <= MAX_RETRIES) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
          console.log(`[${reqId}] ⚠ UPSTREAM_${response.status} → backoff ${backoff}ms`);
          await sleep(backoff);
          continue;
        }
        lastErr = new Error(`Upstream returned ${response.status}`);
        break;
      }

      if (response.status >= 200 && response.status < 300) recordSuccess(hostname);

      // ── 4xx client error: forward as-is ──
      if (response.status >= 400 && response.status < 500) {
        cleanup();
        res.setHeader('x-proxy-req-id', reqId);
        res.status(response.status);
        response.headers.forEach((v, k) => { try { res.setHeader(k, v); } catch {} });
        return res.send(await response.text());
      }

      // ── 2xx: TWO-PHASE RESPONSE ──
      if (response.body) {
        const upstream = Readable.fromWeb(response.body);

        // PHASE 1: tunggu first chunk sebelum commit headers
        let firstChunk;
        try {
          firstChunk = await waitFirstChunk(upstream);
        } catch (phaseErr) {
          upstream.destroy();
          cleanup();
          recordFail(hostname);

          const reason = phaseErr.code || phaseErr.message;
          console.log(`[${reqId}] ⚠ ${reason} attempt ${attempt} — 0 body bytes, retrying`);

          if (attempt <= MAX_RETRIES && !clientGone) {
            const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
            console.log(`[${reqId}] 🔁 RETRY ${attempt + 1} in ${backoff}ms`);
            await sleep(backoff);
            continue;
          }

          lastErr = phaseErr;
          break;
        }

        // PHASE 2: first chunk tiba → commit headers ke client
        streamingStarted = true;
        console.log(`[${reqId}] 📥 FIRST_BYTE after ${Date.now() - start}ms [model:${model}] [url:${currentUrl}]`);

        res.status(response.status);
        response.headers.forEach((v, k) => {
          if (k.toLowerCase() === 'content-length') return;
          try { res.setHeader(k, v); } catch {}
        });
        res.setHeader('Cache-Control', 'no-cache, no-transform');
        res.setHeader('X-Accel-Buffering', 'no');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('x-proxy-req-id', reqId);

        upstream.unshift(firstChunk);
        resetStall();
        upstream.on('data', () => resetStall());

        await new Promise((resolve, reject) => {
          pipeline(upstream, res, (err) => {
            cleanup();
            if (!err) return resolve();
            if (err.code === 'ERR_STREAM_PREMATURE_CLOSE' || clientGone) {
              console.log(`[${reqId}] ⚡ CLIENT_CLOSED_STREAM`);
              return resolve();
            }
            if (err.name === 'AbortError' || err.message?.includes('abort')) return reject(err);
            console.log(`[${reqId}] ⚠ Pipeline error: ${err.message}`);
            reject(err);
          });
        });

        console.log(`[${reqId}] ✅ DONE ${response.status} in ${Date.now() - start}ms [model:${model}]`);
        return;
      }

      // No body (HEAD, dll)
      cleanup();
      res.end();
      return;

    } catch (err) {
      cleanup();
      lastErr = err;

      if (streamingStarted) {
        console.log(`[${reqId}] ❌ ERROR mid-stream (${Date.now() - start}ms): ${err.message}`);
        if (!res.writableEnded) res.end();
        return;
      }

      recordFail(hostname);

      const retryable =
        err.name === 'AbortError' ||
        ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'EPIPE', 'ENOTFOUND'].includes(err.code) ||
        err.message?.includes('fetch failed');

      if (retryable && attempt <= MAX_RETRIES) {
        const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
        const reason = err.name === 'AbortError' ? 'timeout' : (err.code || err.message);
        console.log(`[${reqId}] ⚠ ${reason} → backoff ${backoff}ms → retry ${attempt + 1}`);
        await sleep(backoff);
        continue;
      }
      break;
    }
  }

  console.log(`[${reqId}] ❌ FAILED after ${totalAttempts} attempt(s) in ${Date.now() - start}ms`);
  if (!res.headersSent) {
    const code = lastStatus || (lastErr?.name === 'AbortError' ? 504 : 502);
    res.setHeader('x-proxy-req-id', reqId);
    res.status(code).json({
      error: code === 504 ? 'Gateway Timeout' : 'Bad Gateway',
      message: lastErr?.message || 'Upstream failed',
      attempts: totalAttempts, reqId,
    });
  }
}
