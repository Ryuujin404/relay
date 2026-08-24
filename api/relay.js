export const config = {
  runtime: 'nodejs',
  maxDuration: 300,
  api: { bodyParser: false },
};

// ── Timeout Config ──
const UPSTREAM_TIMEOUT     = 290_000; // 4m 50s hard limit
const CONNECT_TIMEOUT      = 15_000;  // Buka koneksi
const FIRST_BYTE_TIMEOUT   = 180_000; // ⬅️ 3 menit buat model reasoning/thinking
const INTER_BYTE_TIMEOUT   = 45_000;  // ⬅️ 45 detik antar-token setelah streaming jalan
const MAX_RETRIES          = 3;
const MAX_BACKOFF_MS       = 8_000;

function uid() {
  return Math.random().toString(36).slice(2, 10);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

export default async function handler(req, res) {
  const reqId = req.headers['x-request-id'] || uid();
  const start = Date.now();

  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-relay-target, x-relay-path, x-request-id');
    return res.status(204).end();
  }

  const target = req.headers['x-relay-target'];
  const relayPath = req.headers['x-relay-path'] || '/';
  if (!target) {
    return res.status(400).json({ error: 'Missing x-relay-target header', reqId });
  }

  const targetUrl = target.replace(/\/$/, '') + relayPath;
  console.log(`[${reqId}] ▶ START ${req.method} ${targetUrl}`);

  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (!['x-relay-target','x-relay-path','host','content-length','connection'].includes(kl)) {
      headers[k] = v;
    }
  }
  headers['x-request-id'] = reqId;

  let body;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const raw = Buffer.concat(chunks).toString();
    if (raw) {
      try { JSON.parse(raw); body = raw; }
      catch { return res.status(400).json({ error: 'Invalid JSON body', reqId }); }
    }
  }

  let lastErr;
  let lastStatus;

  for (let attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
    const ctrl = new AbortController();
    let totalTimer, connectTimer, stallTimer;
    let isConnected = false;
    let firstByteReceived = false;

    const cleanup = () => {
      clearTimeout(totalTimer);
      clearTimeout(connectTimer);
      clearTimeout(stallTimer);
    };

    const resetStallTimer = () => {
      clearTimeout(stallTimer);
      // ⬇️ REASONING-AWARE: sebelum first byte = 3 menit, sesudah = 45 detik
      const timeout = firstByteReceived ? INTER_BYTE_TIMEOUT : FIRST_BYTE_TIMEOUT;
      stallTimer = setTimeout(() => {
        const phase = firstByteReceived ? 'inter-byte (45s)' : 'first-byte (180s)';
        console.log(`[${reqId}] ⏱ STALL_TIMEOUT [${phase}] attempt ${attempt}`);
        ctrl.abort();
      }, timeout);
    };

    try {
      totalTimer = setTimeout(() => {
        console.log(`[${reqId}] ⏱ TOTAL_TIMEOUT (290s) attempt ${attempt}`);
        ctrl.abort();
      }, UPSTREAM_TIMEOUT);

      connectTimer = setTimeout(() => {
        if (!isConnected) {
          console.log(`[${reqId}] ⏱ CONNECT_TIMEOUT (15s) attempt ${attempt}`);
          ctrl.abort();
        }
      }, CONNECT_TIMEOUT);

      console.log(`[${reqId}] 🔄 ATTEMPT ${attempt}/${MAX_RETRIES + 1} → ${targetUrl}`);

      const response = await fetch(targetUrl, {
        method: req.method,
        headers,
        body,
        signal: ctrl.signal,
      });

      isConnected = true;
      clearTimeout(connectTimer);

      const cl = response.headers.get('content-length');
      if (cl && parseInt(cl) > 50 * 1024 * 1024) {
        cleanup();
        return res.status(413).json({ error: 'Response too large (>50MB)', reqId });
      }

      lastStatus = response.status;

      if (response.status >= 502 && response.status <= 504) {
        cleanup();
        if (attempt <= MAX_RETRIES) {
          const backoff = Math.min(1000 * Math.pow(2, attempt - 1), MAX_BACKOFF_MS);
          console.log(`[${reqId}] ⚠ UPSTREAM_${response.status} → backoff ${backoff}ms`);
          await sleep(backoff);
          continue;
        }
      }

      if (response.status >= 400 && response.status < 500 && response.status !== 429) {
        cleanup();
        res.status(response.status);
        response.headers.forEach((v, k) => { try { res.setHeader(k, v); } catch {} });
        const txt = await response.text();
        return res.send(txt);
      }

      res.status(response.status);
      response.headers.forEach((v, k) => {
        if (k.toLowerCase() === 'content-length') return;
        try { res.setHeader(k, v); } catch {}
      });
      res.setHeader('Cache-Control', 'no-cache, no-transform');
      res.setHeader('X-Accel-Buffering', 'no');

      if (response.body) {
        const { pipeline } = require('node:stream');
        const { Readable } = require('node:stream');
        const upstream = Readable.fromWeb(response.body);

        // ⬇️ REASONING-AWARE: start stall timer (180s untuk first byte)
        resetStallTimer();

        upstream.on('data', (chunk) => {
          if (!firstByteReceived) {
            firstByteReceived = true;
            const ttfb = Date.now() - start;
            console.log(`[${reqId}] 📥 FIRST_BYTE after ${ttfb}ms (reasoning-safe)`);
          }
          resetStallTimer(); // reset ke INTER_BYTE_TIMEOUT (45s)
        });

        await new Promise((resolve, reject) => {
          pipeline(upstream, res, (err) => {
            cleanup();
            if (err && (err.message === 'STALL_TIMEOUT' || err.name === 'AbortError')) {
              reject(err);
            } else if (err) {
              console.log(`[${reqId}] Pipeline error: ${err.message}`);
              reject(err);
            } else {
              resolve();
            }
          });
        });

        console.log(`[${reqId}] ✅ DONE ${response.status} in ${Date.now() - start}ms`);
        return;

      } else {
        cleanup();
        res.end();
        return;
      }

    } catch (err) {
      cleanup();
      lastErr = err;

      const isRetryable = (
        err.name === 'AbortError' ||
        ['ECONNRESET','ETIMEDOUT','ECONNREFUSED','EPIPE','ENOTFOUND'].includes(err.code) ||
        (err.message && err.message.includes('fetch failed'))
      );

      if (isRetryable && attempt <= MAX_RETRIES) {
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
    res.status(code).json({
      error: code === 504 ? 'Gateway Timeout' : 'Bad Gateway',
      message: lastErr?.message || 'Upstream failed after max retries',
      attempts: MAX_RETRIES + 1,
      reqId,
    });
  }
}
