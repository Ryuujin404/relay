// ============================================
// 9router Vercel Proxy — Production Grade
// Timeout: 300s | Retry: 2x | Stream: ON
// ============================================

export const config = {
  runtime: 'nodejs',
  maxDuration: 300,
  api: { bodyParser: false },
};

const UPSTREAM_TIMEOUT = 290_000;
const MAX_RETRIES = 2;

function generateId() {
  return crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2, 10);
}

export default async function handler(req, res) {
  const reqId = req.headers['x-request-id'] || generateId();
  const start = Date.now();

  // ── 1. CORS Preflight ────────────────────
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-relay-target, x-relay-path, x-request-id');
    res.setHeader('Access-Control-Max-Age', '86400');
    return res.status(204).end();
  }

  // ── 2. Parse Target ──────────────────────
  const target = req.headers['x-relay-target'];
  const relayPath = req.headers['x-relay-path'] || '/';
  
  if (!target) {
    return res.status(400).json({ error: 'Missing x-relay-target header', reqId });
  }

  const targetUrl = target.replace(/\/$/, '') + relayPath;
  console.log(`[${reqId}] ▶ ${req.method} ${targetUrl}`);

  // ── 3. Headers ───────────────────────────
  const headers = {};
  for (const [key, value] of Object.entries(req.headers)) {
    const k = key.toLowerCase();
    if (!['x-relay-target','x-relay-path','host','content-length','connection'].includes(k)) {
      headers[key] = value;
    }
  }
  headers['x-request-id'] = reqId;
  headers['connection'] = 'keep-alive';

  // ── 4. Body ──────────────────────────────
  let body = undefined;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    try {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const raw = Buffer.concat(chunks).toString('utf8');
      if (raw) { JSON.parse(raw); body = raw; }
    } catch (err) {
      console.log(`[${reqId}] ❌ Bad body: ${err.message}`);
      return res.status(400).json({ error: 'Invalid JSON body', reqId });
    }
  }

  // ── 5. Retry Loop ────────────────────────
  let lastError;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), UPSTREAM_TIMEOUT);

    try {
      const response = await fetch(targetUrl, {
        method: req.method,
        headers,
        body,
        signal: ctrl.signal,
      });

      clearTimeout(timer);

      // Kalau upstream 502/503/504, retry dulu
      if (response.status >= 502 && response.status <= 504 && attempt < MAX_RETRIES) {
        console.log(`[${reqId}] ⚠ Upstream ${response.status}, retry ${attempt}/${MAX_RETRIES}`);
        await new Promise(r => setTimeout(r, 1000)); // backoff 1 detik
        continue;
      }

      // Success atau client error (4xx) — langsung return
      res.status(response.status);
      response.headers.forEach((value, key) => {
        if (!['content-encoding','transfer-encoding'].includes(key.toLowerCase())) {
          try { res.setHeader(key, value); } catch {}
        }
      });
      res.setHeader('x-proxy-req-id', reqId);

      if (response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          res.write(Buffer.from(value));
        }
      }
      res.end();
      
      console.log(`[${reqId}] ✅ ${response.status} in ${Date.now()-start}ms (attempt ${attempt})`);
      return; // SELESAI

    } catch (err) {
      clearTimeout(timer);
      lastError = err;
      
      const isRetryable = err.name === 'AbortError' || 
                          err.code === 'ECONNRESET' || 
                          err.code === 'ETIMEDOUT' ||
                          err.code === 'ECONNREFUSED';
      
      if (isRetryable && attempt < MAX_RETRIES) {
        console.log(`[${reqId}] ⚠ ${err.name || err.code}, retry ${attempt}/${MAX_RETRIES}`);
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }
      break;
    }
  }

  // ── 6. Semua Retry Gagal ─────────────────
  console.log(`[${reqId}] ❌ Failed after ${MAX_RETRIES} attempts: ${lastError?.message}`);
  
  if (!res.headersSent) {
    const status = lastError?.name === 'AbortError' ? 504 : 502;
    res.status(status).json({
      error: status === 504 ? 'Gateway Timeout' : 'Bad Gateway',
      message: lastError?.message || 'Upstream failed',
      reqId,
    });
  }
}
