export const config = {
  runtime: 'nodejs',
  maxDuration: 300,
  api: { bodyParser: false },
};

const UPSTREAM_TIMEOUT = 290_000;
const CONNECT_TIMEOUT = 15_000;   // ⬅️ Baru: 15 detik buat connect
const MAX_RETRIES = 2;
const MAX_BODY_MB = 50;

function uid() {
  return Math.random().toString(36).slice(2, 10);
}

export default async function handler(req, res) {
  const reqId = req.headers['x-request-id'] || uid();
  const start = Date.now();

  // CORS
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

  // Headers
  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (!['x-relay-target','x-relay-path','host','content-length','connection'].includes(kl)) headers[k] = v;
  }
  headers['x-request-id'] = reqId;

  // Body
  let body;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const raw = Buffer.concat(chunks).toString();
    if (raw) { JSON.parse(raw); body = raw; }
  }

  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const ctrl = new AbortController();
    const totalTimer = setTimeout(() => ctrl.abort(), UPSTREAM_TIMEOUT);

    // ⬅️ Baru: Connect timeout terpisah
    const connectTimer = setTimeout(() => {
      console.log(`[${reqId}] ⏱ Connect timeout (15s)`);
      ctrl.abort();
    }, CONNECT_TIMEOUT);

    try {
      const response = await fetch(targetUrl, {
        method: req.method,
        headers,
        body,
        signal: ctrl.signal,
      });

      clearTimeout(totalTimer);
      clearTimeout(connectTimer);

      // ⬅️ Baru: Response size guard
      const cl = response.headers.get('content-length');
      if (cl && parseInt(cl) > MAX_BODY_MB * 1024 * 1024) {
        return res.status(413).json({ error: 'Response too large', reqId });
      }

      // Retry kalau 502/503/504
      if (response.status >= 502 && response.status <= 504 && attempt < MAX_RETRIES) {
        console.log(`[${reqId}] ⚠ ${response.status}, retry ${attempt}/${MAX_RETRIES}`);
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }

      // Stream response
      res.status(response.status);
      res.setHeader('x-proxy-req-id', reqId);
      response.headers.forEach((v, k) => {
        if (!['content-encoding','transfer-encoding'].includes(k.toLowerCase())) {
          try { res.setHeader(k, v); } catch {}
        }
      });

      if (response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          res.write(Buffer.from(value));
        }
      }
      res.end();
      console.log(`[${reqId}] ✅ ${response.status} ${Date.now()-start}ms (try:${attempt})`);
      return;

    } catch (err) {
      clearTimeout(totalTimer);
      clearTimeout(connectTimer);
      lastErr = err;

      const retryable = err.name === 'AbortError' || ['ECONNRESET','ETIMEDOUT','ECONNREFUSED'].includes(err.code);
      if (retryable && attempt < MAX_RETRIES) {
        console.log(`[${reqId}] ⚠ ${err.name||err.code}, retry ${attempt}/${MAX_RETRIES}`);
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }
      break;
    }
  }

  if (!res.headersSent) {
    const code = lastErr?.name === 'AbortError' ? 504 : 502;
    res.status(code).json({ error: code===504?'Gateway Timeout':'Bad Gateway', message: lastErr?.message, reqId });
  }
}
