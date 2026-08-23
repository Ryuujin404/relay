export const config = {
  runtime: 'nodejs',
  maxDuration: 300,
  api: { bodyParser: false },
};

const UPSTREAM_TIMEOUT = 290_000;
const CONNECT_TIMEOUT = 15_000;
const MAX_RETRIES = 2;

function uid() {
  return Math.random().toString(36).slice(2, 10);
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
  if (!target) return res.status(400).json({ error: 'Missing x-relay-target', reqId });

  const targetUrl = target.replace(/\/$/, '') + relayPath;

  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (!['x-relay-target','x-relay-path','host','content-length','connection','accept-encoding'].includes(kl)) {
      headers[k] = v;
    }
  }
  headers['x-request-id'] = reqId;
  headers['accept-encoding'] = 'identity'; // ⬇️ Jangan gzip biar bisa stream langsung

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
    const connectTimer = setTimeout(() => ctrl.abort(), CONNECT_TIMEOUT);

    try {
      const response = await fetch(targetUrl, {
        method: req.method,
        headers,
        body,
        signal: ctrl.signal,
      });

      clearTimeout(totalTimer);
      clearTimeout(connectTimer);

      const cl = response.headers.get('content-length');
      if (cl && parseInt(cl) > 50 * 1024 * 1024) {
        return res.status(413).json({ error: 'Response too large', reqId });
      }

      if (response.status >= 502 && response.status <= 504 && attempt < MAX_RETRIES) {
        console.log(`[${reqId}] ⚠ ${response.status}, retry ${attempt}/${MAX_RETRIES}`);
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }

      // ⬇️ STREAMING FIX: Set headers anti-buffer
      res.status(response.status);
      res.setHeader('X-Accel-Buffering', 'no');
      res.setHeader('Transfer-Encoding', 'chunked');
      
      const ct = response.headers.get('content-type');
      if (ct) res.setHeader('Content-Type', ct);
      
      res.setHeader('x-proxy-req-id', reqId);

      if (response.body) {
        const reader = response.body.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          res.write(Buffer.from(value));
          if (res.flush) res.flush(); // ⬇️ Flush langsung ke client
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
