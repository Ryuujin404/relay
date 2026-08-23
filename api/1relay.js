// ============================================
// Vercel Node.js Proxy — Optimized for 9router
// Timeout: 300s | Payload: 50MB | Stream: ON
// ============================================

export const config = {
  runtime: 'nodejs',           // ⬅️ Edge = 30s, Node.js = 300s
  maxDuration: 300,          // 5 menit penuh
  api: {
    bodyParser: false,         // Kita parse manual biar lebih kontrol
  },
};

const UPSTREAM_TIMEOUT = 290_000; // 290 detik (10 detik buffer sebelum Vercel kill)

export default async function handler(req, res) {
  const start = Date.now();

  // ── 1. Parse target ──────────────────────
  const target = req.headers['x-relay-target'];
  const relayPath = req.headers['x-relay-path'] || '/';

  if (!target) {
    console.log('[PROXY] ❌ Missing x-relay-target');
    return res.status(400).json({ error: 'Missing x-relay-target header' });
  }

  const targetUrl = target.replace(/\/$/, '') + relayPath;
  console.log(`[PROXY] ▶ ${req.method} ${targetUrl}`);

  // ── 2. Bersihin headers ──────────────────
  const headers = {};
  for (const [key, value] of Object.entries(req.headers)) {
    const k = key.toLowerCase();
    if (
      k !== 'x-relay-target' &&
      k !== 'x-relay-path' &&
      k !== 'host' &&
      k !== 'content-length' &&   // Biar fetch hitung ulang
      k !== 'connection'
    ) {
      headers[key] = value;
    }
  }
  headers['connection'] = 'keep-alive';

  // ── 3. Baca body (raw) ───────────────────
  let body = undefined;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    try {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const raw = Buffer.concat(chunks).toString('utf8');
      if (raw) {
        // Validate JSON (9router selalu kirim JSON)
        JSON.parse(raw);
        body = raw;
      }
    } catch (err) {
      console.log('[PROXY] ❌ Bad body:', err.message);
      return res.status(400).json({ error: 'Invalid JSON body' });
    }
  }

  // ── 4. Abort controller (graceful timeout) ─
  const ctrl = new AbortController();
  const timer = setTimeout(() => {
    console.log('[PROXY] ⏱ Aborting (290s limit)');
    ctrl.abort();
  }, UPSTREAM_TIMEOUT);

  // ── 5. Fetch upstream ────────────────────
  try {
    const response = await fetch(targetUrl, {
      method: req.method,
      headers,
      body,
      signal: ctrl.signal,
    });

    clearTimeout(timer);

    // Forward status
    res.status(response.status);

    // Forward headers (skip yang bikin conflict)
    response.headers.forEach((value, key) => {
      const k = key.toLowerCase();
      if (['content-encoding', 'transfer-encoding'].includes(k)) return;
      try { res.setHeader(key, value); } catch {}
    });

    // Stream response balik ke client
    if (response.body) {
      const reader = response.body.getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        res.write(Buffer.from(value));
      }
    }

    res.end();
    console.log(`[PROXY] ✅ Done ${response.status} in ${Date.now() - start}ms`);

  } catch (err) {
    clearTimeout(timer);

    if (err.name === 'AbortError') {
      console.log('[PROXY] ⏱ Upstream timeout');
      if (!res.headersSent) {
        return res.status(504).json({
          error: 'Gateway Timeout',
          message: 'Upstream took too long (>290s)',
        });
      }
    }

    console.log('[PROXY] ❌ Error:', err.message);
    if (!res.headersSent) {
      return res.status(502).json({
        error: 'Bad Gateway',
        message: err.message,
      });
    }
  }
}
