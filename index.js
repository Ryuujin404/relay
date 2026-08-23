const express = require('express');
const cors = require('cors');
const http = require('http');
const https = require('https');

const app = express();
app.use(cors());
app.use(express.json({ limit: '100mb' }));

app.get('/health', (req, res) => res.json({ ok: true }));

app.all('*', (req, res) => {
  const target = req.headers['x-target-url'] || process.env.DEFAULT_TARGET_URL;
  if (!target) return res.status(400).json({ error: 'No target URL' });

  try {
    const url = new URL(target);
    const options = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: req.method,
      headers: { ...req.headers, host: url.hostname },
      timeout: 600000, // 10 menit
    };
    delete options.headers['x-target-url'];

    const proxyReq = https.request(options, (proxyRes) => {
      res.status(proxyRes.statusCode);
      Object.keys(proxyRes.headers).forEach(k => res.setHeader(k, proxyRes.headers[k]));
      proxyRes.pipe(res);
    });

    proxyReq.on('error', (e) => {
      if (!res.headersSent) res.status(502).json({ error: e.message });
    });

    proxyReq.on('timeout', () => {
      proxyReq.destroy();
      if (!res.headersSent) res.status(504).json({ error: 'Timeout' });
    });

    req.pipe(proxyReq);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Proxy on ${PORT}`));
