const express = require('express');
const cors = require('cors');
const http = require('http');
const https = require('https');

const app = express();
app.use(cors());
app.use(express.json({ limit: '100mb' }));

// Logging biar tau request masuk
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// Health check buat Railway
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

// Proxy handler — forward SEMUA request ke target
app.all('*', async (req, res) => {
  // 9router biasanya kirim target URL via header atau query
  // Atau kita bisa hardcode beberapa provider di env
  const targetUrl = req.headers['x-target-url'] || process.env.DEFAULT_TARGET_URL;
  
  if (!targetUrl) {
    return res.status(400).json({ 
      error: 'No target URL. Provide x-target-url header or set DEFAULT_TARGET_URL' 
    });
  }

  try {
    const url = new URL(targetUrl);
    const options = {
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + url.search,
      method: req.method,
      headers: {
        ...req.headers,
        host: url.hostname,
      },
      // ⬇️ INI PENTING: Railway unlimited timeout, tapi set biar nggak hang selamanya
      timeout: 600000, // 10 menit
    };

    // Hapus header yang bikin conflict
    delete options.headers['x-target-url'];
    delete options.headers['content-length']; // biar auto-recalculate

    const client = url.protocol === 'https:' ? https : http;
    
    const proxyReq = client.request(options, (proxyRes) => {
      res.status(proxyRes.statusCode);
      Object.keys(proxyRes.headers).forEach(key => {
        res.setHeader(key, proxyRes.headers[key]);
      });
      proxyRes.pipe(res);
    });

    proxyReq.on('error', (err) => {
      console.error('Proxy error:', err.message);
      if (!res.headersSent) {
        res.status(502).json({ error: 'Proxy error', detail: err.message });
      }
    });

    proxyReq.on('timeout', () => {
      console.error('Proxy timeout');
      proxyReq.destroy();
      if (!res.headersSent) {
        res.status(504).json({ error: 'Gateway timeout' });
      }
    });

    if (req.method !== 'GET' && req.method !== 'HEAD') {
      proxyReq.write(JSON.stringify(req.body));
    }
    proxyReq.end();

  } catch (err) {
    console.error('Error:', err.message);
    res.status(500).json({ error: 'Internal error', detail: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Generic AI Proxy running on port ${PORT}`);
  console.log(`Default target: ${process.env.DEFAULT_TARGET_URL || '(none - must provide x-target-url header)'}`);
});
