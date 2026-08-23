export const config = {
  maxDuration: 300,
};

export default async function handler(req, res) {
  const target = req.headers['x-relay-target'];
  const relayPath = req.headers['x-relay-path'] || '/';
  
  if (!target) {
    return res.status(400).json({ error: 'Missing x-relay-target header' });
  }

  const targetUrl = target.replace(/\/$/, '') + relayPath;

  // Bersihin headers
  const headers = {};
  for (const [key, value] of Object.entries(req.headers)) {
    if (key !== 'x-relay-target' && key !== 'x-relay-path' && key !== 'host' && key !== 'content-length') {
      headers[key] = value;
    }
  }

  try {
    const response = await fetch(targetUrl, {
      method: req.method,
      headers,
      body: req.method !== 'GET' && req.method !== 'HEAD' ? JSON.stringify(req.body) : undefined,
    });

    res.status(response.status);
    for (const [key, value] of response.headers) {
      res.setHeader(key, value);
    }
    
    const body = await response.text();
    res.send(body);
    
  } catch (error) {
    res.status(502).json({ error: error.message });
  }
}
