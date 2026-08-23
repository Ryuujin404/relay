export const config = {
  runtime: "nodejs",   // ⬇️ ganti dari "edge" ke "nodejs" (timeout bisa 300s)
  maxDuration: 300,    // ⬆️ 5 menit
};

export default async function handler(req) {
  const target = req.headers.get("x-relay-target");
  const relayPath = req.headers.get("x-relay-path") || "/";
  if (!target) {
    return new Response(JSON.stringify({ error: "Missing x-relay-target header" }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }

  const targetUrl = target.replace(/\/$/, "") + relayPath;

  const headers = new Headers(req.headers);
  headers.delete("x-relay-target");
  headers.delete("x-relay-path");
  headers.delete("host");

  // ⬇️ TAMBAH: AbortController biar nggak hang selamanya
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 290000);

  const response = await fetch(targetUrl, {
    method: req.method,
    headers,
    body: req.method !== "GET" && req.method !== "HEAD" ? req.body : undefined,
    signal: controller.signal,   // ⬆️ pakai signal
  });

  return new Response(response.body, {
    status: response.status,
    headers: response.headers,
  });
}
