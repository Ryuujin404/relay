export const config = { runtime: "edge" };

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
  headers.delete("content-length");
  
  // ⬇️ TAMBAHIN INI: Force Accept header ke text/event-stream (format streaming)
  headers.set("Accept", "text/event-stream, application/json");

  let body = undefined;

  if (req.method !== "GET" && req.method !== "HEAD") {
    const text = await req.text();
    try {
      const json = JSON.parse(text);
      if (json.stream === false) {
        json.stream = true;
        console.log("[PROXY] Forced stream: false → true");
      }
      body = JSON.stringify(json);
    } catch {
      body = text;
    }
  }

  const response = await fetch(targetUrl, {
    method: req.method,
    headers,
    body,
    duplex: "half",
  });

  return new Response(response.body, {
    status: response.status,
    headers: response.headers,
  });
}
