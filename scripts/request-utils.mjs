// Client identity for rate limiting. X-Forwarded-For is client-forgeable:
// a proxy APPENDS the real IP, so the leftmost value is attacker-controlled.
// Behind a trusted proxy (Railway, or WARDROBE_TRUST_PROXY=true) the RIGHTMOST
// hop is the one the proxy itself observed; otherwise trust only the socket.
export function isTrustedProxy(env = {}) {
  const read = (name) => env?.[name] ?? process.env[name];
  return Boolean(read("RAILWAY_ENVIRONMENT") || read("RAILWAY_PROJECT_ID") || read("WARDROBE_TRUST_PROXY") === "true");
}

export function clientIp(req, trustProxy) {
  if (trustProxy) {
    const forwarded = (req.headers["x-forwarded-for"] || "").toString();
    const hops = forwarded.split(",").map((part) => part.trim()).filter(Boolean);
    if (hops.length) return hops[hops.length - 1];
  }
  return req.socket?.remoteAddress || "unknown";
}
