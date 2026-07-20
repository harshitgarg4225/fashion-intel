import { createHmac, timingSafeEqual } from "node:crypto";

// Cookie-session gate for every /api route. Active only when
// WARDROBE_PASSPHRASE is set in the environment (deliberately env-only:
// a UI-settable passphrase could be planted before the owner sets one).
const COOKIE = "wardrobe_session";

function json(res, status, value) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(value));
}

async function body(req, limit = 16 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error("Request body too large"), { status: 413 });
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8")); }
  catch { return {}; }
}

export function sessionTokenFor(passphrase) {
  return createHmac("sha256", passphrase).update("fashion-intel-session-v1").digest("hex");
}

function safeEqual(a, b) {
  const bufferA = Buffer.from(String(a));
  const bufferB = Buffer.from(String(b));
  return bufferA.length === bufferB.length && timingSafeEqual(bufferA, bufferB);
}

function cookieValue(req) {
  const header = req.headers.cookie || "";
  for (const part of header.split(";")) {
    const [name, ...rest] = part.trim().split("=");
    if (name === COOKIE) return rest.join("=");
  }
  return "";
}

export function authApi(options = {}) {
  const setting = (name, fallback = "") => options.env?.[name] || process.env[name] || fallback;
  const passphrase = () => setting("WARDROBE_PASSPHRASE").trim();

  async function handler(req, res, next) {
    const url = new URL(req.url, "http://localhost");
    const required = Boolean(passphrase());
    if (url.pathname === "/api/auth/status" && req.method === "GET") {
      const authed = !required || safeEqual(cookieValue(req), sessionTokenFor(passphrase()));
      return json(res, 200, { required, authed });
    }
    if (url.pathname === "/api/auth/login" && req.method === "POST") {
      if (!required) return json(res, 200, { authed: true });
      const input = await body(req);
      const attempt = typeof input.passphrase === "string" ? input.passphrase : "";
      if (!attempt || !safeEqual(attempt, passphrase())) {
        await new Promise((resolve) => setTimeout(resolve, 400));
        return json(res, 401, { error: "That passphrase is not right." });
      }
      const secure = (req.headers["x-forwarded-proto"] || "").includes("https") ? "; Secure" : "";
      res.setHeader("Set-Cookie", `${COOKIE}=${sessionTokenFor(passphrase())}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${30 * 24 * 3600}${secure}`);
      return json(res, 200, { authed: true });
    }
    if (!url.pathname.startsWith("/api/")) return next();
    if (!required || safeEqual(cookieValue(req), sessionTokenFor(passphrase()))) return next();
    return json(res, 401, { error: "auth_required" });
  }

  return {
    name: "wardrobe-auth-api",
    apply: "serve",
    configureServer(server) { server.middlewares.use(handler); },
    configurePreviewServer(server) { server.middlewares.use(handler); },
  };
}
