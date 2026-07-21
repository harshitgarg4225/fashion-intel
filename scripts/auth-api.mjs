import { createHmac, timingSafeEqual } from "node:crypto";
import { clientIp, isTrustedProxy } from "./request-utils.mjs";

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
  const trustProxy = isTrustedProxy(options.env);
  const failedLogins = new Map();
  const DECAY_MS = 15 * 60 * 1000;
  const LOCK_MS = 10 * 60 * 1000;

  function loginLock(req) {
    const ip = clientIp(req, trustProxy);
    const entry = failedLogins.get(ip);
    if (!entry) return { ip, locked: false };
    const now = Date.now();
    if (entry.lockedUntil && now >= entry.lockedUntil) {
      failedLogins.delete(ip);
      return { ip, locked: false };
    }
    if (now - entry.lastFailAt > DECAY_MS) {
      failedLogins.delete(ip);
      return { ip, locked: false };
    }
    return { ip, locked: entry.count >= 8 && now < entry.lockedUntil };
  }

  function recordFailure(ip) {
    const now = Date.now();
    let entry = failedLogins.get(ip);
    if (!entry || now - entry.lastFailAt > DECAY_MS) entry = { count: 0, lockedUntil: 0, lastFailAt: now };
    entry.count += 1;
    entry.lastFailAt = now;
    if (entry.count >= 8) entry.lockedUntil = now + LOCK_MS;
    failedLogins.set(ip, entry);
    if (failedLogins.size > 5000) failedLogins.clear();
  }

  async function handler(req, res, next) {
    const url = new URL(req.url, "http://localhost");
    if (url.pathname === "/api/health" && req.method === "GET") {
      return json(res, 200, { ok: true });
    }
    const required = Boolean(passphrase());
    if (url.pathname === "/api/auth/status" && req.method === "GET") {
      const authed = !required || safeEqual(cookieValue(req), sessionTokenFor(passphrase()));
      return json(res, 200, { required, authed });
    }
    if (url.pathname === "/api/auth/login" && req.method === "POST") {
      if (!required) return json(res, 200, { authed: true });
      const { ip, locked } = loginLock(req);
      if (locked) return json(res, 429, { error: "Too many attempts. Try again in ten minutes." });
      const input = await body(req);
      const attempt = typeof input.passphrase === "string" ? input.passphrase : "";
      if (!attempt || !safeEqual(attempt, passphrase())) {
        recordFailure(ip);
        await new Promise((resolve) => setTimeout(resolve, 400));
        return json(res, 401, { error: "That passphrase is not right." });
      }
      failedLogins.delete(ip);
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
