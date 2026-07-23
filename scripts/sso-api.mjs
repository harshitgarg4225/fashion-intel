import { createHmac, randomUUID, timingSafeEqual, createHash } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { atomicJson } from "./import-job-api.mjs";
import { isMultiTenant, runAsUser, userDirFor } from "./tenant.mjs";
import { findUserByRefCode, generateRefCode, referralBonuses, referralSummary } from "./referrals.mjs";

// Google SSO + per-user gating for multi-tenant mode. Inactive unless
// MIRA_MULTI_TENANT=true. Requires GOOGLE_CLIENT_ID/SECRET (web OAuth
// client with redirect .../auth/google/callback) and MIRA_SESSION_SECRET.
const COOKIE = "mira_session";
const SCOPE = "openid email profile";

function json(res, status, value) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(value));
}

export function ssoApi(options = {}) {
  const setting = (name, fallback = "") => options.env?.[name] || process.env[name] || fallback;
  const active = () => isMultiTenant(options.env);
  const secret = () => setting("MIRA_SESSION_SECRET").trim();
  const pendingStates = new Set();
  let baseDir = null;
  let usersFile = null;

  const sign = (userId) => createHmac("sha256", secret()).update(`mira-user-${userId}`).digest("base64url");
  const userIdFor = (email) => createHash("sha256").update(email.trim().toLowerCase()).digest("hex").slice(0, 16);

  function cookieUser(req) {
    const header = req.headers.cookie || "";
    for (const part of header.split(";")) {
      const [name, ...rest] = part.trim().split("=");
      if (name !== COOKIE) continue;
      const [userId, signature] = rest.join("=").split(".");
      if (!userId || !signature) return null;
      const expected = Buffer.from(sign(userId));
      const given = Buffer.from(signature);
      if (expected.length === given.length && timingSafeEqual(expected, given)) return userId;
      return null;
    }
    return null;
  }

  function refCookie(req) {
    const header = req.headers.cookie || "";
    for (const part of header.split(";")) {
      const [name, ...rest] = part.trim().split("=");
      if (name === "mira_ref") return decodeURIComponent(rest.join("=")).slice(0, 32);
    }
    return null;
  }

  async function loadUsers() {
    try { return JSON.parse(await readFile(usersFile, "utf8")); }
    catch (error) { if (error.code === "ENOENT") return {}; throw error; }
  }

  function redirectUri(req) {
    const proto = (req.headers["x-forwarded-proto"] || "").includes("https") ? "https" : "http";
    return `${proto}://${req.headers.host || "localhost"}/auth/google/callback`;
  }

  async function handler(req, res, next) {
    if (!active()) return next();
    const url = new URL(req.url, "http://localhost");

    if (url.pathname === "/auth/google" && req.method === "GET") {
      if (!setting("GOOGLE_CLIENT_ID").trim() || !secret() || secret().length < 16) {
        return json(res, 503, { error: "SSO is not configured: set GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and a MIRA_SESSION_SECRET of at least 16 characters." });
      }
      const state = randomUUID();
      pendingStates.add(state);
      setTimeout(() => pendingStates.delete(state), 10 * 60 * 1000).unref?.();
      const target = new URL(setting("GOOGLE_OAUTH_BASE_URL", "https://accounts.google.com/o/oauth2/v2/auth"));
      target.searchParams.set("client_id", setting("GOOGLE_CLIENT_ID"));
      target.searchParams.set("redirect_uri", redirectUri(req));
      target.searchParams.set("response_type", "code");
      target.searchParams.set("scope", SCOPE);
      target.searchParams.set("state", state);
      res.statusCode = 302;
      res.setHeader("Location", target.toString());
      return res.end();
    }

    if (url.pathname === "/auth/google/callback" && req.method === "GET") {
      const state = url.searchParams.get("state") || "";
      const code = url.searchParams.get("code") || "";
      if (!pendingStates.delete(state) || !code) return json(res, 400, { error: "Sign-in expired. Start again." });
      const tokenResponse = await fetch(setting("GOOGLE_TOKEN_URL", "https://oauth2.googleapis.com/token"), {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: setting("GOOGLE_CLIENT_ID"),
          client_secret: setting("GOOGLE_CLIENT_SECRET"),
          code,
          grant_type: "authorization_code",
          redirect_uri: redirectUri(req),
        }),
      });
      const token = await tokenResponse.json().catch(() => ({}));
      if (!tokenResponse.ok || !token.access_token) return json(res, 502, { error: "Google sign-in could not be completed." });
      const infoResponse = await fetch(setting("GOOGLE_USERINFO_URL", "https://openidconnect.googleapis.com/v1/userinfo"), {
        headers: { Authorization: `Bearer ${token.access_token}` },
      });
      const info = await infoResponse.json().catch(() => ({}));
      if (!infoResponse.ok || !info.email) return json(res, 502, { error: "Google did not return a verified account." });
      const userId = userIdFor(info.email);
      const users = await loadUsers();
      const now = new Date().toISOString();
      const isNewUser = !users[userId];
      users[userId] = {
        ...users[userId],
        id: userId,
        email: info.email.trim().toLowerCase(),
        name: typeof info.name === "string" ? info.name.slice(0, 120) : "",
        picture: typeof info.picture === "string" ? info.picture.slice(0, 500) : "",
        credits: users[userId]?.credits ?? Number(setting("MIRA_FREE_RENDERS", "25")),
        createdAt: users[userId]?.createdAt || now,
        lastLoginAt: now,
      };
      // Referral attribution: a share page set the mira_ref cookie; a brand-new
      // account records who invited them. Self-referrals are ignored.
      if (isNewUser) {
        const referrer = findUserByRefCode(users, refCookie(req));
        if (referrer && referrer.id !== userId) {
          users[userId].referredBy = referrer.id;
          users[userId].referredAt = now;
        }
      }
      await atomicJson(usersFile, users);
      await mkdir(userDirFor(baseDir, userId), { recursive: true });
      const secure = (req.headers["x-forwarded-proto"] || "").includes("https") ? "; Secure" : "";
      res.setHeader("Set-Cookie", [
        `${COOKIE}=${userId}.${sign(userId)}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${30 * 24 * 3600}${secure}`,
        `mira_ref=; SameSite=Lax; Path=/; Max-Age=0`,
      ]);
      res.statusCode = 302;
      res.setHeader("Location", "/");
      return res.end();
    }

    if (url.pathname === "/api/auth/logout" && req.method === "POST") {
      res.setHeader("Set-Cookie", `${COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0`);
      return json(res, 200, { ok: true });
    }

    if (url.pathname === "/api/auth/status" && req.method === "GET") {
      const userId = cookieUser(req);
      return json(res, 200, { required: true, mode: "google", authed: Boolean(userId) });
    }

    if (!url.pathname.startsWith("/api/")) return next();
    if (url.pathname === "/api/health" || url.pathname === "/api/billing/webhook") return next();

    const userId = cookieUser(req);
    if (!userId) return json(res, 401, { error: "auth_required", mode: "google" });
    const users = await loadUsers();
    const user = users[userId];
    if (!user) return json(res, 401, { error: "auth_required", mode: "google" });

    if (url.pathname === "/api/me" && req.method === "DELETE") {
      // Right to erasure: remove the closet directory, share links, billing
      // records, and the account itself, then end the session.
      await rm(userDirFor(baseDir, userId), { recursive: true, force: true });
      const sharesFile = path.join(baseDir, "shares.json");
      try {
        const shares = JSON.parse(await readFile(sharesFile, "utf8"));
        await atomicJson(sharesFile, shares.filter((entry) => entry.userId !== userId));
      } catch {}
      const billingFile = path.join(baseDir, "billing.json");
      try {
        const orders = JSON.parse(await readFile(billingFile, "utf8"));
        await atomicJson(billingFile, orders.filter((entry) => entry.userId !== userId));
      } catch {}
      delete users[userId];
      await atomicJson(usersFile, users);
      res.setHeader("Set-Cookie", `${COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0`);
      return json(res, 200, { deleted: true });
    }

    if (url.pathname === "/api/me" && req.method === "GET") {
      let rendersUsed = 0;
      try {
        const usage = JSON.parse(await readFile(path.join(userDirFor(baseDir, userId), "usage.json"), "utf8"));
        rendersUsed = Number(usage.totalImageCalls) || 0;
      } catch {}
      // Mint the invite code on first request so the invite link is always ready.
      if (!user.refCode) {
        user.refCode = generateRefCode(users);
        await atomicJson(usersFile, users);
      }
      const referrals = await referralSummary(userId);
      return json(res, 200, {
        id: user.id, email: user.email, name: user.name, picture: user.picture,
        credits: user.credits, rendersUsed,
        invitePath: `/?ref=${user.refCode}`,
        referrals,
        referralBonuses: referralBonuses(options.env),
      });
    }

    const userDir = userDirFor(baseDir, userId);
    await mkdir(path.join(userDir, "jobs"), { recursive: true });
    await mkdir(path.join(userDir, "imported"), { recursive: true });
    return runAsUser(user, userDir, () => next());
  }

  return {
    name: "mira-sso-api",
    apply: "serve",
    configResolved(config) {
      const dataDir = path.resolve(config.root, setting("WARDROBE_DATA_DIR", "data"));
      baseDir = dataDir;
      usersFile = path.join(dataDir, "users.json");
    },
    configureServer(server) { server.middlewares.use(handler); },
    configurePreviewServer(server) { server.middlewares.use(handler); },
  };
}
