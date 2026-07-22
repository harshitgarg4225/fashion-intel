import { createCipheriv, createDecipheriv, randomBytes, randomUUID, scryptSync } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { makeSetting } from "./settings-store.mjs";
import { tenantDataDir } from "./tenant.mjs";

const API_ROOT = "/api/google";
const SCOPE = "https://www.googleapis.com/auth/photospicker.mediaitems.readonly";
const PICKER_IMPORT_LIMIT = 24;
const SESSION_ID = /^[\w./-]+$/;

function json(res, status, value) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(value));
}

export function googlePhotosApi(options = {}) {
  let root;
  let baseTokenDir;
  const tokenFileFn = () => path.join(tenantDataDir() || baseTokenDir, "google-token.json");
  const bridge = options.bridge || {};
  const pendingStates = new Set();
  const setting = makeSetting(options);

  // Tokens are encrypted at rest when a passphrase is configured.
  function tokenKey() {
    const passphrase = (options.env?.WARDROBE_PASSPHRASE || process.env.WARDROBE_PASSPHRASE || "").trim();
    return passphrase ? scryptSync(passphrase, "fashion-intel-token-v1", 32) : null;
  }

  function sealToken(record) {
    const key = tokenKey();
    if (!key) return JSON.stringify(record, null, 2);
    const iv = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", key, iv);
    const encrypted = Buffer.concat([cipher.update(JSON.stringify(record), "utf8"), cipher.final()]);
    return JSON.stringify({ enc: "aes-256-gcm", iv: iv.toString("base64"), tag: cipher.getAuthTag().toString("base64"), data: encrypted.toString("base64") });
  }

  function openToken(raw) {
    const parsed = JSON.parse(raw);
    if (parsed?.enc !== "aes-256-gcm") return parsed;
    const key = tokenKey();
    if (!key) throw new Error("Stored Google token is encrypted; WARDROBE_PASSPHRASE is required to read it.");
    const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(parsed.iv, "base64"));
    decipher.setAuthTag(Buffer.from(parsed.tag, "base64"));
    return JSON.parse(Buffer.concat([decipher.update(Buffer.from(parsed.data, "base64")), decipher.final()]).toString("utf8"));
  }
  const authBase = () => setting("GOOGLE_OAUTH_BASE_URL", "https://accounts.google.com/o/oauth2/v2/auth");
  const tokenUrl = () => setting("GOOGLE_TOKEN_URL", "https://oauth2.googleapis.com/token");
  const pickerBase = () => setting("GOOGLE_PICKER_BASE_URL", "https://photospicker.googleapis.com/v1").replace(/\/$/, "");
  const configured = () => Boolean(setting("GOOGLE_CLIENT_ID").trim() && setting("GOOGLE_CLIENT_SECRET").trim());

  async function loadToken() {
    try { return openToken(await readFile(tokenFileFn(), "utf8")); }
    catch (error) { if (error.code === "ENOENT") return null; throw error; }
  }

  async function saveToken(payload, previous = null) {
    const record = {
      access_token: payload.access_token,
      refresh_token: payload.refresh_token || previous?.refresh_token || null,
      expires_at: Date.now() + (Math.max(60, Number(payload.expires_in) || 3600) - 30) * 1000,
    };
    await mkdir(path.dirname(tokenFileFn()), { recursive: true });
    await writeFile(tokenFileFn(), `${sealToken(record)}\n`, { mode: 0o600 });
    return record;
  }

  async function accessToken() {
    let token = await loadToken();
    if (!token) throw Object.assign(new Error("Google Photos is not connected yet."), { status: 401 });
    if (Date.now() < token.expires_at) return token.access_token;
    if (!token.refresh_token) throw Object.assign(new Error("The Google Photos session expired. Reconnect to continue."), { status: 401 });
    const response = await fetch(tokenUrl(), {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: setting("GOOGLE_CLIENT_ID"),
        client_secret: setting("GOOGLE_CLIENT_SECRET"),
        refresh_token: token.refresh_token,
        grant_type: "refresh_token",
      }),
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      await rm(tokenFileFn(), { force: true });
      throw Object.assign(new Error("Google Photos access could not be refreshed. Reconnect to continue."), { status: 401 });
    }
    token = await saveToken(result, token);
    return token.access_token;
  }

  async function pickerRequest(method, pathName, token) {
    const response = await fetch(`${pickerBase()}${pathName}`, { method, headers: { Authorization: `Bearer ${token}` } });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw Object.assign(new Error(result.error?.message || `Google Photos request failed (${response.status})`), { status: response.status === 401 ? 401 : 502 });
    return result;
  }

  function redirectUri(req) {
    const host = req.headers.host || "localhost:5173";
    const proto = (req.headers["x-forwarded-proto"] || "").includes("https") ? "https" : "http";
    return `${proto}://${host}${API_ROOT}/callback`;
  }

  async function handler(req, res, next) {
    const url = new URL(req.url, "http://localhost");
    if (!url.pathname.startsWith(`${API_ROOT}/`) && url.pathname !== API_ROOT) return next();
    try {
      if (url.pathname === `${API_ROOT}/status` && req.method === "GET") {
        const token = await loadToken();
        return json(res, 200, { configured: configured(), connected: Boolean(token) });
      }
      if (url.pathname === `${API_ROOT}/auth` && req.method === "GET") {
        if (!configured()) return json(res, 503, { error: "Add GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET to .env to connect Google Photos." });
        const state = randomUUID();
        pendingStates.add(state);
        setTimeout(() => pendingStates.delete(state), 10 * 60 * 1000).unref?.();
        const target = new URL(authBase());
        target.searchParams.set("client_id", setting("GOOGLE_CLIENT_ID"));
        target.searchParams.set("redirect_uri", redirectUri(req));
        target.searchParams.set("response_type", "code");
        target.searchParams.set("scope", SCOPE);
        target.searchParams.set("access_type", "offline");
        target.searchParams.set("prompt", "consent");
        target.searchParams.set("state", state);
        res.statusCode = 302;
        res.setHeader("Location", target.toString());
        return res.end();
      }
      if (url.pathname === `${API_ROOT}/callback` && req.method === "GET") {
        const state = url.searchParams.get("state") || "";
        const code = url.searchParams.get("code") || "";
        if (!pendingStates.delete(state)) return json(res, 400, { error: "Invalid or expired sign-in attempt. Start the connection again." });
        if (!code) return json(res, 400, { error: url.searchParams.get("error") || "Google did not return an authorization code." });
        const response = await fetch(tokenUrl(), {
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
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result.access_token) return json(res, 502, { error: result.error_description || "Google sign-in could not be completed." });
        await saveToken(result);
        res.statusCode = 302;
        res.setHeader("Location", "/?google=connected");
        return res.end();
      }
      if (url.pathname === `${API_ROOT}/disconnect` && req.method === "POST") {
        await rm(tokenFileFn(), { force: true });
        return json(res, 200, { disconnected: true });
      }
      if (url.pathname === `${API_ROOT}/picker/session` && req.method === "POST") {
        const token = await accessToken();
        const session = await pickerRequest("POST", "/sessions", token);
        return json(res, 200, {
          id: session.id,
          pickerUri: session.pickerUri,
          pollIntervalMs: Math.max(2000, Math.round(Number.parseFloat(session.pollingConfig?.pollInterval) * 1000) || 3000),
        });
      }
      const sessionMatch = url.pathname.match(/^\/api\/google\/picker\/session\/([^/]+)(?:\/(import))?$/);
      if (sessionMatch && SESSION_ID.test(decodeURIComponent(sessionMatch[1]))) {
        const sessionId = decodeURIComponent(sessionMatch[1]);
        const token = await accessToken();
        if (!sessionMatch[2] && req.method === "GET") {
          const session = await pickerRequest("GET", `/sessions/${encodeURIComponent(sessionId)}`, token);
          return json(res, 200, { id: session.id, mediaItemsSet: Boolean(session.mediaItemsSet) });
        }
        if (sessionMatch[2] === "import" && req.method === "POST") {
          if (!bridge.createJobsFromImage) return json(res, 503, { error: "The import pipeline is not ready yet. Retry in a moment." });
          const setup = await bridge.setupStatus?.();
          if (setup && !setup.ready) return json(res, 503, { error: "Setup required: add OPENAI_API_KEY in .env and a PNG photo of yourself at data/model-reference.png." });
          const items = [];
          let pageToken = "";
          do {
            const page = await pickerRequest("GET", `/mediaItems?sessionId=${encodeURIComponent(sessionId)}&pageSize=100${pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : ""}`, token);
            items.push(...(page.mediaItems || []));
            pageToken = page.nextPageToken || "";
          } while (pageToken && items.length < PICKER_IMPORT_LIMIT * 2);
          const photos = items.filter((item) => (item.type || "PHOTO") === "PHOTO" && item.mediaFile?.baseUrl);
          const selected = photos.slice(0, PICKER_IMPORT_LIMIT);
          if (!selected.length) return json(res, 400, { error: "No photos were selected in Google Photos." });
          void (async () => {
            for (const item of selected) {
              try {
                const download = await fetch(`${item.mediaFile.baseUrl}=d`, { headers: { Authorization: `Bearer ${token}` } });
                if (!download.ok) throw new Error(`download failed (${download.status})`);
                await bridge.createJobsFromImage(Buffer.from(await download.arrayBuffer()));
              } catch (error) {
                console.error(`[fashion-intel] Google Photos import skipped ${item.mediaFile?.filename || item.id}: ${error.message}`);
              }
            }
            await pickerRequest("DELETE", `/sessions/${encodeURIComponent(sessionId)}`, token).catch(() => {});
          })();
          return json(res, 202, { queued: selected.length, skipped: photos.length - selected.length });
        }
      }
      return json(res, 404, { error: "Not found" });
    } catch (error) {
      const statusCode = error.status || 500;
      return json(res, statusCode, { error: error.message || "Internal server error" });
    }
  }

  return {
    name: "wardrobe-google-photos-api",
    apply: "serve",
    configResolved(config) {
      root = config.root;
      baseTokenDir = path.resolve(root, setting("WARDROBE_DATA_DIR", "data"));
    },
    configureServer(server) { server.middlewares.use(handler); },
    configurePreviewServer(server) { server.middlewares.use(handler); },
  };
}
