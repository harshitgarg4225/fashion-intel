import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { isMultiTenant } from "./tenant.mjs";

// Keys the in-app setup screen may store in data/settings.json.
// Environment variables always win over stored values.
export const STORABLE_KEYS = new Set([
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "GEMINI_API_KEY",
  "FAL_API_KEY",
  "GOOGLE_CLIENT_ID",
  "GOOGLE_CLIENT_SECRET",
]);

let file = null;
let cache = {};

export async function initSettingsStore(dataDir) {
  file = path.join(dataDir, "settings.json");
  try {
    cache = JSON.parse(await readFile(file, "utf8"));
  } catch {
    cache = {};
  }
}

export function storedSetting(name) {
  return typeof cache[name] === "string" ? cache[name] : "";
}

export async function saveStoredSettings(patch) {
  for (const [name, value] of Object.entries(patch)) {
    if (!STORABLE_KEYS.has(name)) continue;
    const trimmed = typeof value === "string" ? value.trim() : "";
    if (trimmed) cache[name] = trimmed;
    else delete cache[name];
  }
  if (!file) return cache;
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, `${JSON.stringify(cache, null, 2)}\n`, { mode: 0o600 });
  return cache;
}

export function storedKeyStatus() {
  return Object.fromEntries([...STORABLE_KEYS].map((name) => [name, Boolean(cache[name])]));
}

export function makeSetting(options = {}) {
  // In multi-tenant mode only environment configuration counts — per-instance
  // stored keys would leak across users.
  if (isMultiTenant(options.env)) {
    return (name, fallback = "") => options.env?.[name] || process.env[name] || fallback;
  }
  return (name, fallback = "") => options.env?.[name] || process.env[name] || storedSetting(name) || fallback;
}
