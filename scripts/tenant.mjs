import { AsyncLocalStorage } from "node:async_hooks";
import { readFile } from "node:fs/promises";
import path from "node:path";

// Request-scoped tenancy. In multi-tenant mode the SSO middleware wraps each
// authenticated request in runAsUser(), and every data path in the app
// resolves through tenantDataDir() — the engine itself stays tenant-blind.
export const tenantStorage = new AsyncLocalStorage();

let baseDir = null;

export function initTenancy(dir) { baseDir = dir; }
export function baseDataDir() { return baseDir; }

export function isMultiTenant(env = {}) {
  return (env?.MIRA_MULTI_TENANT ?? process.env.MIRA_MULTI_TENANT) === "true";
}

export function tenantDataDir() {
  return tenantStorage.getStore()?.userDir || baseDir;
}

export function currentUser() {
  return tenantStorage.getStore()?.user || null;
}

export function userDirFor(base, userId) {
  return path.join(base, "users", userId);
}

export function runAsUser(user, userDir, fn) {
  return tenantStorage.run({ user, userDir }, fn);
}

// Render credits: a user's lifetime image generations are capped by their
// credit allowance. Only enforced when a tenant user is in context.
export async function checkRenderCredits(env = {}) {
  const context = tenantStorage.getStore();
  if (!isMultiTenant(env) || !context?.user) return;
  const allowance = Number(context.user.credits ?? env?.MIRA_FREE_RENDERS ?? process.env.MIRA_FREE_RENDERS ?? 25);
  let lifetime = 0;
  try {
    const usage = JSON.parse(await readFile(path.join(context.userDir, "usage.json"), "utf8"));
    lifetime = Number(usage.totalImageCalls) || 0;
  } catch {}
  if (lifetime >= allowance) {
    throw new Error(`You've used all ${allowance} render credits. More are on the way — for now, ask the owner to raise your allowance.`);
  }
}
