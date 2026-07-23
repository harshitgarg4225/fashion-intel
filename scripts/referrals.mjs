import { randomBytes } from "node:crypto";
import { readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { baseDataDir, currentUser, isMultiTenant } from "./tenant.mjs";

// The referral loop: share pages carry the sharer's code, signups record who
// referred them, and both sides earn render credits when the referred user
// completes their FIRST successful render (not signup — renders are the
// costly action, so throwaway accounts earn nothing).

const usersFile = () => path.join(baseDataDir(), "users.json");

async function writeAtomic(file, value) {
  const temp = `${file}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`;
  await writeFile(temp, JSON.stringify(value, null, 2), { mode: 0o600 });
  await rename(temp, file);
}

async function loadUsers() {
  try { return JSON.parse(await readFile(usersFile(), "utf8")); }
  catch (error) { if (error.code === "ENOENT") return {}; throw error; }
}

export function generateRefCode(users) {
  const taken = new Set(Object.values(users).map((user) => user.refCode).filter(Boolean));
  let code;
  do { code = randomBytes(6).toString("base64url"); } while (taken.has(code));
  return code;
}

export function findUserByRefCode(users, code) {
  if (!code || typeof code !== "string" || code.length > 32) return null;
  return Object.values(users).find((user) => user.refCode === code) || null;
}

// Returns the user's referral code, minting and persisting one on first use.
export async function refCodeFor(userId) {
  const users = await loadUsers();
  const user = users[userId];
  if (!user) return null;
  if (!user.refCode) {
    user.refCode = generateRefCode(users);
    await writeAtomic(usersFile(), users);
  }
  return user.refCode;
}

export function referralBonuses(env = {}) {
  const read = (name, fallback) => {
    const value = Number(env?.[name] ?? process.env[name]);
    return Number.isFinite(value) && value >= 0 ? Math.round(value) : fallback;
  };
  return { referrer: read("MIRA_REFERRAL_BONUS", 10), referred: read("MIRA_REFERRED_BONUS", 5) };
}

// Fire-and-forget after a successful image generation: if the current tenant
// user was referred and hasn't triggered the reward yet, credit both sides.
export async function maybeGrantReferralBonus(env = {}) {
  if (!isMultiTenant(env)) return;
  const user = currentUser();
  if (!user?.id) return;
  const users = await loadUsers();
  const record = users[user.id];
  if (!record?.referredBy || record.refRewarded) return;
  const bonuses = referralBonuses(env);
  record.refRewarded = true;
  record.credits = (Number(record.credits) || 0) + bonuses.referred;
  const referrer = users[record.referredBy];
  if (referrer) {
    referrer.credits = (Number(referrer.credits) || 0) + bonuses.referrer;
    referrer.referralEarned = (Number(referrer.referralEarned) || 0) + bonuses.referrer;
  }
  await writeAtomic(usersFile(), users);
}

// Summary for /api/me: invite link, how many friends joined, credits earned.
export async function referralSummary(userId) {
  const users = await loadUsers();
  const user = users[userId];
  if (!user) return null;
  const referred = Object.values(users).filter((entry) => entry.referredBy === userId);
  return {
    refCode: user.refCode || null,
    referredCount: referred.length,
    referredActivated: referred.filter((entry) => entry.refRewarded).length,
    referralEarned: Number(user.referralEarned) || 0,
  };
}
