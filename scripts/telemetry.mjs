import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { tenantDataDir } from "./tenant.mjs";

// Rough per-call estimates (USD) used only for the local spend meter.
const EST_COST = { image: 0.17, vision: 0.01 };

let fallbackDir = null;

export function initTelemetry(dir) {
  fallbackDir = dir;
}

function dataDir() {
  return tenantDataDir() || fallbackDir;
}

function today() {
  return new Date().toISOString().slice(0, 10);
}

export async function audit(event) {
  if (!dataDir()) return;
  try {
    await mkdir(dataDir(), { recursive: true });
    await appendFile(path.join(dataDir(), "audit.jsonl"), `${JSON.stringify({ ts: new Date().toISOString(), ...event })}\n`);
  } catch {}
}

async function loadUsage() {
  if (!dataDir()) return { days: {} };
  try {
    return JSON.parse(await readFile(path.join(dataDir(), "usage.json"), "utf8"));
  } catch {
    return { days: {} };
  }
}

export async function recordUsage(kind) {
  if (!dataDir()) return;
  try {
    const usage = await loadUsage();
    const day = usage.days[today()] || { visionCalls: 0, imageCalls: 0, estCostUsd: 0 };
    if (kind === "image") day.imageCalls += 1;
    else day.visionCalls += 1;
    day.estCostUsd = Math.round((day.estCostUsd + (EST_COST[kind] || 0)) * 1000) / 1000;
    usage.days[today()] = day;
    if (kind === "image") usage.totalImageCalls = (Number(usage.totalImageCalls) || 0) + 1;
    const keep = Object.keys(usage.days).sort().slice(-60);
    usage.days = Object.fromEntries(keep.map((key) => [key, usage.days[key]]));
    await mkdir(dataDir(), { recursive: true });
    await writeFile(path.join(dataDir(), "usage.json"), `${JSON.stringify(usage, null, 2)}\n`);
  } catch {}
}

export async function usageToday() {
  const usage = await loadUsage();
  return usage.days[today()] || { visionCalls: 0, imageCalls: 0, estCostUsd: 0 };
}

export async function checkImageBudget(setting) {
  const budget = Number.parseFloat(setting("WARDROBE_DAILY_BUDGET"));
  if (!Number.isFinite(budget) || budget <= 0) return;
  const day = await usageToday();
  if (day.estCostUsd + EST_COST.image > budget) {
    throw new Error(`Daily budget reached (about $${day.estCostUsd.toFixed(2)} of $${budget.toFixed(2)} used). Raise WARDROBE_DAILY_BUDGET or try tomorrow.`);
  }
}
