#!/usr/bin/env node
// Export (and optionally wipe) all local Fashion Intel data.
//   npm run backup        → writes wardrobe-backup-<stamp>.tgz in the repo root
//   npm run backup:wipe   → same, then deletes data/ entirely
import { spawnSync } from "node:child_process";
import { existsSync, rmSync } from "node:fs";
import path from "node:path";

const dataDir = process.env.WARDROBE_DATA_DIR || "data";
const wipe = process.argv.includes("--wipe");

if (!existsSync(dataDir)) {
  console.log(`Nothing to back up: ${dataDir}/ does not exist.`);
  process.exit(0);
}

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const archive = `wardrobe-backup-${stamp}.tgz`;
const result = spawnSync("tar", ["czf", archive, dataDir], { stdio: "inherit" });
if (result.status !== 0) {
  console.error("Backup failed; nothing was deleted.");
  process.exit(result.status || 1);
}
console.log(`Backed up ${dataDir}/ to ${path.resolve(archive)}`);

if (wipe) {
  rmSync(dataDir, { recursive: true, force: true });
  console.log(`Wiped ${dataDir}/ — your photos, tokens, and database are removed from this machine. The archive above is the only copy.`);
}
