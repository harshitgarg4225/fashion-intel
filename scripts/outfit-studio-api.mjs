import { randomUUID } from "node:crypto";
import { mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import sharp from "sharp";
import { atomicJson, imageEdit } from "./import-job-api.mjs";
import { curateOutfit, buildOutfitImagePrompt, planCapsule } from "./stylist.mjs";
import { resolveStylistProvider } from "./ai-providers.mjs";
import { checkImageBudget, initTelemetry, usageToday } from "./telemetry.mjs";
import { makeSetting } from "./settings-store.mjs";

const API_ROOT = "/api/outfits";
const IMAGE_ROOT = "/api/outfits/images";
const MAX_CONCURRENT_GENERATIONS = 3;

function json(res, status, value) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(value));
}

async function body(req, limit = 64 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error("Request body too large"), { status: 413 });
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8")); }
  catch { throw Object.assign(new Error("Expected a JSON request body"), { status: 400 }); }
}

const VISUAL_STYLIST_MAX_ITEMS = 20;

function decodeDataUrl(raw) {
  if (!raw || typeof raw !== "string") return null;
  const match = raw.match(/^data:([^;]+);base64,(.+)$/s);
  const data = Buffer.from(match?.[2] || raw, "base64");
  return data.length ? { data, mime: match?.[1] || "image/png" } : null;
}

export function outfitStudioApi(options = {}) {
  let root;
  let outfitsFile;
  let outfitImagesDir;
  let inspirationDir;
  let profileFile;
  let libraryFile;
  let importedDir;
  const running = new Map();
  const setting = makeSetting(options);

  function currentSeason() {
    const month = new Date().getMonth();
    const northern = ["winter", "winter", "spring", "spring", "spring", "summer", "summer", "summer", "fall", "fall", "fall", "winter"][month];
    if (setting("WARDROBE_HEMISPHERE", "north").toLowerCase() !== "south") return northern;
    return { winter: "summer", spring: "fall", summer: "winter", fall: "spring" }[northern];
  }

  function stylable(library) {
    const season = currentSeason();
    return library.filter((item) => !item.inLaundry && !item.wishlist && (!item.seasons?.length || item.seasons.includes(season)));
  }

  async function loadOutfits() {
    try { return JSON.parse(await readFile(outfitsFile, "utf8")); }
    catch (error) { if (error.code === "ENOENT") return []; throw error; }
  }

  async function loadLibrary() {
    try { return JSON.parse(await readFile(libraryFile, "utf8")); }
    catch (error) { if (error.code === "ENOENT") return []; throw error; }
  }

  async function updateOutfit(id, patch) {
    const records = await loadOutfits();
    const index = records.findIndex((record) => record.id === id);
    if (index === -1) return null;
    records[index] = { ...records[index], ...patch, updatedAt: new Date().toISOString() };
    await atomicJson(outfitsFile, records);
    return records[index];
  }

  async function loadProfile() {
    try { return JSON.parse(await readFile(profileFile, "utf8")); }
    catch (error) { if (error.code === "ENOENT") return { styleNotes: "", hardRules: "" }; throw error; }
  }

  function garmentFile(item) {
    const name = path.basename(new URL(item.image, "http://localhost").pathname);
    return path.join(importedDir, name);
  }

  async function garmentReference(item, name) {
    return { data: await readFile(garmentFile(item)), mime: "image/png", name: `${name}.png` };
  }

  async function modelReference() {
    const referencePath = path.resolve(root, setting("WARDROBE_MODEL_REFERENCE", "data/model-reference.png"));
    try {
      return { data: await readFile(referencePath), mime: "image/png", name: "model.png" };
    } catch (error) {
      if (error.code === "ENOENT") throw new Error(`Model reference not found at ${referencePath}. Set WARDROBE_MODEL_REFERENCE or add data/model-reference.png.`);
      throw error;
    }
  }

  async function setupStatus() {
    const imageProvider = setting("WARDROBE_IMAGE_PROVIDER", "openai").toLowerCase();
    const hasOpenAIKey = Boolean((imageProvider === "gemini" ? setting("GEMINI_API_KEY") : setting("OPENAI_API_KEY")).trim());
    const provider = resolveStylistProvider(setting);
    const hasStylistKey = provider === "anthropic" ? Boolean(setting("ANTHROPIC_API_KEY").trim()) : Boolean(setting("OPENAI_API_KEY").trim());
    let hasModelReference = false;
    try {
      hasModelReference = (await stat(path.resolve(root, setting("WARDROBE_MODEL_REFERENCE", "data/model-reference.png")))).isFile();
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    const library = await loadLibrary();
    const tops = library.filter((item) => item.part === "upperbody").length;
    const bottoms = library.filter((item) => item.part === "lowerbody").length;
    return {
      ready: hasOpenAIKey && hasStylistKey && hasModelReference && tops > 0 && bottoms > 0,
      hasOpenAIKey,
      hasStylistKey,
      hasModelReference,
      stylistProvider: provider,
      tops,
      bottoms,
    };
  }

  async function generate(id) {
    if (running.has(id)) return running.get(id);
    const task = (async () => {
      try {
        const records = await loadOutfits();
        const record = records.find((entry) => entry.id === id);
        if (!record) return;
        const library = await loadLibrary();
        const byId = new Map(library.map((item) => [item.id, item]));

        let outfit = null;
        const reusable = record.garmentIds?.length >= 2 && record.garmentIds.every((garmentId) => byId.has(garmentId));
        if (reusable) {
          const roles = { upperbody: "top", lowerbody: "bottom", wholebody_up: "outer", shoes: "shoes", accessories_up: "accessory" };
          outfit = { name: record.name, occasion: record.occasion, reason: record.reason, setting: record.setting, top: null, bottom: null, outer: null, shoes: null, accessory: null };
          for (const garmentId of record.garmentIds) {
            const item = byId.get(garmentId);
            const role = roles[item.part];
            if (role && !outfit[role]) outfit[role] = item;
          }
          if (!outfit.top || !outfit.bottom) outfit = null;
        }

        if (!outfit) {
          await updateOutfit(id, { status: "curating", error: null });
          const usedCombinations = records
            .filter((entry) => entry.id !== id && entry.garmentIds?.length >= 2)
            .map((entry) => entry.garmentIds.slice(0, 2).map((garmentId) => byId.get(garmentId)?.name || garmentId));
          const available = stylable(library);
          if (!available.some((item) => item.part === "upperbody") || !available.some((item) => item.part === "lowerbody")) {
            throw new Error("Not enough wearable pieces: at least one in-season top and bottom must be out of the laundry (wishlist items don't count).");
          }
          const feedback = records
            .filter((entry) => entry.id !== id && entry.verdict && entry.garmentIds?.length)
            .slice(-8)
            .map((entry) => ({ name: entry.name, verdict: entry.verdict, reason: entry.verdictReason || "", garments: entry.garmentIds.map((garmentId) => byId.get(garmentId)?.name || garmentId) }));
          const profile = await loadProfile();
          const visualSetting = setting("WARDROBE_VISUAL_STYLIST", "auto");
          let itemImages = [];
          if (visualSetting !== "off" && available.length <= VISUAL_STYLIST_MAX_ITEMS) {
            try {
              itemImages = await Promise.all(available.map(async (item) => ({
                data: await sharp(await readFile(garmentFile(item))).resize(96, 96, { fit: "inside" }).png().toBuffer(),
                mime: "image/png",
              })));
            } catch {
              itemImages = [];
            }
          }
          let inspirationImage = null;
          if (record.inspiration) {
            try {
              inspirationImage = { data: await readFile(path.join(inspirationDir, `${record.id}.png`)), mime: "image/png" };
            } catch {}
          }
          let extraInstruction = null;
          if (record.remix?.keepIds?.length) {
            const keepNames = record.remix.keepIds.map((garmentId) => byId.get(garmentId)?.name || garmentId);
            extraInstruction = `Remix constraint: keep exactly these already-chosen items in the outfit: ${keepNames.join(", ")}. Replace ONLY the ${record.remix.swapSlot} with a different ${record.remix.swapSlot} than "${byId.get(record.remix.swapId)?.name || "the previous one"}". Do not change any kept item.`;
          }
          outfit = await curateOutfit({ setting, items: available, direction: record.direction, mood: record.mood, usedCombinations, feedback, profile, itemImages, inspirationImage, extraInstruction });
          if (record.remix?.keepIds?.length) {
            const chosen = new Set([outfit.top, outfit.bottom, outfit.outer, outfit.shoes, outfit.accessory].filter(Boolean).map((item) => item.id));
            if (!record.remix.keepIds.every((garmentId) => chosen.has(garmentId)) || chosen.has(record.remix.swapId)) {
              throw new Error("The stylist could not produce a valid swap. Try again or generate a fresh look.");
            }
          }
          await updateOutfit(id, {
            name: outfit.name,
            occasion: outfit.occasion,
            reason: outfit.reason,
            setting: outfit.setting,
            garmentIds: [outfit.top, outfit.bottom, outfit.outer, outfit.shoes, outfit.accessory].filter(Boolean).map((item) => item.id),
          });
        }

        await updateOutfit(id, { status: "rendering", error: null });
        await checkImageBudget(setting);
        const images = [await modelReference(), await garmentReference(outfit.top, "top"), await garmentReference(outfit.bottom, "bottom")];
        if (outfit.outer) images.push(await garmentReference(outfit.outer, "outer"));
        if (outfit.shoes) images.push(await garmentReference(outfit.shoes, "shoes"));
        if (outfit.accessory) images.push(await garmentReference(outfit.accessory, "accessory"));
        const bytes = await imageEdit({
          setting,
          modelSetting: "OPENAI_OUTFIT_MODEL",
          size: "1024x1024",
          images,
          prompt: buildOutfitImagePrompt(outfit),
        });
        await mkdir(outfitImagesDir, { recursive: true });
        const fileName = `${id}.png`;
        await writeFile(path.join(outfitImagesDir, fileName), bytes);
        await updateOutfit(id, { status: "ready", image: `${IMAGE_ROOT}/${fileName}`, error: null });
      } catch (error) {
        await updateOutfit(id, { status: "failed", error: error.message });
      }
    })().finally(() => running.delete(id));
    running.set(id, task);
    return task;
  }

  async function handler(req, res, next) {
    const url = new URL(req.url, "http://localhost");
    if (!url.pathname.startsWith(`${API_ROOT}`) && !url.pathname.startsWith("/api/review") && url.pathname !== "/api/profile" && url.pathname !== "/api/usage" && url.pathname !== "/api/capsule") return next();
    try {
      if (url.pathname === "/api/profile" && req.method === "GET") {
        return json(res, 200, await loadProfile());
      }
      if (url.pathname === "/api/profile" && req.method === "PUT") {
        const input = await body(req);
        const profile = {
          styleNotes: typeof input.styleNotes === "string" ? input.styleNotes.trim().slice(0, 2000) : "",
          hardRules: typeof input.hardRules === "string" ? input.hardRules.trim().slice(0, 2000) : "",
        };
        await atomicJson(profileFile, profile);
        return json(res, 200, profile);
      }
      const reviewMatch = url.pathname.match(/^\/api\/review\/week(\/collage\.png)?$/);
      if (reviewMatch && req.method === "GET") {
        const offset = Math.max(0, Math.min(52, Math.round(Number(url.searchParams.get("offset")) || 0)));
        const now = new Date();
        const monday = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
        monday.setUTCDate(monday.getUTCDate() - ((monday.getUTCDay() + 6) % 7) - (offset * 7));
        const weekDates = Array.from({ length: 7 }, (_, index) => {
          const date = new Date(monday);
          date.setUTCDate(monday.getUTCDate() + index);
          return date.toISOString().slice(0, 10);
        });
        const records = await loadOutfits();
        const library = await loadLibrary();
        const byId = new Map(library.map((item) => [item.id, item]));
        const days = weekDates.map((date) => ({
          date,
          weekday: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][(new Date(`${date}T00:00:00Z`).getUTCDay() + 6) % 7],
          looks: records
            .filter((record) => (record.wornAt || []).some((worn) => worn.slice(0, 10) === date))
            .map((record) => ({ id: record.id, name: record.name, mood: record.mood, image: record.image })),
        }));
        const pieceCounts = new Map();
        const moods = new Map();
        for (const day of days) {
          for (const look of day.looks) {
            const record = records.find((entry) => entry.id === look.id);
            if (look.mood) moods.set(look.mood, (moods.get(look.mood) || 0) + 1);
            for (const garmentId of record?.garmentIds || []) {
              pieceCounts.set(garmentId, (pieceCounts.get(garmentId) || 0) + 1);
            }
          }
        }
        const topPieceEntry = [...pieceCounts.entries()].sort((a, b) => b[1] - a[1])[0];
        const stats = {
          daysDressed: days.filter((day) => day.looks.length).length,
          totalWears: days.reduce((total, day) => total + day.looks.length, 0),
          topPiece: topPieceEntry ? { name: byId.get(topPieceEntry[0])?.name || "a piece", count: topPieceEntry[1] } : null,
          moods: [...moods.entries()].sort((a, b) => b[1] - a[1]).map(([mood]) => mood).slice(0, 3),
        };
        if (!reviewMatch[1]) {
          return json(res, 200, { start: weekDates[0], end: weekDates[6], offset, days, stats });
        }

        // Shareable weekly collage
        const TILE = 244, GAP = 12, TOP1 = 226, TOP2 = 546, LABEL = 26;
        const positions = [
          ...Array.from({ length: 4 }, (_, index) => ({ left: 34 + index * (TILE + GAP), top: TOP1 })),
          ...Array.from({ length: 3 }, (_, index) => ({ left: 34 + Math.round((TILE + GAP) / 2) + index * (TILE + GAP), top: TOP2 })),
        ];
        const composites = [];
        const escape = (text) => String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        let svgParts = [];
        for (const [index, day] of days.entries()) {
          const { left, top } = positions[index];
          const look = day.looks[0];
          let placed = false;
          if (look) {
            try {
              const bytes = await sharp(path.join(outfitImagesDir, `${look.id}.png`)).resize(TILE, TILE, { fit: "cover" }).png().toBuffer();
              composites.push({ input: bytes, left, top });
              placed = true;
            } catch {}
          }
          if (!placed) {
            svgParts.push(`<rect x="${left}" y="${top}" width="${TILE}" height="${TILE}" fill="#fdfcf9" stroke="#e0d9ca"/>`);
            svgParts.push(`<text x="${left + TILE / 2}" y="${top + TILE / 2 + 8}" text-anchor="middle" font-family="Georgia,serif" font-size="26" fill="#c9c1b0">&#8212;</text>`);
          }
          svgParts.push(`<text x="${left + TILE / 2}" y="${top + TILE + LABEL}" text-anchor="middle" font-size="13" letter-spacing="3" fill="#71695c">${day.weekday.slice(0, 3).toUpperCase()}</text>`);
        }
        const pretty = (date) => new Date(`${date}T00:00:00Z`).toLocaleDateString("en-US", { month: "long", day: "numeric", timeZone: "UTC" });
        const range = `${pretty(weekDates[0])} — ${pretty(weekDates[6])}, ${weekDates[6].slice(0, 4)}`;
        const summary = stats.totalWears
          ? `${stats.daysDressed} of 7 days dressed${stats.topPiece ? ` · most worn: ${escape(stats.topPiece.name)}` : ""}${stats.moods.length ? ` · felt ${escape(stats.moods.join(", "))}` : ""}`
          : "No looks logged this week";
        const chrome = `<svg width="1080" height="1350">
          <text x="540" y="84" text-anchor="middle" font-size="12" letter-spacing="6" fill="#9d7b4f">THE WEEK IN LOOKS</text>
          <text x="540" y="150" text-anchor="middle" font-family="Georgia,serif" font-size="52" letter-spacing="3" fill="#16130e">${range}</text>
          <rect x="512" y="176" width="56" height="1" fill="#9d7b4f"/>
          ${svgParts.join("\n")}
          <text x="540" y="920" text-anchor="middle" font-family="Georgia,serif" font-size="24" font-style="italic" fill="#71695c">${summary}</text>
          <rect x="512" y="1256" width="56" height="1" fill="#9d7b4f"/>
          <text x="540" y="1298" text-anchor="middle" font-size="14" letter-spacing="6" fill="#16130e">FASHION INTEL</text>
        </svg>`;
        const collage = await sharp({ create: { width: 1080, height: 1350, channels: 3, background: { r: 248, g: 245, b: 239 } } })
          .composite([...composites, { input: Buffer.from(chrome), left: 0, top: 0 }])
          .png()
          .toBuffer();
        res.setHeader("Content-Type", "image/png");
        res.setHeader("Content-Disposition", `inline; filename="fashion-intel-week-${weekDates[0]}.png"`);
        res.setHeader("Cache-Control", "no-store");
        return res.end(collage);
      }
      if (url.pathname === "/api/capsule" && req.method === "POST") {
        const input = await body(req);
        const days = Math.max(1, Math.min(21, Math.round(Number(input.days) || 0)));
        if (!Number.isFinite(Number(input.days)) || !Number(input.days)) return json(res, 400, { error: "Tell me how many days the trip is." });
        const destination = typeof input.destination === "string" ? input.destination.trim().slice(0, 120) : "";
        const notes = typeof input.notes === "string" ? input.notes.trim().slice(0, 400) : "";
        const library = await loadLibrary();
        const available = stylable(library);
        if (available.length < 2) return json(res, 400, { error: "Import a few wearable pieces first." });
        const plan = await planCapsule({ setting, items: available, days, destination, notes });
        return json(res, 200, plan);
      }
      if (url.pathname === "/api/usage" && req.method === "GET") {
        const budget = Number.parseFloat(setting("WARDROBE_DAILY_BUDGET"));
        return json(res, 200, { today: await usageToday(), budgetUsd: Number.isFinite(budget) && budget > 0 ? budget : null });
      }
      if (url.pathname === API_ROOT && req.method === "GET") {
        return json(res, 200, await loadOutfits());
      }
      if (url.pathname === `${API_ROOT}/config` && req.method === "GET") {
        return json(res, 200, await setupStatus());
      }
      const imageMatch = url.pathname.match(/^\/api\/outfits\/images\/([\w.-]+)$/i);
      if (imageMatch && req.method === "GET") {
        const file = path.join(outfitImagesDir, path.basename(imageMatch[1]));
        await stat(file);
        res.setHeader("Content-Type", "image/png");
        res.setHeader("Cache-Control", "no-store");
        return res.end(await readFile(file));
      }
      if (url.pathname === API_ROOT && req.method === "POST") {
        const status = await setupStatus();
        if (!status.ready) {
          const missing = [
            !status.hasOpenAIKey && "OPENAI_API_KEY in .env",
            !status.hasStylistKey && "an API key for the stylist provider",
            !status.hasModelReference && "a PNG photo of yourself at data/model-reference.png",
            (!status.tops || !status.bottoms) && "at least one imported top and one bottom",
          ].filter(Boolean).join(", ");
          return json(res, 503, { error: `Setup required: add ${missing}.` });
        }
        if (running.size >= MAX_CONCURRENT_GENERATIONS) {
          return json(res, 429, { error: `Up to ${MAX_CONCURRENT_GENERATIONS} looks can generate at once. Give the current ones a moment to finish.` });
        }
        const input = await body(req, 12 * 1024 * 1024);
        const direction = typeof input.direction === "string" ? input.direction.trim().slice(0, 500) || null : null;
        const mood = typeof input.mood === "string" ? input.mood.trim().slice(0, 120) || null : null;
        const inspiration = decodeDataUrl(input.inspirationDataUrl);
        const now = new Date().toISOString();
        const record = {
          id: randomUUID(),
          name: "New look",
          occasion: [],
          reason: "",
          setting: "",
          mood,
          direction,
          inspiration: Boolean(inspiration),
          garmentIds: [],
          image: null,
          favorite: false,
          verdict: null,
          verdictReason: null,
          wornAt: [],
          status: "curating",
          error: null,
          createdAt: now,
          updatedAt: now,
        };
        if (inspiration) {
          await mkdir(inspirationDir, { recursive: true });
          await writeFile(path.join(inspirationDir, `${record.id}.png`), await sharp(inspiration.data).rotate().resize(1024, 1024, { fit: "inside", withoutEnlargement: true }).png().toBuffer(), { mode: 0o600 });
        }
        const records = await loadOutfits();
        await atomicJson(outfitsFile, [...records, record]);
        void generate(record.id);
        return json(res, 202, record);
      }
      const match = url.pathname.match(/^\/api\/outfits\/([a-f0-9-]{36})(?:\/(retry|wear|remix|card\.png))?$/i);
      if (!match) return json(res, 404, { error: "Not found" });
      const records = await loadOutfits();
      const record = records.find((entry) => entry.id === match[1]);
      if (!record) return json(res, 404, { error: "Outfit not found" });
      if (match[2] === "retry" && req.method === "POST") {
        if (["curating", "rendering"].includes(record.status) && running.has(record.id)) {
          return json(res, 409, { error: "This outfit is already generating" });
        }
        const updated = await updateOutfit(record.id, { status: record.garmentIds?.length ? "rendering" : "curating", error: null });
        void generate(record.id);
        return json(res, 202, updated);
      }
      if (match[2] === "card.png" && req.method === "GET") {
        if (record.status !== "ready" || !record.image) return json(res, 409, { error: "This look has no rendered photo yet." });
        const library = await loadLibrary();
        const byId = new Map(library.map((item) => [item.id, item]));
        const photo = await sharp(path.join(outfitImagesDir, `${record.id}.png`)).resize(1000, 1000, { fit: "cover" }).png().toBuffer();
        const thumbs = [];
        for (const garmentId of (record.garmentIds || []).slice(0, 6)) {
          const item = byId.get(garmentId);
          if (!item) continue;
          try {
            thumbs.push(await sharp(await readFile(garmentFile(item))).resize(120, 120, { fit: "inside" }).png().toBuffer());
          } catch {}
        }
        const escape = (text) => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        const label = `<svg width="1080" height="1400"><style>text{font-family:Helvetica,Arial,sans-serif;fill:#191919}</style><text x="40" y="1245" font-size="42" font-weight="600">${escape(record.name)}</text><text x="40" y="1290" font-size="24" fill="#66625d">${escape([record.mood ? `feels ${record.mood}` : "", ...(record.occasion || [])].filter(Boolean).join(" · "))}</text><text x="40" y="1360" font-size="20" letter-spacing="4" fill="#66625d">FASHION INTEL</text></svg>`;
        const composites = [
          { input: photo, left: 40, top: 40 },
          ...thumbs.map((thumb, index) => ({ input: thumb, left: 40 + (index * 140), top: 1080 })),
          { input: Buffer.from(label), left: 0, top: 0 },
        ];
        const card = await sharp({ create: { width: 1080, height: 1400, channels: 3, background: { r: 244, g: 240, b: 232 } } })
          .composite(composites)
          .png()
          .toBuffer();
        res.setHeader("Content-Type", "image/png");
        res.setHeader("Content-Disposition", `inline; filename="fashion-intel-${record.id.slice(0, 8)}.png"`);
        res.setHeader("Cache-Control", "no-store");
        return res.end(card);
      }
      if (match[2] === "remix" && req.method === "POST") {
        if (record.status !== "ready" || !record.garmentIds?.length) return json(res, 409, { error: "Only a finished look can be remixed." });
        if (running.size >= MAX_CONCURRENT_GENERATIONS) return json(res, 429, { error: "Give the current generations a moment to finish." });
        const input = await body(req);
        const slotParts = { top: "upperbody", bottom: "lowerbody", outer: "wholebody_up", shoes: "shoes", accessory: "accessories_up" };
        const slot = slotParts[input.slot] ? input.slot : null;
        if (!slot) return json(res, 400, { error: "Pick which piece to swap: top, bottom, outer, shoes, or accessory." });
        const library = await loadLibrary();
        const byId = new Map(library.map((item) => [item.id, item]));
        const swapId = (record.garmentIds || []).find((garmentId) => byId.get(garmentId)?.part === slotParts[slot]);
        if (!swapId) return json(res, 400, { error: `This look has no ${slot} to swap.` });
        const keepIds = record.garmentIds.filter((garmentId) => garmentId !== swapId);
        const now = new Date().toISOString();
        const remixRecord = {
          id: randomUUID(),
          name: `${record.name} (new ${slot})`,
          occasion: record.occasion || [],
          reason: "",
          setting: "",
          mood: record.mood || null,
          direction: record.direction || null,
          inspiration: false,
          remix: { keepIds, swapId, swapSlot: slot },
          garmentIds: [],
          image: null,
          favorite: false,
          verdict: null,
          verdictReason: null,
          wornAt: [],
          status: "curating",
          error: null,
          createdAt: now,
          updatedAt: now,
        };
        const allRecords = await loadOutfits();
        await atomicJson(outfitsFile, [...allRecords, remixRecord]);
        void generate(remixRecord.id);
        return json(res, 202, remixRecord);
      }
      if (match[2] === "wear" && req.method === "POST") {
        const wornAt = [...(record.wornAt || []), new Date().toISOString()];
        return json(res, 200, await updateOutfit(record.id, { wornAt }));
      }
      if (!match[2] && req.method === "PATCH") {
        const input = await body(req);
        const patch = {};
        if (typeof input.favorite === "boolean") patch.favorite = input.favorite;
        if (typeof input.name === "string" && input.name.trim()) patch.name = input.name.trim().slice(0, 80);
        if (input.verdict === null || ["up", "down"].includes(input.verdict)) {
          patch.verdict = input.verdict;
          patch.verdictReason = input.verdict && typeof input.verdictReason === "string" ? input.verdictReason.trim().slice(0, 120) || null : null;
        }
        if (!Object.keys(patch).length) return json(res, 400, { error: "Nothing to update" });
        return json(res, 200, await updateOutfit(record.id, patch));
      }
      if (!match[2] && req.method === "DELETE") {
        await atomicJson(outfitsFile, records.filter((entry) => entry.id !== record.id));
        await rm(path.join(outfitImagesDir, `${record.id}.png`), { force: true });
        await rm(path.join(inspirationDir, `${record.id}.png`), { force: true });
        return json(res, 200, { deleted: true, id: record.id });
      }
      if (!match[2] && req.method === "GET") return json(res, 200, record);
      return json(res, 404, { error: "Not found" });
    } catch (error) {
      const statusCode = error.code === "ENOENT" ? 404 : error.status || 500;
      return json(res, statusCode, { error: statusCode === 500 ? error.message || "Internal server error" : error.message });
    }
  }

  return {
    name: "wardrobe-outfit-studio-api",
    apply: "serve",
    async configResolved(config) {
      root = config.root;
      const dataDir = path.resolve(root, setting("WARDROBE_DATA_DIR", "data"));
      outfitsFile = path.join(dataDir, "outfits.json");
      outfitImagesDir = path.join(dataDir, "outfit-images");
      inspirationDir = path.join(dataDir, "inspiration");
      profileFile = path.join(dataDir, "profile.json");
      libraryFile = path.join(dataDir, "library.json");
      importedDir = path.join(dataDir, "imported");
      await mkdir(dataDir, { recursive: true });
      initTelemetry(dataDir);
      const records = await loadOutfits();
      const interrupted = records.filter((record) => ["curating", "rendering"].includes(record.status));
      for (const record of interrupted) {
        await updateOutfit(record.id, { status: "failed", error: "Generation was interrupted by a restart. Retry to continue." });
      }
    },
    configureServer(server) { server.middlewares.use(handler); },
    configurePreviewServer(server) { server.middlewares.use(handler); },
  };
}
