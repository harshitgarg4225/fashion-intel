import { randomUUID } from "node:crypto";
import { mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { atomicJson, openAIEdit } from "./import-job-api.mjs";
import { curateOutfit, buildOutfitImagePrompt } from "./stylist.mjs";
import { resolveStylistProvider } from "./ai-providers.mjs";

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

export function outfitStudioApi(options = {}) {
  let root;
  let outfitsFile;
  let outfitImagesDir;
  let libraryFile;
  let importedDir;
  const running = new Map();
  const setting = (name, fallback = "") => options.env?.[name] || process.env[name] || fallback;
  const apiBaseUrl = () => setting("OPENAI_API_BASE_URL", "https://api.openai.com/v1").replace(/\/$/, "");

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
    const hasOpenAIKey = Boolean(setting("OPENAI_API_KEY").trim());
    const provider = resolveStylistProvider(setting);
    const hasStylistKey = provider === "anthropic" ? Boolean(setting("ANTHROPIC_API_KEY").trim()) : hasOpenAIKey;
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
          outfit = await curateOutfit({ setting, items: library, direction: record.direction, mood: record.mood, usedCombinations });
          await updateOutfit(id, {
            name: outfit.name,
            occasion: outfit.occasion,
            reason: outfit.reason,
            setting: outfit.setting,
            garmentIds: [outfit.top, outfit.bottom, outfit.outer, outfit.shoes, outfit.accessory].filter(Boolean).map((item) => item.id),
          });
        }

        await updateOutfit(id, { status: "rendering", error: null });
        const key = setting("OPENAI_API_KEY");
        if (!key) throw new Error("OPENAI_API_KEY is not configured");
        const images = [await modelReference(), await garmentReference(outfit.top, "top"), await garmentReference(outfit.bottom, "bottom")];
        if (outfit.outer) images.push(await garmentReference(outfit.outer, "outer"));
        if (outfit.shoes) images.push(await garmentReference(outfit.shoes, "shoes"));
        if (outfit.accessory) images.push(await garmentReference(outfit.accessory, "accessory"));
        const bytes = await openAIEdit({
          key,
          baseUrl: apiBaseUrl(),
          model: setting("OPENAI_OUTFIT_MODEL", setting("OPENAI_IMAGE_MODEL", "gpt-image-2")),
          quality: setting("OPENAI_IMAGE_QUALITY", "high"),
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
    if (!url.pathname.startsWith(`${API_ROOT}`)) return next();
    try {
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
        const input = await body(req);
        const direction = typeof input.direction === "string" ? input.direction.trim().slice(0, 500) || null : null;
        const mood = typeof input.mood === "string" ? input.mood.trim().slice(0, 120) || null : null;
        const now = new Date().toISOString();
        const record = {
          id: randomUUID(),
          name: "New look",
          occasion: [],
          reason: "",
          setting: "",
          mood,
          direction,
          garmentIds: [],
          image: null,
          favorite: false,
          status: "curating",
          error: null,
          createdAt: now,
          updatedAt: now,
        };
        const records = await loadOutfits();
        await atomicJson(outfitsFile, [...records, record]);
        void generate(record.id);
        return json(res, 202, record);
      }
      const match = url.pathname.match(/^\/api\/outfits\/([a-f0-9-]{36})(?:\/(retry))?$/i);
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
      if (!match[2] && req.method === "PATCH") {
        const input = await body(req);
        const patch = {};
        if (typeof input.favorite === "boolean") patch.favorite = input.favorite;
        if (typeof input.name === "string" && input.name.trim()) patch.name = input.name.trim().slice(0, 80);
        if (!Object.keys(patch).length) return json(res, 400, { error: "Nothing to update" });
        return json(res, 200, await updateOutfit(record.id, patch));
      }
      if (!match[2] && req.method === "DELETE") {
        await atomicJson(outfitsFile, records.filter((entry) => entry.id !== record.id));
        await rm(path.join(outfitImagesDir, `${record.id}.png`), { force: true });
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
      libraryFile = path.join(dataDir, "library.json");
      importedDir = path.join(dataDir, "imported");
      await mkdir(dataDir, { recursive: true });
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
