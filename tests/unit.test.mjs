import test from "node:test";
import assert from "node:assert/strict";
import sharp from "sharp";
import { looseSchema } from "../scripts/ai-providers.mjs";
import { buildGarmentPrompt, frameTransparentGarment, hashDistance, imageHash, processChromaBackground } from "../scripts/import-job-api.mjs";
import { buildCurationPrompt, buildOutfitImagePrompt } from "../scripts/stylist.mjs";

const solid = (r, g, b, size = 64) => sharp({ create: { width: size, height: size, channels: 4, background: { r, g, b, alpha: 1 } } }).png().toBuffer();

test("looseSchema strips additionalProperties recursively", () => {
  const schema = {
    type: "object",
    additionalProperties: false,
    properties: { items: { type: "array", items: { type: "object", additionalProperties: false, properties: { x: { type: "integer" } } } } },
  };
  const loose = looseSchema(schema);
  assert.equal(loose.additionalProperties, undefined);
  assert.equal(loose.properties.items.items.additionalProperties, undefined);
  assert.equal(loose.properties.items.items.properties.x.type, "integer");
  assert.equal(schema.additionalProperties, false, "input must not be mutated");
});

test("imageHash is stable and separates distinct images", async () => {
  const navy = await solid(20, 30, 90);
  const navyAgain = await solid(20, 30, 90);
  const cream = await solid(235, 228, 210);
  const a = await imageHash(navy);
  assert.equal(hashDistance(a, await imageHash(navyAgain)), 0);
  assert.ok(hashDistance(a, await imageHash(cream)) > 6, "different colors should not read as duplicates");
});

test("hashDistance handles malformed input as maximally distant", () => {
  assert.equal(hashDistance(null, "abc"), 64);
  assert.equal(hashDistance("ab", "abcd"), 64);
});

test("frameTransparentGarment centers content on a 1024 canvas", async () => {
  const small = await sharp({ create: { width: 100, height: 40, channels: 4, background: { r: 200, g: 40, b: 40, alpha: 1 } } }).png().toBuffer();
  const framed = await frameTransparentGarment(small);
  const meta = await sharp(framed).metadata();
  assert.equal(meta.width, 1024);
  assert.equal(meta.height, 1024);
  const corner = await sharp(framed).extract({ left: 0, top: 0, width: 4, height: 4 }).ensureAlpha().raw().toBuffer();
  assert.equal(corner[3], 0, "corners must be transparent");
});

test("processChromaBackground removes the key and keeps the garment", async () => {
  const garment = await sharp({ create: { width: 200, height: 200, channels: 4, background: { r: 0, g: 255, b: 0, alpha: 1 } } })
    .composite([{ input: await sharp({ create: { width: 80, height: 80, channels: 4, background: { r: 180, g: 30, b: 30, alpha: 1 } } }).png().toBuffer(), left: 60, top: 60 }])
    .png()
    .toBuffer();
  const { bytes, verification } = await processChromaBackground(garment, "#00ff00");
  assert.equal(verification.contaminatedPixels, 0);
  const { data, info } = await sharp(bytes).ensureAlpha().raw().toBuffer({ resolveWithObject: true });
  const center = ((info.height / 2) * info.width + info.width / 2) * 4;
  assert.ok(data[center + 3] > 200, "garment center must stay opaque");
  assert.ok(data[center] > 120 && data[center + 1] < 90, "garment must stay red, not green");
});

test("buildGarmentPrompt embeds the chroma key and metadata", () => {
  const prompt = buildGarmentPrompt({ name: "Navy Tee", part: "upperbody", color: "#142878", tags: ["cotton"] }, "#ff00ff");
  assert.match(prompt, /#ff00ff/);
  assert.match(prompt, /Navy Tee/);
  assert.match(prompt, /cotton/);
});

test("buildOutfitImagePrompt includes the layered clause only with an outer layer", () => {
  const base = { name: "Look", setting: "a plaza", top: {}, bottom: {}, outer: null, shoes: null, accessory: null };
  assert.doesNotMatch(buildOutfitImagePrompt(base), /Layered-look clause/);
  assert.match(buildOutfitImagePrompt({ ...base, outer: {} }), /Layered-look clause/);
});

test("buildCurationPrompt reflects mood, feedback, rules, and visuals", () => {
  const items = [
    { id: "t1", name: "Navy Tee", part: "upperbody", color: "#142878", tags: [] },
    { id: "b1", name: "Chinos", part: "lowerbody", color: "#b4aa96", tags: [] },
  ];
  const prompt = buildCurationPrompt({
    items,
    mood: "confident",
    direction: "office day",
    usedCombinations: [["Navy Tee", "Chinos"]],
    feedback: [{ name: "Old Look", verdict: "down", reason: "too formal", garments: ["Navy Tee", "Chinos"] }],
    profile: { styleNotes: "prefers earth tones", hardRules: "never crop tops" },
    hasItemImages: true,
    hasInspiration: true,
  });
  assert.match(prompt, /wants to feel: confident/);
  assert.match(prompt, /office day/);
  assert.match(prompt, /too formal/);
  assert.match(prompt, /never crop tops/);
  assert.match(prompt, /garment photo 1/);
  assert.match(prompt, /inspiration photo/);
  assert.match(prompt, /Navy Tee \+ Chinos/);
});

test("computeStreak counts consecutive worn days and stays alive before today's wear", async () => {
  const { computeStreak } = await import("../scripts/wear-stats.mjs");
  const outfits = [
    { wornAt: ["2026-07-18T08:00:00Z", "2026-07-19T08:00:00Z", "2026-07-20T08:00:00Z"] },
    { wornAt: ["2026-07-16T09:00:00Z"] },
  ];
  assert.equal(computeStreak(outfits, "2026-07-20"), 3);
  assert.equal(computeStreak(outfits, "2026-07-21"), 3, "streak survives the morning before dressing");
  assert.equal(computeStreak(outfits, "2026-07-23"), 0, "a missed day breaks it");
  assert.equal(computeStreak([], "2026-07-20"), 0);
});
