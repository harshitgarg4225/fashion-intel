import { structuredAnalysis } from "./ai-providers.mjs";

const CURATION_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    name: { type: "string" },
    occasion: { type: "array", items: { type: "string" }, maxItems: 3 },
    reason: { type: "string" },
    setting: { type: "string" },
    topId: { type: "string" },
    bottomId: { type: "string" },
    outerId: { anyOf: [{ type: "string" }, { type: "null" }] },
    shoesId: { anyOf: [{ type: "string" }, { type: "null" }] },
    accessoryId: { anyOf: [{ type: "string" }, { type: "null" }] },
  },
  required: ["name", "occasion", "reason", "setting", "topId", "bottomId", "outerId", "shoesId", "accessoryId"],
};

const PART_LABELS = {
  upperbody: "top",
  wholebody_up: "outer layer",
  lowerbody: "bottom",
  accessories_up: "accessory",
  shoes: "shoes",
};

function describeItem(item) {
  const colors = [item.color, item.secondaryColor].filter(Boolean).join(" / ");
  const tags = (item.tags || []).join(", ");
  return `- id: ${item.id} | ${PART_LABELS[item.part] || item.part} | ${item.name}${colors ? ` | colors ${colors}` : ""}${tags ? ` | ${tags}` : ""}`;
}

const MOOD_GUIDANCE = `
The user told you how they want to FEEL today. Your job is to translate that feeling into concrete garment choices — this matters more than any generic styling rule. Examples of translation:
- confident / powerful → structure and sharpness: strong shoulders, darker or saturated dominant color, cleaner lines, higher contrast
- relaxed / effortless → softness and ease: relaxed silhouettes, washed and natural fabrics, low contrast, comfortable layers
- playful / fun → the wardrobe's boldest color, graphic, or pattern as the statement piece, lighter overall energy
- cozy / comforted → texture first: knits, fleece, soft layers, warm tones
- romantic / soft → gentler tones, drape, delicate details
- sharp / put-together → the most polished pieces: tailoring, crisp fabrics, deliberate color discipline
- bold / seen → maximum statement the wardrobe supports, worn with conviction
- grounded / calm → earth tones, naturals, quiet minimal pieces
Pick real items that genuinely embody the feeling; explain the outfit in terms of the feeling in your reason.
`;

export function buildCurationPrompt({ items, direction, mood, usedCombinations }) {
  const inventory = items.map(describeItem).join("\n");
  const used = usedCombinations.length
    ? `\nAlready-created combinations to avoid repeating (top+bottom pairs):\n${usedCombinations.map((combo) => `- ${combo.join(" + ")}`).join("\n")}\n`
    : "";
  const feeling = mood ? `\nThe user wants to feel: ${mood}.\n${MOOD_GUIDANCE}` : "";
  const brief = direction ? `\nAdditional context from the user (occasion, weather, constraints): ${direction}\n` : "";

  return `You are an expert personal stylist curating one complete outfit from a real wardrobe.

Wardrobe inventory (each line is one garment):
${inventory}
${used}${feeling}${brief}
Curate exactly one outfit:
- Choose exactly one top (a "top" item) as topId and exactly one bottom (a "bottom" item) as bottomId.
- Optionally add one outer layer, one pair of shoes, and one restrained accessory when they genuinely improve the look; otherwise return null for those fields.
- Use only ids that appear in the inventory, in their listed roles.
- Favor tonal or analogous color harmony; use complementary contrast selectively with one dominant piece.
- Let one graphic, pattern, texture, or saturated piece carry the statement; balance visual weight and silhouette.
- Keep layered looks physically plausible.
- Do not repeat an already-created top+bottom combination.
- Give the outfit a short evocative name, 1-3 lowercase occasion labels (e.g. smart-casual, weekend, office), one sentence explaining why the combination works, and a restrained real-world photo setting description (e.g. "a quiet warm-stone courtyard with restrained greenery").`;
}

export async function curateOutfit({ setting, items, direction, mood, usedCombinations = [] }) {
  const result = await structuredAnalysis({
    setting,
    prompt: buildCurationPrompt({ items, direction, mood, usedCombinations }),
    schema: CURATION_SCHEMA,
    schemaName: "curated_outfit",
  });

  const byId = new Map(items.map((item) => [item.id, item]));
  const pick = (id, parts) => {
    if (!id || typeof id !== "string") return null;
    const item = byId.get(id);
    return item && parts.includes(item.part) ? item : null;
  };

  const top = pick(result.topId, ["upperbody"]);
  const bottom = pick(result.bottomId, ["lowerbody"]);
  if (!top || !bottom) throw new Error("The stylist did not select a valid top and bottom from the wardrobe");

  const outer = pick(result.outerId, ["wholebody_up"]);
  const shoes = pick(result.shoesId, ["shoes"]);
  const accessory = pick(result.accessoryId, ["accessories_up"]);

  return {
    name: String(result.name || "New look").trim().slice(0, 80) || "New look",
    occasion: Array.isArray(result.occasion) ? result.occasion.filter((label) => typeof label === "string").map((label) => label.trim().toLowerCase().slice(0, 30)).filter(Boolean).slice(0, 3) : [],
    reason: String(result.reason || "").trim().slice(0, 400),
    setting: String(result.setting || "a quiet neutral daylight interior").trim().slice(0, 200),
    top,
    bottom,
    outer,
    shoes,
    accessory,
  };
}

export function buildOutfitImagePrompt(outfit) {
  const references = [
    "Image 1: identity reference for the exact person to preserve.",
    "Image 2: exact top garment reference.",
    "Image 3: exact bottom garment reference.",
  ];
  let index = 4;
  if (outfit.outer) references.push(`Image ${index++}: exact outer-layer reference. Preserve its real construction and closure exactly; never invent a zipper, buttons, placket, or opening.`);
  if (outfit.shoes) references.push(`Image ${index++}: exact shoe reference.`);
  if (outfit.accessory) references.push(`Image ${index++}: exact accessory reference.`);

  const dressed = [
    "Dress them in the exact top and bottom references",
    outfit.outer ? " plus the exact outer-layer reference" : "",
    outfit.shoes || outfit.accessory ? " and the exact selected shoes/accessory references" : "",
    ".",
  ].join("");

  const layeredClause = outfit.outer
    ? "\n\nLayered-look clause: Layer the exact inner top and outer layer naturally so both remain visibly identifiable. First inspect the outer reference. If it has a real full front button or zipper closure, it may be worn naturally open or partly open using only that closure. If it is a pullover or has no full front opening, keep it closed exactly as designed and reveal the inner top only at its real collar or neckline, sleeve or cuff edge, or a natural 2-4 cm untucked hem below the outer layer. Never invent, add, split, unzip, unbutton, or simulate a closure. Keep the outer garment at its true length even when it overlaps the waistband."
    : "";

  return `Use case: identity-preserve
Asset type: square outfit gallery photograph

${references.join("\n")}

Primary request: Create a professional square editorial fashion photograph of the person from Image 1 wearing all of the exact referenced garments, and only those garments.

Outfit: ${outfit.name}
Scene/backdrop: ${outfit.setting}.

Subject: Preserve the same person's recognizable face, hair, age, build, skin texture, and body proportions. ${dressed} Plain understated shoes and invisible basics such as socks are allowed only where needed when no shoe reference is provided. Do not add, replace, or invent any other visible clothing or accessory.

Style/medium: Photorealistic natural editorial fashion campaign with authentic skin and fabric texture and no synthetic AI polish.

Composition/framing: Square 1:1 image. Show the complete person and outfit from head through shoes. Keep the person centered and occupying most of the frame with modest breathing room. Use a relaxed, mostly front-facing pose with arms away from the torso so every item remains readable.

Lighting/mood: Warm professional natural light, realistic shadows, and restrained editorial color grading.

Garment fidelity: Preserve every referenced garment precisely: color, material, fit, construction, pattern, graphics, logos, text, proportions, distinctive details, and real closure construction. Keep the top and bottom recognizable without changing their natural length, tuck, or construction.${layeredClause}

Avoid: Completely hidden selected garments, invented zippers, buttons, openings or plackets, unnatural layering, extra layers, hats, bags, scarves, jewelry, visible unreferenced undershirts, crossed arms, hands blocking clothing, garment redesign, changed logos or text, cropped feet, extra people, text overlays, watermarks, studio cutout appearance, or synthetic AI polish.`;
}
