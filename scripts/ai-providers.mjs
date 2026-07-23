import { audit, recordUsage } from "./telemetry.mjs";

const ANTHROPIC_VERSION = "2023-06-01";

export function resolveStylistProvider(setting) {
  const explicit = (setting("WARDROBE_STYLIST_PROVIDER") || "").trim().toLowerCase();
  if (["anthropic", "openai", "gemini"].includes(explicit)) return explicit;
  if (setting("ANTHROPIC_API_KEY").trim()) return "anthropic";
  if (setting("OPENAI_API_KEY").trim()) return "openai";
  if (setting("GEMINI_API_KEY").trim()) return "gemini";
  return "openai";
}

async function openAIStructured({ key, baseUrl, model, prompt, images = [], schema, schemaName }) {
  const content = [
    { type: "input_text", text: prompt },
    ...images.map((image) => ({ type: "input_image", image_url: `data:${image.mime};base64,${image.data.toString("base64")}` })),
  ];
  const response = await fetch(`${baseUrl}/responses`, {
    method: "POST",
    headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      model,
      input: [{ role: "user", content }],
      text: { format: { type: "json_schema", name: schemaName, strict: true, schema } },
    }),
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(result.error?.message || `OpenAI analysis failed (${response.status})`);
  const outputText = result.output_text || result.output?.flatMap((item) => item.content || []).find((item) => item.type === "output_text")?.text;
  if (!outputText) throw new Error("OpenAI analysis returned no structured result");
  return JSON.parse(outputText);
}

export function looseSchema(schema) {
  if (Array.isArray(schema)) return schema.map(looseSchema);
  if (!schema || typeof schema !== "object") return schema;
  const copy = {};
  for (const [key, value] of Object.entries(schema)) {
    if (key === "additionalProperties") continue;
    copy[key] = looseSchema(value);
  }
  return copy;
}

async function anthropicStructured({ key, baseUrl, model, prompt, images = [], schema, schemaName }) {
  const content = [
    ...images.map((image) => ({ type: "image", source: { type: "base64", media_type: image.mime, data: image.data.toString("base64") } })),
    { type: "text", text: prompt },
  ];
  const response = await fetch(`${baseUrl}/messages`, {
    method: "POST",
    headers: { "x-api-key": key, "anthropic-version": ANTHROPIC_VERSION, "Content-Type": "application/json" },
    body: JSON.stringify({
      model,
      max_tokens: 4096,
      messages: [{ role: "user", content }],
      tools: [{ name: schemaName, description: "Record the structured result.", input_schema: looseSchema(schema) }],
      tool_choice: { type: "tool", name: schemaName },
    }),
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(result.error?.message || `Anthropic analysis failed (${response.status})`);
  const toolUse = result.content?.find((block) => block.type === "tool_use");
  if (!toolUse?.input) throw new Error("Anthropic analysis returned no structured result");
  return toolUse.input;
}

// Gemini's responseSchema is an OpenAPI-style subset: no additionalProperties,
// and nullability is expressed with `nullable` rather than anyOf/type arrays.
export function geminiSchema(schema) {
  if (Array.isArray(schema)) return schema.map(geminiSchema);
  if (!schema || typeof schema !== "object") return schema;
  const nullableAnyOf = Array.isArray(schema.anyOf) && schema.anyOf.length === 2 && schema.anyOf.some((entry) => entry?.type === "null");
  if (nullableAnyOf) {
    const real = schema.anyOf.find((entry) => entry?.type !== "null");
    return { ...geminiSchema(real), nullable: true };
  }
  const copy = {};
  for (const [key, value] of Object.entries(schema)) {
    if (key === "additionalProperties" || key === "$schema") continue;
    if (key === "type" && Array.isArray(value)) {
      copy.type = value.find((entry) => entry !== "null") || "string";
      if (value.includes("null")) copy.nullable = true;
      continue;
    }
    copy[key] = geminiSchema(value);
  }
  return copy;
}

async function geminiStructured({ key, baseUrl, model, prompt, images = [], schema }) {
  const parts = [
    ...images.map((image) => ({ inline_data: { mime_type: image.mime, data: image.data.toString("base64") } })),
    { text: prompt },
  ];
  const response = await fetch(`${baseUrl}/models/${model}:generateContent?key=${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ role: "user", parts }],
      generationConfig: { responseMimeType: "application/json", responseSchema: geminiSchema(schema) },
    }),
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(result.error?.message || `Gemini analysis failed (${response.status})`);
  const text = result.candidates?.[0]?.content?.parts?.map((part) => part.text || "").join("");
  if (!text) throw new Error("Gemini analysis returned no structured result");
  return JSON.parse(text);
}

export async function structuredAnalysis({ setting, prompt, images = [], schema, schemaName }) {
  const provider = resolveStylistProvider(setting);
  void audit({ type: "vision", provider, schema: schemaName, images: images.length });
  void recordUsage("vision", provider);
  if (provider === "anthropic") {
    const key = setting("ANTHROPIC_API_KEY").trim();
    if (!key) throw new Error("ANTHROPIC_API_KEY is not configured");
    return anthropicStructured({
      key,
      baseUrl: (setting("ANTHROPIC_API_BASE_URL") || "https://api.anthropic.com/v1").replace(/\/$/, ""),
      model: setting("ANTHROPIC_MODEL") || "claude-sonnet-5",
      prompt,
      images,
      schema,
      schemaName,
    });
  }
  if (provider === "gemini") {
    const key = setting("GEMINI_API_KEY").trim();
    if (!key) throw new Error("GEMINI_API_KEY is not configured");
    return geminiStructured({
      key,
      baseUrl: (setting("GEMINI_API_BASE_URL") || "https://generativelanguage.googleapis.com/v1beta").replace(/\/$/, ""),
      model: setting("GEMINI_MODEL") || "gemini-2.5-flash",
      prompt,
      images,
      schema,
    });
  }
  const key = setting("OPENAI_API_KEY").trim();
  if (!key) throw new Error("OPENAI_API_KEY is not configured");
  return openAIStructured({
    key,
    baseUrl: (setting("OPENAI_API_BASE_URL") || "https://api.openai.com/v1").replace(/\/$/, ""),
    model: setting("OPENAI_VISION_MODEL") || "gpt-5.4-mini",
    prompt,
    images,
    schema,
    schemaName,
  });
}
