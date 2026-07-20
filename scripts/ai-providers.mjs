const ANTHROPIC_VERSION = "2023-06-01";

export function resolveStylistProvider(setting) {
  const explicit = (setting("WARDROBE_STYLIST_PROVIDER") || "").trim().toLowerCase();
  if (explicit === "anthropic" || explicit === "openai") return explicit;
  if (setting("ANTHROPIC_API_KEY").trim()) return "anthropic";
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

function looseSchema(schema) {
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

export async function structuredAnalysis({ setting, prompt, images = [], schema, schemaName }) {
  const provider = resolveStylistProvider(setting);
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
