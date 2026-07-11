"""
Stylist — a personal AI shopping assistant.

Single-service deployment (Railway):
  - FastAPI serves the static frontend and one streaming chat endpoint.
  - Stateless: conversation history and the user's style profile live in the
    browser (localStorage) and are sent with each request. No database.
  - Product links are retailer search deep-links built client-side — no
    catalog or affiliate API costs.

Environment variables:
  ANTHROPIC_API_KEY     required — console.anthropic.com
  CLAUDE_MODEL          optional — default "claude-haiku-4-5" (cheapest).
                        Set "claude-sonnet-5" or "claude-opus-4-8" for
                        higher-quality styling at higher cost.
  MAX_TOKENS            optional — per-reply output cap (default 2048)
  RATE_LIMIT_PER_HOUR   optional — per-IP request cap (default 40)
"""

import json
import os
import time
from collections import defaultdict, deque
from pathlib import Path

import anthropic
from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5")
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "2048"))
RATE_LIMIT_PER_HOUR = int(os.environ.get("RATE_LIMIT_PER_HOUR", "40"))
HISTORY_LIMIT = 24          # most-recent messages kept per request
MAX_MESSAGE_CHARS = 4000    # per-message input cap

STATIC_DIR = Path(__file__).parent / "static"

client = anthropic.Anthropic()
app = FastAPI(title="Stylist")

SYSTEM_TEMPLATE = """\
You are Stylist — a sharp, warm personal fashion stylist and shopping assistant. \
You talk like a stylish friend with great taste: confident, specific, zero filler.

Today's date: {today}.

User style profile (set by the user; respect it unless they say otherwise):
{profile}

## How you work
- Tailor every suggestion to the user's region, season/weather, budget, body/fit \
notes, occasion, and stated style. If key details are missing, make one smart \
assumption and say so in a few words — ask at most one clarifying question, and \
only when the answer would genuinely change your recommendation.
- Give reasons a stylist would give ("the straight-leg balances the boxy top"), \
briefly.
- Quote prices as realistic ranges in the user's local currency.
- Stay on fashion, grooming, and shopping. If asked something unrelated, redirect \
warmly in one sentence.

## Output format (strict)
- Keep prose tight: 1–3 short sentences before or between cards. No headings, no \
long lists in prose.
- Whenever you recommend clothing/outfits/items, express them as look cards. \
Output 1–3 cards, each as a fenced block exactly like this:

```look
{{"title": "Monsoon office", "occasion": "workday commute, light rain", "why": "Breathable layers that survive humidity and still read polished.", "palette": ["#e8e3d8", "#26364a", "#8c5a3c"], "image_prompt": "editorial fashion photograph of a man in a white relaxed cotton shirt and navy tapered chinos walking a rainy city street under an awning, soft overcast light", "items": [{{"name": "White relaxed cotton shirt", "category": "top", "search": "men white relaxed fit cotton shirt", "price": "₹1,200–2,000"}}, {{"name": "Navy tapered chinos", "category": "bottom", "search": "men navy tapered chinos", "price": "₹1,500–2,500"}}]}}
```

  Rules for cards:
  - Valid JSON on the block's own lines. Keys: title, occasion, why, palette, \
image_prompt, items.
  - palette: 3 hex colors capturing the look's color story.
  - image_prompt: one sentence describing a premium fashion-editorial photo of the \
full look on a model matching the user's gender presentation — outfit, setting, \
light. No brand names, no logos, no text, no real people's likeness.
  - Each item: name, category (top|bottom|outerwear|footwear|accessory|dress|other), \
search, price.
  - "search" is a precise retail search query: include gender qualifier, color, \
fit, and garment type (e.g. "women beige wide leg linen trousers"). Never include \
brand names unless the user asked for that brand.
- For single-item requests (e.g. "find me white sneakers"), still use one look \
card with one or two items.
- End EVERY reply with 3–4 short quick-reply suggestions the user might tap next, \
as the final fenced block:

```chips
["Cheaper version", "Make it more formal", "Swap the footwear"]
```
"""


def build_system(profile: dict) -> str:
    today = time.strftime("%A, %d %B %Y")
    if profile:
        lines = [f"- {k}: {v}" for k, v in profile.items() if v]
        profile_text = "\n".join(lines) or "- (not provided yet)"
    else:
        profile_text = "- (not provided yet)"
    return SYSTEM_TEMPLATE.format(today=today, profile=profile_text)


# --- tiny in-memory per-IP rate limiter (protects the API bill) ---
_hits: dict[str, deque] = defaultdict(deque)


def client_ip(request: Request) -> str:
    """Real client IP behind Railway's edge proxy (X-Forwarded-For),
    falling back to the socket peer for direct/local access."""
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    return request.client.host if request.client else "unknown"


async def read_json_object(request: Request) -> dict:
    """Parse the request body, tolerating malformed or non-object JSON."""
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def allow(ip: str) -> bool:
    now = time.time()
    q = _hits[ip]
    while q and now - q[0] > 3600:
        q.popleft()
    if len(q) >= RATE_LIMIT_PER_HOUR:
        return False
    q.append(now)
    return True


def clean_messages(raw) -> list[dict]:
    """Validate and trim client-supplied history."""
    if not isinstance(raw, list):
        return []
    out = []
    for m in raw:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        content = m.get("content")
        if role not in ("user", "assistant") or not isinstance(content, str):
            continue
        content = content.strip()[:MAX_MESSAGE_CHARS]
        if content:
            out.append({"role": role, "content": content})
    out = out[-HISTORY_LIMIT:]
    while out and out[0]["role"] != "user":
        out.pop(0)
    return out


def sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


@app.get("/healthz")
def healthz():
    return {"ok": True, "model": MODEL}


@app.post("/api/chat")
async def chat(request: Request):
    if not allow(client_ip(request)):
        return JSONResponse(
            {"error": "You're styling fast! Please wait a bit and try again."},
            status_code=429,
        )

    body = await read_json_object(request)
    messages = clean_messages(body.get("messages"))
    if not messages:
        return JSONResponse({"error": "No message provided."}, status_code=400)

    profile = body.get("profile") if isinstance(body.get("profile"), dict) else {}
    # keep profile injection bounded
    profile = {str(k)[:40]: str(v)[:200] for k, v in list(profile.items())[:12]}
    system = build_system(profile)

    def generate():
        try:
            with client.messages.stream(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=system,
                messages=messages,
            ) as stream:
                for text in stream.text_stream:
                    yield sse({"type": "text", "text": text})
                final = stream.get_final_message()
            yield sse({"type": "done", "stop_reason": final.stop_reason})
        except anthropic.AuthenticationError:
            yield sse({"type": "error",
                       "error": "Server is missing a valid ANTHROPIC_API_KEY."})
        except anthropic.RateLimitError:
            yield sse({"type": "error",
                       "error": "The stylist is a little overloaded — try again in a minute."})
        except anthropic.APIStatusError as e:
            yield sse({"type": "error", "error": f"Upstream error ({e.status_code}). Try again."})
        except anthropic.APIConnectionError:
            yield sse({"type": "error", "error": "Network hiccup reaching the model. Try again."})
        except Exception:
            # e.g. missing ANTHROPIC_API_KEY raises TypeError at request time
            yield sse({"type": "error",
                       "error": "The stylist isn't configured yet (check ANTHROPIC_API_KEY on the server)."})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Daily visual feed — one structured-output call generates 5 collections.
# ---------------------------------------------------------------------------

_ITEM_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "category": {"type": "string",
                     "enum": ["top", "bottom", "outerwear", "footwear",
                              "accessory", "dress", "other"]},
        "search": {"type": "string"},
        "price": {"type": "string"},
        "image_prompt": {"type": "string"},
    },
    "required": ["name", "category", "search", "price", "image_prompt"],
    "additionalProperties": False,
}

FEED_SCHEMA = {
    "type": "object",
    "properties": {
        "greeting": {"type": "string"},
        "collections": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "tagline": {"type": "string"},
                    "occasion": {"type": "string"},
                    "why": {"type": "string"},
                    "tip": {"type": "string"},
                    "palette": {"type": "array", "items": {"type": "string"}},
                    "image_prompt": {"type": "string"},
                    "items": {"type": "array", "items": _ITEM_SCHEMA},
                },
                "required": ["title", "tagline", "occasion", "why", "tip",
                             "palette", "image_prompt", "items"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["greeting", "collections"],
    "additionalProperties": False,
}

FEED_SYSTEM = """\
You are the curation engine of Stylist — you produce a daily, deeply personal \
visual fashion feed, like the cover wall of a magazine made for one person.

Produce EXACTLY 5 collections. Across the five, cover: (1) today's weather and \
season, (2) work / daytime, (3) an evening or social occasion, (4) relaxed \
weekend, and (5) one adventurous trend-forward wildcard that stretches the \
user's style a notch. Never repeat a silhouette or color story across cards.

Tailor everything to the user profile: gender presentation, region (climate, \
culture, local pricing in local currency — include ethnic/fusion pieces where \
regionally natural), budget level, style vibes, fit notes. Respect "loved" \
signals and avoid "less of this" signals.

Field rules:
- greeting: short and warm; use the person's name if given; nod to the day or \
weather. No emoji.
- title: 2–4 evocative words, magazine-cover energy. Must not repeat any title \
in avoid_titles.
- tagline: one sharp sentence of intent.
- occasion: 2–5 word context label.
- why: 1–2 sentences of stylist logic (silhouette, proportion, fabric, color).
- tip: one insider styling tip for this look.
- palette: exactly 3-4 muted, cohesive hex colors that define the color story.
- image_prompt: one sentence describing a premium editorial fashion photograph \
of the complete outfit worn by a model matching the user's gender presentation \
— garments in detail, setting, light, mood. No brand names, no logos, no text, \
no real person's likeness.
- items: 3–5 pieces. search = precise retail query (gender + color + fit + \
garment, never brands). price = realistic local range. item image_prompt = \
"studio product photograph of <item>, floating on a warm neutral seamless \
background, soft shadow" adapted to the piece.
"""


@app.post("/api/feed")
async def feed(request: Request):
    if not allow(client_ip(request)):
        return JSONResponse(
            {"error": "You're refreshing fast! Please wait a bit."}, status_code=429
        )

    body = await read_json_object(request)
    profile = body.get("profile") if isinstance(body.get("profile"), dict) else {}
    profile = {str(k)[:40]: str(v)[:200] for k, v in list(profile.items())[:12]}
    ctx = body.get("context") if isinstance(body.get("context"), dict) else {}

    brief = {
        "date": time.strftime("%A, %d %B %Y"),
        "profile": profile or "not provided — assume versatile, mid-budget",
        "weather": str(ctx.get("weather", "unknown"))[:120],
        "loved": [str(x)[:60] for x in ctx.get("loved", [])[:8]],
        "less_of_this": [str(x)[:60] for x in ctx.get("less", [])[:8]],
        "avoid_titles": [str(x)[:60] for x in ctx.get("avoid", [])[:15]],
    }

    def call():
        return client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=FEED_SYSTEM,
            messages=[{
                "role": "user",
                "content": "Generate today's feed for this brief:\n"
                           + json.dumps(brief, ensure_ascii=False),
            }],
            output_config={"format": {"type": "json_schema", "schema": FEED_SCHEMA}},
        )

    try:
        resp = await run_in_threadpool(call)
        if resp.stop_reason == "refusal":
            return JSONResponse({"error": "Couldn't style that request."}, status_code=502)
        text = next((b.text for b in resp.content if b.type == "text"), "")
        data = json.loads(text)
        data["collections"] = data.get("collections", [])[:6]
        return JSONResponse(data)
    except anthropic.AuthenticationError:
        return JSONResponse({"error": "Server is missing a valid ANTHROPIC_API_KEY."},
                            status_code=500)
    except anthropic.RateLimitError:
        return JSONResponse({"error": "The stylist is overloaded — try again in a minute."},
                            status_code=503)
    except anthropic.APIStatusError as e:
        return JSONResponse({"error": f"Upstream error ({e.status_code})."}, status_code=502)
    except (anthropic.APIConnectionError, json.JSONDecodeError):
        return JSONResponse({"error": "Couldn't reach the stylist — try again."},
                            status_code=502)
    except Exception:
        return JSONResponse(
            {"error": "The stylist isn't configured yet (check ANTHROPIC_API_KEY)."},
            status_code=500)


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
