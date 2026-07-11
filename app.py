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

import re

import anthropic
from fastapi import FastAPI, Request
from fastapi.responses import (FileResponse, JSONResponse, PlainTextResponse,
                               StreamingResponse)
from fastapi.staticfiles import StaticFiles

MODEL = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5")
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "2048"))
RATE_LIMIT_PER_HOUR = int(os.environ.get("RATE_LIMIT_PER_HOUR", "40"))
HISTORY_LIMIT = 24          # most-recent messages kept per request
MAX_MESSAGE_CHARS = 4000    # per-message input cap

STATIC_DIR = Path(__file__).parent / "static"

# Async client: streaming replies don't pin a threadpool thread each,
# so one small instance can hold many concurrent streams.
client = anthropic.AsyncAnthropic()
app = FastAPI(title="Stylist", docs_url=None, redoc_url=None, openapi_url=None)

MAX_BODY_BYTES = 6_500_000  # ~4MB base64 image + headroom

CSP = (
    "default-src 'self'; "
    "img-src 'self' data: https://image.pollinations.ai; "
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
    "font-src https://fonts.gstatic.com; "
    "connect-src 'self' https://api.open-meteo.com; "
    "worker-src 'self'; "
    "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
)


@app.middleware("http")
async def hardening(request: Request, call_next):
    if request.method == "POST":
        try:
            if int(request.headers.get("content-length", "0")) > MAX_BODY_BYTES:
                return JSONResponse({"error": "That photo is too large — try a smaller one."},
                                    status_code=413)
        except ValueError:
            pass
    resp = await call_next(request)
    h = resp.headers
    h.setdefault("X-Content-Type-Options", "nosniff")
    h.setdefault("X-Frame-Options", "DENY")
    h.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    h.setdefault("Permissions-Policy", "geolocation=(self), microphone=(self), camera=()")
    h.setdefault("Content-Security-Policy", CSP)
    path = request.url.path
    if path.startswith("/static/"):
        h.setdefault("Cache-Control", "public, max-age=3600")
    elif path in ("/", "/sw.js"):
        h.setdefault("Cache-Control", "no-cache")
    return resp

SYSTEM_TEMPLATE = """\
You are Stylist — a sharp, warm personal fashion stylist and shopping assistant \
for busy working professionals. You talk like a stylish friend with great taste: \
confident, specific, zero filler. Your user has minutes, not hours — lead with \
the answer, decide for them, and keep alternatives to one line.

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
- When the user shares a PHOTO (an outfit, a garment, a screenshot of a look): \
name what you see in one short sentence — key pieces, colors, fit — then make it \
shoppable. A full outfit becomes one look card with search queries for close \
matches; a single product gets 2–3 look cards showing different ways to style \
it. Never identify or comment on the person; talk only about the clothes.

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

    # Optional photo riding with the newest user turn (visual search).
    image = body.get("image")
    if isinstance(image, dict) and messages[-1]["role"] == "user":
        media_type = image.get("media_type")
        data = image.get("data")
        if (media_type in ("image/jpeg", "image/png", "image/webp")
                and isinstance(data, str) and 0 < len(data) <= 4_000_000):
            messages[-1] = {
                "role": "user",
                "content": [
                    {"type": "image",
                     "source": {"type": "base64", "media_type": media_type, "data": data}},
                    {"type": "text", "text": messages[-1]["content"]},
                ],
            }

    profile = body.get("profile") if isinstance(body.get("profile"), dict) else {}
    # keep profile injection bounded
    profile = {str(k)[:40]: str(v)[:200] for k, v in list(profile.items())[:12]}
    system = build_system(profile)

    async def generate():
        try:
            async with client.messages.stream(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=system,
                messages=messages,
            ) as stream:
                async for text in stream.text_stream:
                    yield sse({"type": "text", "text": text})
                final = await stream.get_final_message()
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
visual fashion feed for a busy working professional who has minutes, not hours. \
Decide for them; they can always push back in chat.

Produce EXACTLY 5 collections:
1. TODAY'S ANSWER — exactly what to wear to work today, given the weather, the \
day of the week, and their dress code. Decisive, zero hedging: this is the \
outfit. Tagline states why it wins today in one line.
2. HIGH STAKES — a client-facing / boardroom / big-meeting look that reads \
authoritative without trying.
3. AFTER HOURS — desk to dinner or drinks with minimal change (one swap max).
4. WEEKEND RESET — off-duty ease that still looks decided.
5. WILDCARD — travel, an occasion on the horizon, or one trend worth their \
time. Stretch their style a notch.
Never repeat a silhouette or color story across cards.

Tailor everything to the user profile: gender presentation, region (climate, \
culture, local pricing in local currency — include ethnic/fusion pieces where \
regionally natural), budget level, style vibes, fit notes. Respect "loved" \
signals and avoid "less of this" signals. style_answers are direct quiz \
answers from the user — treat them as strong, explicit preferences (colour \
leanings, fit, footwear, prints, accessories, layering). If style_answers \
point to a flattering colour family, make ONE of the five collections a \
colour story around it — title it like a shade editorial (e.g. "The Sapphire \
Hour"), say in the tagline why that shade works for them, and build every \
piece inside that colour family.

Field rules:
- greeting: one decisive, time-aware line; use the person's name if given; nod \
to the day or weather. They have 30 seconds — no filler, no emoji.
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


class FeedStreamParser:
    """Incrementally extracts feed events from a streaming JSON response.

    The model streams one big JSON object; users shouldn't wait for all of
    it. This scanner emits the greeting the moment its string closes and
    each collection the moment its object's braces balance — so the first
    card can render seconds before generation finishes.
    """

    _GREETING = re.compile(r'"greeting"\s*:\s*"((?:[^"\\]|\\.)*)"')
    _ARRAY = re.compile(r'"collections"\s*:\s*\[')

    def __init__(self):
        self.buf = ""
        self.greeting_sent = False
        self.cursor = None  # scan position inside the collections array

    def feed(self, delta: str) -> list[dict]:
        self.buf += delta
        events = []
        if not self.greeting_sent:
            m = self._GREETING.search(self.buf)
            if m:
                try:
                    text = json.loads('"' + m.group(1) + '"')
                except json.JSONDecodeError:
                    text = m.group(1)
                events.append({"type": "greeting", "text": text})
                self.greeting_sent = True
        if self.cursor is None:
            m = self._ARRAY.search(self.buf)
            if m:
                self.cursor = m.end()
        if self.cursor is not None:
            while True:
                found = self._next_object(self.cursor)
                if found is None:
                    break
                obj, self.cursor = found
                if obj is not None:
                    events.append({"type": "collection", "data": obj})
        return events

    def _next_object(self, start: int):
        """Return (parsed_or_None, new_cursor) for the next brace-balanced
        object at/after `start`, or None if it isn't complete yet."""
        s, n = self.buf, len(self.buf)
        i = start
        while i < n and s[i] in " \t\r\n,":
            i += 1
        if i >= n or s[i] != "{":
            return None
        depth, in_str, esc = 0, False, False
        for j in range(i, n):
            ch = s[j]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
            elif ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(s[i:j + 1]), j + 1
                    except json.JSONDecodeError:
                        return None, j + 1  # skip a malformed element
        return None


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
    if not isinstance(ctx.get("loved"), list):
        ctx["loved"] = []
    if not isinstance(ctx.get("less"), list):
        ctx["less"] = []
    if not isinstance(ctx.get("avoid"), list):
        ctx["avoid"] = []
    quiz = ctx.get("quiz") if isinstance(ctx.get("quiz"), dict) else {}
    quiz = {str(k)[:30]: str(v)[:60] for k, v in list(quiz.items())[:10]}

    brief = {
        "date": time.strftime("%A, %d %B %Y"),
        "profile": profile or "not provided — assume versatile, mid-budget",
        "weather": str(ctx.get("weather", "unknown"))[:120],
        "loved": [str(x)[:60] for x in ctx["loved"][:8]],
        "less_of_this": [str(x)[:60] for x in ctx["less"][:8]],
        "style_answers": quiz,
        "avoid_titles": [str(x)[:60] for x in ctx["avoid"][:20]],
    }

    async def generate():
        parser = FeedStreamParser()
        count = 0
        try:
            async with client.messages.stream(
                model=MODEL,
                max_tokens=4096,
                system=FEED_SYSTEM,
                messages=[{
                    "role": "user",
                    "content": "Generate today's feed for this brief:\n"
                               + json.dumps(brief, ensure_ascii=False),
                }],
                output_config={"format": {"type": "json_schema", "schema": FEED_SCHEMA}},
            ) as stream:
                async for delta in stream.text_stream:
                    for ev in parser.feed(delta):
                        if ev["type"] == "collection":
                            if count >= 6:
                                continue
                            count += 1
                        yield sse(ev)
                await stream.get_final_message()
            if count == 0:
                yield sse({"type": "error",
                           "error": "The stylist came back empty — try again."})
            else:
                yield sse({"type": "done"})
        except anthropic.AuthenticationError:
            yield sse({"type": "error",
                       "error": "Server is missing a valid ANTHROPIC_API_KEY."})
        except anthropic.RateLimitError:
            yield sse({"type": "error",
                       "error": "The stylist is overloaded — try again in a minute."})
        except anthropic.APIStatusError as e:
            yield sse({"type": "error", "error": f"Upstream error ({e.status_code})."})
        except anthropic.APIConnectionError:
            yield sse({"type": "error", "error": "Couldn't reach the stylist — try again."})
        except Exception:
            yield sse({"type": "error",
                       "error": "The stylist isn't configured yet (check ANTHROPIC_API_KEY)."})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/sw.js")
def service_worker():
    # Served from the root so the service worker can control the whole app.
    return FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript")


@app.get("/robots.txt")
def robots():
    return PlainTextResponse("User-agent: *\nAllow: /\n")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
