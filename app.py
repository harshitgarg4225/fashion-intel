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
{{"title": "Monsoon office", "occasion": "workday commute, light rain", "why": "Breathable layers that survive humidity and still read polished.", "items": [{{"name": "White relaxed cotton shirt", "category": "top", "search": "men white relaxed fit cotton shirt", "price": "₹1,200–2,000"}}, {{"name": "Navy tapered chinos", "category": "bottom", "search": "men navy tapered chinos", "price": "₹1,500–2,500"}}]}}
```

  Rules for cards:
  - Valid JSON on the block's own lines. Keys: title, occasion, why, items.
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
    ip = request.client.host if request.client else "unknown"
    if not allow(ip):
        return JSONResponse(
            {"error": "You're styling fast! Please wait a bit and try again."},
            status_code=429,
        )

    body = await request.json()
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


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
