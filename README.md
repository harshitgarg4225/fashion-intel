# Stylist — AI shopping assistant

A personal fashion stylist + shopping assistant, built to be the **cheapest possible thing to run** while keeping a polished, product-grade UX.

**Try it:** deploy to Railway (below), open the URL on your phone, answer the 30-second style quiz, and ask for a look.

## What it does

- Chat with a stylist that knows your **gender presentation, region, budget, vibe, and fit notes** (30-second onboarding quiz, editable anytime).
- Replies stream in live and include **look cards** — structured outfits with per-item prices and one-tap **shop buttons** that deep-link to retailer searches (Myntra / Amazon / Ajio / Flipkart in India; ASOS / Nordstrom / Zara / Amazon in the US & UK).
- **Quick-reply chips** after every answer ("Cheaper version", "Make it more formal"…).
- **Save looks** with ♡ — they live in a Saved tab.
- Mobile-first editorial design, automatic dark mode, works as a full-screen web app.

## Why it's cheap

| Cost line | This app |
|---|---|
| Hosting | **1 Railway service** (fits the Hobby plan). No database, no cron, no workers. |
| Storage | **$0** — chat history, profile, and saved looks live in the browser (localStorage). The server is stateless. |
| Product data | **$0** — no catalog API, no scraping, no affiliate feed. The model writes precise search queries; the app builds retailer deep links. Inventory and prices are always live because the retailer renders them. |
| Model | **Claude Haiku 4.5** by default ($1 / $5 per MTok — Anthropic's cheapest, and its speed makes the chat feel snappy). Replies are capped at ~2k output tokens and history at 24 messages. |
| Abuse | Built-in per-IP rate limit (40 req/hour by default). |

Typical conversation turn ≈ 2–4k input + 0.5–1.5k output tokens → **well under ₹1 (~$0.01) per message** on Haiku.

## Deploy on Railway

1. Push this repo to GitHub (already done if you're reading this there).
2. On [railway.app](https://railway.app): **New Project → Deploy from GitHub repo** → pick `fashion-intel`.
3. In the service → **Variables**, add:
   - `ANTHROPIC_API_KEY` — from [console.anthropic.com](https://console.anthropic.com)
4. Deploy. Railway reads `railway.json` (Nixpacks detects Python via `requirements.txt`) and starts `uvicorn`. Health check: `/healthz`.
5. **Settings → Networking → Generate Domain** to get a public URL.

### Optional variables

| Variable | Default | Notes |
|---|---|---|
| `CLAUDE_MODEL` | `claude-haiku-4-5` | Set `claude-sonnet-5` or `claude-opus-4-8` for noticeably better styling judgment at ~3–5× the token price. |
| `MAX_TOKENS` | `2048` | Per-reply output cap. |
| `RATE_LIMIT_PER_HOUR` | `40` | Per-IP request cap. |

## Run locally

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
uvicorn app:app --reload
# open http://localhost:8000
```

## Architecture

```
Browser (static HTML/CSS/JS, localStorage state)
   │  POST /api/chat  {messages, profile}   ← full history sent each turn
   ▼
FastAPI (app.py, stateless)
   │  messages.stream()
   ▼
Claude (Haiku 4.5) — system prompt = stylist persona + user profile + card format
   │
   ▼
SSE stream → client parses ```look / ```chips fenced JSON → cards & chips
```

The model emits outfit data as fenced ` ```look ` JSON blocks inside its normal streamed reply; the client renders those as cards and turns each item's `search` query into retailer links for the user's region. ````chips` blocks become quick-reply buttons.

## Deliberate non-goals (kept out to stay cheap)

- No image generation / virtual try-on (GPU cost).
- No live catalog or affiliate integration (feed contracts, stale-stock problems).
- No accounts or server-side persistence (privacy win too — nothing to delete).

Each of these can be layered on later without changing the core loop.
