<div align="center">

# Mira

Your AI clothing journal.

[![License: MIT](https://img.shields.io/badge/license-MIT-191919?style=flat-square)](LICENSE)
[![Node 22+](https://img.shields.io/badge/node-22%2B-191919?style=flat-square)](package.json)

</div>

**Mira** turns photos of your clothes into an organized digital closet, then dresses you from it — starting from how you want to *feel*. (Repository and environment variables keep their original names for compatibility.)

- **"How do you want to feel today?"** — pick a feeling (confident, cozy, bold, effortless…) or describe it in your own words, add an occasion preset and one-tap local weather — or attach an inspiration photo — and the stylist curates a complete look from your real closet, explains why it works, and renders a lookbook photo of *you* wearing it. Ask for **3 options**, get **another take**, tap **Wear it** when you walk out the door, and 👍/👎 with a reason — the stylist learns from every verdict, your **style profile** (notes + hard rules), and the garment photos themselves.
- **Import from anywhere** — drop, paste, or upload photos; pick photos straight from **Google Photos** (official Picker API); or point it at a **local folder** (export an Apple Photos album to a folder and import it in one go). AI vision detects every garment, extracts a clean product cutout, and models each piece on you.
- **Only *your* clothes** — with your reference photo in place, the face filter keeps garments worn by you (or shown unworn) and skips friends, mannequins, and store photos automatically.
- **Closet** — browse by category, search by name/tag/category, see per-category counts and your wardrobe's dominant color palette. Mark pieces as in-the-laundry (the stylist skips them), add prices, and get flagged when an import looks like a duplicate you already own.
- **Insights** — most-worn and never-worn pieces, cost-per-wear, color balance, category bars, and gap advice ("one more bottom unlocks 14 new pairings"), all computed from your own wear history.
- **Journal** — the heart of Mira: snap what you actually wore (a mirror selfie is enough — no AI, no credits) with a note and a feeling, or log a styled look with **Wear it**. Week by week, day by day, with wear **streaks**, tap-to-backfill for missed days, and a one-tap **weekly collage**.
- **Share loops** — every look and every week can become a **public share link**: a beautiful page with the render, the story, rich link previews (OG images) in WhatsApp/iMessage/X, and a "Get dressed by Mira" call-to-action. Native share sheet on mobile, copy-link on desktop, revocable any time, rate-limited, and never exposing anything you didn't explicitly share.
- **Trust built in** — a local audit log of every AI call, a daily cost meter with an optional hard budget (`WARDROBE_DAILY_BUDGET`), and `npm run backup` / `backup:wipe` to export or erase everything. First run captures your reference photo in-app via webcam or upload.

Everything stays local: originals, cutouts, renders, outfits, and the JSON database live in `data/` on your machine. See [ROADMAP.md](ROADMAP.md) for the full product plan.

> **Building on this codebase (human or AI)?** Start with **[AGENTS.md](AGENTS.md)** — architecture, tenancy rules, provider swapping, the testing pattern, and the traps that have already cost debugging time.

## Host it for others (multi-tenant)

Flip `MIRA_MULTI_TENANT=true` (plus `MIRA_SESSION_SECRET` and your Google OAuth client) and the same deployment becomes a hosted product: **Continue with Google** sign-in, a fully private closet per account, operator-held API keys, and per-user render credits — plus a built-in **referral loop**: share pages carry the sharer's invite code, signups are attributed, and both sides earn bonus renders when the invited friend generates their first look. Details in [DEPLOY.md](DEPLOY.md).

## Deploy it (Railway)

The app ships production-ready for Railway: `railway.json` is included, the server binds Railway's `PORT` automatically, `/data` volume support keeps your closet across deploys, and `WARDROBE_PASSPHRASE` gates the whole API behind a login (required for any public deployment — it also encrypts the Google token at rest). The complete checklist — including provisioning every third-party API key — is in **[DEPLOY.md](DEPLOY.md)**.

## Quick start

```bash
git clone https://github.com/harshitgarg4225/fashion-intel.git
cd fashion-intel
npm install
cp .env.example .env
npm run dev
```

Then:

1. Add `OPENAI_API_KEY` to `.env` (required for all image generation).
2. Put a clear PNG photo of yourself at `data/model-reference.png`.
3. Open [localhost:5173](http://localhost:5173) and drop in a photo of your clothes.

Optionally add `ANTHROPIC_API_KEY` to run garment detection and outfit curation on Claude — image rendering still uses OpenAI gpt-image, which is the only part that requires an OpenAI key.

### Google Photos import (optional)

1. In [Google Cloud Console](https://console.cloud.google.com/), create a project, enable the **Photos Picker API**, and create an OAuth client (type **Web application**) with `http://localhost:5173/api/google/callback` as an authorized redirect URI.
2. Put the client ID and secret in `.env` (`GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`) and restart.
3. In the import tray, hit **Connect Google Photos**, approve access, then pick photos in Google's own picker — the import starts automatically when you're done picking.

Mira uses only the narrow Picker scope: it can never browse your library, only receive the photos you explicitly pick. The OAuth token is stored locally in `data/` with owner-only permissions.

### Apple Photos / camera roll

There is no public Apple cloud API, so use the folder path: export an album from Photos (**File → Export**) to a folder, then use **From folder** in the import tray with that path. Any folder of jpg/png/webp/heic images works.

### Face filter

When `data/model-reference.png` exists and `WARDROBE_FACE_FILTER` isn't `off`, every import (upload, Google Photos, folder) keeps only garments worn by the person matching your reference photo — or garments shown unworn (flat lay, hanger, product shot). Photos of other people are skipped, which makes whole-camera-roll imports practical.

## How it works

| Step | Model | What happens |
| --- | --- | --- |
| Detect | `OPENAI_VISION_MODEL` | Finds every garment in a photo with bounding boxes, names, colors, and tags |
| Extract | `OPENAI_IMAGE_MODEL` | Reconstructs a clean product cutout on a chroma key, then removes the background locally |
| Model | `OPENAI_IMAGE_MODEL` | Renders the piece worn by you, using your reference photo |
| Style | Claude or OpenAI | Curates one coherent outfit from your closet with color-harmony and silhouette rules |
| Lookbook | `OPENAI_IMAGE_MODEL` | Renders the full outfit on you as a square editorial photo |

Every generation step is reviewable: approve, reject, or regenerate with a correction before anything enters your closet.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `OPENAI_API_KEY` | Required | Image generation and default vision/stylist |
| `OPENAI_VISION_MODEL` | `gpt-5.4-mini` | Garment detection |
| `OPENAI_IMAGE_MODEL` | `gpt-image-2` | Cutouts, modeled photos, outfit renders |
| `OPENAI_IMAGE_QUALITY` | `high` | Image quality |
| `ANTHROPIC_API_KEY` | Optional | Runs detection + stylist on Claude when set |
| `ANTHROPIC_MODEL` | `claude-sonnet-5` | Claude stylist model |
| `WARDROBE_STYLIST_PROVIDER` | auto | Force `openai`, `anthropic`, or `gemini` for detection + stylist |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Gemini detection + stylist model (all-Google stack) |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | Optional | Enables Google Photos import |
| `WARDROBE_FACE_FILTER` | `on` | `off` disables the only-my-clothes filter |
| `WARDROBE_MODEL_REFERENCE` | `data/model-reference.png` | Your reference photo |
| `WARDROBE_DATA_DIR` | `data` | Local storage location |
| `WARDROBE_HOST` | `127.0.0.1` | Set `0.0.0.0` to expose on your LAN (trusted networks only) |
| `WARDROBE_DAILY_BUDGET` | off | Hard-stop image generation past this estimated daily spend (USD) |
| `WARDROBE_VISUAL_STYLIST` | `auto` | Send garment thumbnails to the stylist: `auto` (≤20 pieces), `on`, `off` |
| `WARDROBE_PASSPHRASE` | off | Gates every /api route behind a login; required for public deploys |
| `WARDROBE_IMAGE_PROVIDER` | `openai` | `gemini` (Gemini) or `fal` (Seedream/Qwen via fal.ai, cheapest) render all images |
| `FAL_IMAGE_MODEL` | `fal-ai/bytedance/seedream/v4/edit` | Which fal-hosted edit model renders (`FAL_API_KEY`) |
| `WARDROBE_HEMISPHERE` | `north` | Season logic for the stylist and capsule planner |
| `WARDROBE_ABOUT_URL` | this repo | Where the "Get dressed by Mira" button on share pages points |

API keys can also be entered in the app (import tray → setup) and are stored in `data/settings.json`; environment variables always win.

## Security posture

Local-first by design: the server binds to loopback unless you opt out, personal data and tokens never enter Git, Google access is limited to the picker scope with CSRF-protected OAuth, all endpoints validate and cap input sizes, and image generation is concurrency-limited to bound spend. Details and planned hardening are in [ROADMAP.md](ROADMAP.md).

## Import at scale with agent skills

The repo bundles two agent skills under `.agents/skills/` for bulk work from a coding agent (Codex, Claude Code, etc.):

- `import-clothes` — sweep a whole photo folder or camera roll, extract and model every piece, and write them into the closet database.
- `generate-outfits` — batch-create a verified, modeled lookbook from the closet.

## Credits

Built on [tandpfun/wardrobe](https://github.com/tandpfun/wardrobe) by Thijs Simonian (MIT), the project behind the viral "AI organized my entire wardrobe" post. Mira extends it with an in-app Outfit Studio (outfit curation, rendering, and a lookbook gallery — no external agent required), an optional Claude-powered stylist, closet search, category counts, and a wardrobe color palette.

## License

[MIT](LICENSE)
