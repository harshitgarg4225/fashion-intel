# Deploying Mira on Railway

Mira runs as a single Node service (Vite build + `vite preview` serving the app and all `/api` middleware). This guide takes you from zero to a working deployment.

## 1. Provision the third-party APIs (one time)

| Provider | What it powers | Where to get it |
| --- | --- | --- |
| **OpenAI** (required) | gpt-image rendering: cutouts, modeled photos, outfit renders; default detection + stylist | [platform.openai.com](https://platform.openai.com/api-keys) → create an API key |
| **Anthropic** (optional) | Runs garment detection + outfit curation on Claude | [console.anthropic.com](https://console.anthropic.com/) → API keys |
| **Google Gemini** (optional) | Alternative image renderer (`WARDROBE_IMAGE_PROVIDER=gemini`) | [aistudio.google.com](https://aistudio.google.com/apikey) → API key |
| **Google Photos** (optional) | Import photos straight from Google Photos | [console.cloud.google.com](https://console.cloud.google.com/): create a project → enable the **Photos Picker API** → OAuth consent screen (External, add yourself as test user) → Credentials → OAuth client, type **Web application**, authorized redirect URI `https://YOUR-APP.up.railway.app/api/google/callback` |
| Open-Meteo (weather) | "Use my weather" | Nothing — no key needed |

Keys can be entered two ways: as Railway service variables (preferred for a deployment), or pasted into the app's in-tray setup screen (stored in `data/settings.json` on the volume; env always wins).

## 2. Create the Railway service

1. Railway → **New Project → Deploy from GitHub repo** → pick this repository, branch `main` (or your working branch). `railway.json` already sets the build (`npm ci && npm run build`) and start (`npx vite preview`) commands; Railway injects `PORT` and the server binds `0.0.0.0` automatically when it detects Railway.
2. **Add a volume** (service → Settings → Volumes): mount path `/data`. Without it, your closet is erased on every redeploy.
3. **Set variables** (service → Variables):

```
WARDROBE_DATA_DIR=/data
WARDROBE_PASSPHRASE=<a long random phrase>   ← REQUIRED: this instance is on the public internet
OPENAI_API_KEY=sk-...
# optional:
ANTHROPIC_API_KEY=sk-ant-...
GEMINI_API_KEY=...
WARDROBE_IMAGE_PROVIDER=openai            # or gemini
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
WARDROBE_DAILY_BUDGET=5                   # recommended spend ceiling (USD/day)
WARDROBE_HEMISPHERE=north                 # season logic; use south below the equator
WARDROBE_ABOUT_URL=                       # optional: where share pages' CTA points (default: this repo)
```

4. **Generate a domain** (Settings → Networking → Generate Domain), then put that exact domain into the Google OAuth redirect URI (step 1) if you use Google Photos.
5. Deploy. Open the domain → enter your passphrase → the import tray walks you through the reference photo (webcam or upload) and any keys you didn't set as variables.

## 3. What the passphrase protects

With `WARDROBE_PASSPHRASE` set, every `/api` route (your photos, closet, tokens, keys) requires a session cookie obtained by entering the passphrase; login attempts are timing-safe and rate-slowed. The Google OAuth token is additionally **encrypted at rest** (AES-256-GCM, key derived from the passphrase). Never deploy publicly without the passphrase set.

## 4. Notes and limits

- **One wardrobe per instance.** There are no user accounts; the passphrase gates a single closet. For two people, run two services.
- **Backups**: Railway volumes persist across deploys, but you can also run `npm run backup` locally against a copy, or download `/data` via `railway ssh`.
- **Costs**: image generation is the dominant cost (~$0.10–0.25 per render depending on model/quality). `WARDROBE_DAILY_BUDGET` hard-stops generation past your ceiling; the studio shows today's estimated spend.
- **Local dev is unchanged**: `npm run dev` binds 127.0.0.1 without a passphrase.
