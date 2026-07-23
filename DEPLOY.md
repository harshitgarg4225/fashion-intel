# Deploying Mira on Railway

Mira runs as a single Node service (Vite build + `vite preview` serving the app and all `/api` middleware). This guide takes you from zero to a working deployment.

## 1. Provision the third-party APIs (one time)

| Provider | What it powers | Where to get it |
| --- | --- | --- |
| **OpenAI** (required) | gpt-image rendering: cutouts, modeled photos, outfit renders; default detection + stylist | [platform.openai.com](https://platform.openai.com/api-keys) → create an API key |
| **Anthropic** (optional) | Runs garment detection + outfit curation on Claude | [console.anthropic.com](https://console.anthropic.com/) → API keys |
| **Google Gemini** (optional) | Alternative image renderer (`WARDROBE_IMAGE_PROVIDER=gemini`) | [aistudio.google.com](https://aistudio.google.com/apikey) → API key |
| **fal.ai** (optional) | Cheapest renderer (~$0.03): open-weight Seedream/Qwen edit models on US infra (`WARDROBE_IMAGE_PROVIDER=fal`) | [fal.ai](https://fal.ai/dashboard/keys) → API key |
| **Google Photos** (optional) | Import photos straight from Google Photos | [console.cloud.google.com](https://console.cloud.google.com/): create a project → enable the **Photos Picker API** → OAuth consent screen (External, add yourself as test user) → Credentials → OAuth client, type **Web application**, authorized redirect URI `https://YOUR-APP.up.railway.app/api/google/callback` |
| Open-Meteo (weather) | "Use my weather" | Nothing — no key needed |

Keys can be entered two ways: as Railway service variables (preferred for a deployment), or pasted into the app's in-tray setup screen (stored in `data/settings.json` on the volume; env always wins).

### All-Google stack (GCP credits)

The entire AI stack — garment detection, the stylist, and image rendering — can run on a single Gemini key, which is the cheapest path and lets Google Cloud startup credits cover all AI spend:

```
GEMINI_API_KEY=...
WARDROBE_IMAGE_PROVIDER=gemini
WARDROBE_STYLIST_PROVIDER=gemini    # optional; auto-selected when Gemini is the only key
```

**Privacy requirement**: create the API key on a GCP project **with billing enabled** (paid tier — your credits absorb the cost). Do NOT serve real users on the AI Studio free tier: its terms allow Google to use submitted content to improve their products, which would contradict the privacy policy this app ships (`/legal/privacy`). Free tier is fine for your own development only.

### Cheapest renders (fal.ai — Seedream / Qwen)

`WARDROBE_IMAGE_PROVIDER=fal` + `FAL_API_KEY` switches rendering to open-weight Chinese editing models served from US infrastructure via fal.ai — roughly the cheapest per-render path (~$0.02–0.04 vs ~$0.17 for gpt-image). `FAL_IMAGE_MODEL` picks the model (default `fal-ai/bytedance/seedream/v4/edit`; `fal-ai/qwen-image-edit-plus` is the Qwen alternative). Mix per role: the stylist can stay on Gemini (`WARDROBE_STYLIST_PROVIDER=gemini`) while fal renders. **Quality gate before committing**: identity preservation is the product — generate a few looks of yourself on each provider and pick with your eyes, not the price sheet. Note fal is pay-as-you-go (GCP credits don't cover it), so on a credits-funded launch Gemini is effectively $0 while fal costs real cash.

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

## 5. Multi-tenant mode (onboard customers)

Set these additional variables to turn the same deployment into a hosted product with Google sign-in and per-user closets:

```
MIRA_MULTI_TENANT=true
MIRA_SESSION_SECRET=<random 32+ chars>
MIRA_FREE_RENDERS=25
GOOGLE_CLIENT_ID=...            # same OAuth client as Photos import
GOOGLE_CLIENT_SECRET=...
```

Add `https://YOUR-DOMAIN/auth/google/callback` as a second authorized redirect URI on the Google OAuth client. In this mode:

- Visitors see **Continue with Google**; each account gets a private closet at `/data/users/<id>/` on the volume — imports, looks, journal, reference photo, usage, and audit are fully isolated per user.
- **Your** env API keys serve all users; the in-app key screen is disabled. Each user gets `MIRA_FREE_RENDERS` lifetime render credits (raise a user's `credits` in `data/users.json` to grant more); `WARDROBE_DAILY_BUDGET` still caps each user's daily spend.
- Share links keep working publicly across users; share management is scoped to the owning account. Every share page's CTA is a **referral link**: a visitor who signs up from it is attributed to the sharer, and when they finish their first render both sides earn credits (`MIRA_REFERRAL_BONUS` for the inviter, default 10; `MIRA_REFERRED_BONUS` for the newcomer, default 5). Users also have a personal invite link ("Invite friends" in the account bar), and share pages count views.
- The passphrase gate is replaced by SSO. Before charging money, add: terms/privacy pages, a data-deletion path, and billing — see ROADMAP.

## 6. Payments (Razorpay) & compliance

1. Create a [Razorpay](https://dashboard.razorpay.com/) account, complete KYC, and copy the Key ID/Secret into `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET`.
2. Add a webhook for `payment.captured` pointing to `https://YOUR-DOMAIN/api/billing/webhook` and set `RAZORPAY_WEBHOOK_SECRET`.
3. Set `MIRA_OPERATOR_NAME` and `MIRA_SUPPORT_EMAIL` — these render into the live legal pages Razorpay's activation team checks: `/legal/terms`, `/legal/privacy`, `/legal/refunds`, `/legal/pricing`, `/legal/contact`.
4. Users buy credit packs from the "Add renders" button (₹399/50, ₹999/150, ₹1,999/400 — edit `CREDIT_PACKS` in `scripts/billing-api.mjs` to change). Payments are verified server-side by signature, credited idempotently (checkout callback or webhook, whichever lands first), and never touch card data.
5. Compliance built in: DPDP-aligned privacy policy, self-serve **Delete account** (erases the user's closet, shares, billing records, and account), refunds policy honored by design (failed renders never consume credits).
