# AGENTS.md — orientation for AI agents working on Mira

You are inheriting a **live, revenue-capable product**, not a prototype. Read this file before touching anything; it encodes decisions that are non-obvious from the code and traps that have already cost real debugging time.

**Mira — your AI clothing journal.** A user photographs their clothes once, tells Mira how they want to *feel*, and sees a rendered photo of *themselves* wearing an outfit from their own closet — then journals what they actually wore, day by day.

- **Live**: https://mira-production-892a.up.railway.app
- **Repo**: `harshitgarg4225/fashion-intel` (name is legacy; the product is Mira)
- **Stack**: Vite 6 + React 19 SPA, Node ≥22, sharp; no database (JSON on a volume); no framework backend (see §2)
- **Status**: deployed and serving. Google secrets (`GEMINI_API_KEY`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`) are set by the operator in Railway; without them the app serves but sign-in and generation fail by design.

---

## 1. Non-negotiables

Violating these breaks trust, law, or production. They are not stylistic preferences.

1. **Never commit `data/` or `.env`.** `data/` holds users' personal photos and OAuth tokens. Both are gitignored — keep it that way. When you finish a test run, `rm -rf data`.
2. **Never serve real users on an AI free tier.** Google AI Studio's free tier permits training on submitted content, which contradicts `/legal/privacy` (rendered from `scripts/legal-api.mjs`). Keys must come from a billing-enabled project. Free tier is for local dev only.
3. **Failed generations must never consume credits.** The refund policy promises this. `checkRenderCredits()` runs *before* the provider call; crediting/metering happens *after* success. Preserve that order.
4. **Public share pages (`/s/:token`) are anonymous surfaces.** They must keep: strict CSP (`default-src 'none'`), quote-escaping via `escapeXml`, `X-Robots-Tag: noindex`, rate limiting, and POST-only share creation (GET creation is a CSRF hole — this was a real bug, don't reintroduce it).
5. **Tenant isolation is absolute.** Any new data file must resolve its path through `tenantDataDir()` (§3). A hardcoded `data/foo.json` leaks one user's data into everyone's account.
6. **Don't put the model identifier in repo artifacts** (commits, code comments, PR text).

---

## 2. The one architectural decision that will confuse you

**There is no separate backend server.** The API is implemented as **Vite connect-middleware plugins** in `scripts/*-api.mjs`, each exporting a factory that returns a plugin with `configureServer` (dev) *and* `configurePreviewServer` (prod), plus `apply: "serve"`.

Production is literally `npm run build && vite preview`. That is what Railway runs (`railway.json`).

Consequences you must respect:
- **Plugin order matters** and is defined in `vite.config.mjs`. Auth plugins run before data plugins so unauthenticated requests never reach user data.
- Every plugin **path-gates early**: `if (!url.pathname.startsWith("/api/whatever")) return next();`. If you add an endpoint and it 404s, the gate is the first suspect — this has bitten before (`/api/setup/reference` sat behind an `/api/import/` gate).
- Anything not under `/api/*` bypasses auth by design (that's how public share pages and `/legal/*` work). Be deliberate about which side of that line new routes fall on.
- `apply: "serve"` means these never run at build time.

**Why not Express/Next?** Single process, single build, zero server framework to keep in sync with the client, and `vite preview` gives correct SPA fallback and static serving for free. The cost is the unusual shape you're reading about now.

---

## 3. Multi-tenancy: request-scoped, via AsyncLocalStorage

`scripts/tenant.mjs` is small and load-bearing. `ssoApi` wraps each authenticated request in `runAsUser(user, userDir, next)`, storing context in an `AsyncLocalStorage`. Every data path then resolves at call time:

```js
const dataDir = () => tenantDataDir() || localBaseDir;
const outfitsFileFn = () => path.join(dataDir(), "outfits.json");   // function, NOT a const
```

**The rule: data paths are functions, never constants.** A module-level `const outfitsFile = path.join(...)` captures the wrong tenant forever. Every path in the codebase ends in `Fn()` for this reason.

- Single-tenant mode (no `MIRA_MULTI_TENANT`): no context → base `data/` dir. Both modes share one code path.
- Per-user data lives at `data/users/<sha256(email).slice(0,16)>/`.
- **Exception**: `data/shares.json`, `data/users.json`, `data/billing.json`, `data/events.jsonl` are *global* (cross-tenant), because public share resolution and the operator dashboard need to read across users. Global files carry a `userId` field for ownership checks. `enterShareTenant()` in `outfit-studio-api.mjs` hops into the owning tenant to render a public page.

---

## 4. File map — where things live and why

### Backend (`scripts/`)

| File | Owns | Notes |
| --- | --- | --- |
| `import-job-api.mjs` (995 L) | Garment import pipeline; **`imageEdit()` provider dispatcher**; `atomicJson`, `imageHash`, chroma/framing helpers | Biggest file. Exports utilities used everywhere — import from here rather than duplicating |
| `outfit-studio-api.mjs` (814 L) | Outfit CRUD + generation state machine, journal, weekly review + collage, public `/s/:token` pages, shares, profile, capsule | Second biggest; the product's heart |
| `stylist.mjs` | Prompt construction (`buildCurationPrompt`, `buildOutfitImagePrompt`), curation validation, capsule planner | **Change prompts here, nowhere else** |
| `ai-providers.mjs` | `structuredAnalysis()` — OpenAI / Anthropic / Gemini behind one interface; schema adapters | Text+vision side |
| `tenant.mjs` | AsyncLocalStorage tenancy, `checkRenderCredits()` | §3 |
| `sso-api.mjs` | Google OAuth, session cookies, `/api/me`, account deletion, auth gate for all `/api/*` | Also where signup events + referral attribution fire |
| `auth-api.mjs` | Single-tenant passphrase gate, `/api/health` | Stands down when multi-tenant |
| `billing-api.mjs` | Razorpay orders, signature verification, webhook, credit packs | Idempotent crediting |
| `referrals.mjs` | Invite codes, attribution, first-render reward | §6 |
| `metrics.mjs` / `admin-api.mjs` | Event log + funnel computation / operator dashboard at `/admin/metrics` | §7 |
| `legal-api.mjs` | Public `/legal/*` pages from env-configured operator identity | Razorpay activation requires these |
| `telemetry.mjs` | Audit log, usage counters, provider-aware cost estimates, daily budget | |
| `settings-store.mjs` | `makeSetting()` — resolution order **env > process.env > stored** | Stored keys are ignored in multi-tenant mode |
| `google-photos-api.mjs` | Picker API OAuth + import (narrow scope only) | |
| `request-utils.mjs` | `clientIp()` — rightmost XFF hop, only when proxy is trusted | Spoof resistance; don't "simplify" |
| `wear-stats.mjs` | Pure streak math | Pure = unit-testable |

### Frontend (`src/`)

`App.jsx` (1104 L) is the shell: auth gate, account chip, masthead nav, closet grid, item editor. Views are separate modules — `outfit-studio.jsx`, `journal.jsx`, `insights.jsx`, `import-flow.jsx` — each with a matching `.css`.

**CSS is layered by appending.** `styles.css` contains a base layer, then a "luxury" layer, then the final **Maison layer** (monochrome, `border-radius: 0 !important`, tracked capitals) which overrides earlier layers. Later wins. If a style seems not to apply, something further down the file overrides it. Design intent: Louis Vuitton — strict monochrome, square corners, wide letter-spacing, image-forward.

### Docs

`README.md` (what it is) · `DEPLOY.md` (provisioning + Railway + providers) · `LAUNCH.md` (14-day founder playbook, the four proof metrics) · `ROADMAP.md` (✅/🔜/🧭 backlog) · `UX-REVIEW.md` (persona audit + reasoning behind the journal pivot) · this file.

---

## 5. Providers: swapping AI vendors is a config change, not a code change

Two independent axes:

- **Images** — `WARDROBE_IMAGE_PROVIDER` = `openai` (gpt-image, ~$0.17) | `gemini` (~$0.04) | `fal` (Seedream/Qwen via fal.ai, ~$0.03). Dispatcher: `imageEdit()` in `import-job-api.mjs`.
- **Text/vision** — `WARDROBE_STYLIST_PROVIDER` = `openai` | `anthropic` | `gemini`. Dispatcher: `structuredAnalysis()` in `ai-providers.mjs`. Auto-resolves by which key exists.

They mix freely (e.g. Gemini stylist + fal renderer). To **add a provider**: write one `<name>Edit()` or `<name>Structured()` function, add a branch to the dispatcher, add its cost to `EST_COST` in `telemetry.mjs`, add the key to `STORABLE_KEYS` (`settings-store.mjs`) and `KEY_FIELDS` (`import-flow.jsx`), and name it in the privacy policy's processors list. Schema translation lives with the provider (`geminiSchema`, `looseSchema`) — schemas themselves stay vendor-neutral.

**Quality note**: identity preservation ("that's really me") is the product. Never switch the default renderer on price alone; A/B with real photos first.

---

## 6. The growth loop (understand before editing)

Share page CTA carries `?ref=<code>` → visitor's browser stores a `mira_ref` cookie → Google sign-in round-trips → new account records `referredBy` → **on that user's first successful render**, both sides get credits, exactly once (`refRewarded` flag).

The reward fires on **first render, not signup** — deliberately: renders cost money, so throwaway accounts earn nothing. Self-referrals are rejected. See `referrals.mjs` + the unit test that pins this behavior.

---

## 7. Metrics: how you know if a change worked

`trackEvent()` appends to `data/events.jsonl` (server-side only; no third-party analytics ever touches user photos). `computeMetrics()` derives the funnel; `/admin/metrics` renders it for emails in `MIRA_ADMIN_EMAILS`.

The four numbers that define success (thresholds in `LAUNCH.md`): **activation ≥60%**, **D7 retention ≥25%**, **journalers ≥40%**, **referred share of signups ≥30%**. If you ship a feature, check these before and after. Retention beats novelty.

---

## 8. How to run and verify — including the trick that makes this codebase testable

```bash
npm install
npm run dev          # 127.0.0.1:5173
npm test             # node --test tests/*.test.mjs  (13 tests, keep them green)
npm run build        # must pass before any commit
```

**There are no API keys in a dev sandbox.** The pattern that has verified every feature in this repo: **run a mock provider server, point the app at it via env, drive real HTTP.**

```bash
# 1. mock server on :5198 answering /responses, /images/edits, /token, /userinfo,
#    or Gemini's :generateContent — return the shape the real API returns
node node_modules/.mock.mjs &

# 2. app on :5199 pointed at the mock
GEMINI_API_KEY=mock GEMINI_API_BASE_URL=http://127.0.0.1:5198 \
WARDROBE_IMAGE_PROVIDER=gemini PORT=5199 npx vite preview &

# 3. seed data/ with node+sharp, then drive the real endpoints with fetch and assert
```

Every provider integration has a `*_API_BASE_URL` override precisely so it can be pointed at a mock. Multi-tenant flows are testable the same way: `GOOGLE_TOKEN_URL` / `GOOGLE_USERINFO_URL` / `GOOGLE_OAUTH_BASE_URL` redirect the OAuth dance to your mock.

For UI verification, Playwright is available (`playwright-core`, `executablePath: "/opt/pw-browsers/chromium"`, `PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers`) — **never run `playwright install`.**

**Definition of done for any change**: `npm run build` green, `npm test` green, the affected flow exercised over real HTTP against mocks, `data/` and mock files deleted, then commit.

---

## 9. Traps that have already cost time

- **`pkill -f "vite.*5199"` kills your own shell** — the pattern matches the enclosing command line. Use `for pid in $(pgrep node); do kill $pid; done`.
- **Duplicate SVG attributes crash sharp.** A stray second `letter-spacing` in one `<text>` broke the weekly collage with a cryptic "corrupt header" and took down share pages. sharp's SVG parser is strict; there is no browser leniency here.
- **Railway: don't run `npm ci` in `buildCommand`.** The platform already installed dependencies; a second `npm ci` deletes `node_modules` into a locked cache mount → `EBUSY errno -16`. Build command is just `npm run build`.
- **Railway builder detection follows the default branch.** Legacy Python files on `main` made the builder detect Python and fail with zero logs. Keep deployable branches free of foreign-language marker files (`requirements.txt`, `main.py`).
- **Never gate a file input on `file.type.startsWith("image/")`.** Chrome reports an **empty MIME type for HEIC**, the default iPhone/macOS photo format, so that check silently dropped real users' photos — picking a file did *nothing*, with no error. All four upload surfaces now route through `prepareImageFile()` in `src/image-input.js`, which decodes in-browser, applies EXIF orientation, downscales, re-encodes to JPEG, and throws a message safe to show. Use it for any new upload; never re-add a MIME pre-filter.
- **`node --test tests/`** silently matches nothing — it must be `tests/*.test.mjs`.
- **`@import` must be at the top of a CSS file**; appending one mid-file is silently invalid (font imports live in `main.jsx`).
- **Perceptual hashing on solid colors**: structure-only aHash cannot tell navy from cream. `imageHash` therefore packs 3 color nibbles alongside 16 structure chars, and `hashDistance` weights them. Don't "clean this up."
- Phosphor's hanger icon is `CoatHanger`, not `Hanger`.

---

## 10. Ops facts

Railway project `mira` (`c0d9fa52-319d-4504-9727-8ebb166a1e58`), service `mira` (`e7a7bea1-bca9-4348-a6d1-2ad840ae10fb`), production env (`6acd0ff6-070b-4c37-b3d1-5885d2396f58`), 1 GB volume at `/data`, `WARDROBE_DATA_DIR=/data`.

**Persistence depends entirely on that volume** — if it is ever detached, every user's closet is gone. `npm run backup` archives `data/`.

Scaling reality: one process, JSON files, in-memory rate limits and generation locks ⇒ **no horizontal scaling**, comfortable to roughly a few thousand users. Postgres/SQLite + object storage is the documented next step (`ROADMAP.md`), needed *before* marketing hard, not before proving the MVP.

---

## 11. Where to build next

Highest-leverage items, in order — rationale in `UX-REVIEW.md` and `ROADMAP.md`:

1. **Daily journaling reminder** (opt-in notification at a chosen hour). Biggest untouched retention lever for a journaling product.
2. **"Wear it" with a date picker** — styled looks can only log to *today*, so the journal goes wrong the moment someone logs a day late.
3. **Garment tagging on journal photos** — reuse the import detection so real-life wears feed most-worn/cost-per-wear stats. Closes the loop between journal and closet.
4. **Month view** — week-by-week paging gets tedious past ~6 weeks of entries.
5. **Postgres migration** — when user count or write contention demands it.

Before building any of them: check `/admin/metrics`. If activation is low, onboarding is the bug and none of the above matters yet.

---

## 12. Conventions

- **Match surrounding style**: ES modules, no TypeScript, no semicolonless style, no new dependencies without a clear reason (the dependency list is deliberately tiny).
- **Comments explain *why*, never *what***. Existing comments mark constraints ("Shares are resolved publicly across tenants, so the index is global") — follow that bar.
- **Every user-visible string is product copy.** The voice is warm, plain, second person, never technical ("Add a photo of what you wore — a mirror selfie works perfectly"). No jargon leaks into the UI; hosted users must never be told to "paste an API key".
- **Validate and cap every input** server-side (size limits, slice lengths, enum checks). All existing endpoints do.
- **Be honest in status text.** `deliverShare()` exists because a swallowed `navigator.share` rejection was reporting "Link copied" when nothing was copied. Never claim success you haven't verified — in code or in reports to the user.
