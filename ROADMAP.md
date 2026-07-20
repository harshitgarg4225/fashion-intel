# Fashion Intel — Product Roadmap

The product promise: **"I want to feel X today" → open the app → see yourself dressed for that feeling, from clothes you actually own.**

Legend: ✅ shipped · 🔜 next up · 🧭 explored / needs design

## 1. The core loop (UX)

- ✅ Mood-first Outfit Studio: "How do you want to feel today?" with feeling chips (confident, effortless, sharp, cozy, playful, bold, romantic, grounded) and free-text feelings
- ✅ Feeling → styling translation in the stylist prompt (structure for confidence, texture for coziness, statement pieces for boldness…)
- ✅ One-tap "Surprise me" when no feeling is picked
- ✅ Occasion/constraints field ("dinner with friends", "office, no sneakers")
- ✅ Local weather awareness (one tap, Open-Meteo, no API key, location never stored)
- ✅ Rendered lookbook photo of *you* wearing the outfit, with the stylist's reasoning
- ✅ Favorites (keep the looks that worked; they sort first)
- ✅ "Wear it" button on every look → builds a wear history that powers Insights
- ✅ PWA shortcuts + deep links (`/?view=outfits`, `/?feel=confident`) for a one-tap "dress me" flow from the home screen
- ✅ Feedback loop: 👍/👎 per look with reason chips ("too formal", "not my colors") fed back into every future curation
- ✅ "Another take": same feeling, different outfit in one tap (🔜 "swap just the top" partial regeneration)
- ✅ Multi-look mornings: "3 options" generates three candidate looks side-by-side
- 🧭 Calendar awareness: read today's events (with permission) → "you have a client meeting at 2pm"
- 🧭 Voice flow: "I feel like a rainy-Sunday writer today" spoken while brushing teeth

## 2. Getting clothes in (ingestion)

- ✅ Drag/drop, paste, and file upload with per-garment review pipeline (crop → cutout → modeled photo)
- ✅ Google Photos import via the official Picker API (OAuth, user picks photos in Google's own UI)
- ✅ Local folder import (works with Apple Photos: export an album to a folder; also any camera-roll dump) with batch limits
- ✅ Face-gated import: uses your reference photo to import only clothes worn by *you* or shown unworn — friends, mannequins, and store photos are skipped
- ✅ Batch progress surfaced live in the import tray as pieces are detected
- ✅ Bulk import agent skills (Codex/Claude Code) for whole-camera-roll sweeps
- 🔜 Direct Apple Photos library reader for macOS (osxphotos-based helper script)
- ✅ Duplicate detection: perceptual hash (structure + color) flags likely duplicates at import, dismissible per item
- 🔜 HEIC decoding fallback when the local sharp build lacks libheif
- 🔜 Screenshot import: paste a product page screenshot → "wishlist" item, kept separate from owned clothes
- 🔜 Email receipt parsing: forward order confirmations to auto-add purchases
- 🧭 In-store mode: photograph a garment on the rack → "does this fit my wardrobe? what would it pair with?" before buying
- 🧭 Retailer integrations for automatic purchase sync

## 3. Knowing the wardrobe (intelligence)

- ✅ Automatic garment naming, categorization, colors, and detail tags at import
- ✅ Closet search (name, tag, category), per-category counts, wardrobe color palette
- ✅ Insights tab: cost-per-wear (with optional prices), most-worn pieces, never-worn count, color balance, category bars
- ✅ Gap analysis: tops/bottoms imbalance and missing-layer advice with unlock counts
- 🔜 Seasonal rotation: tag pieces by season, hide off-season items, "pack for a trip" capsule builder
- ✅ Laundry state: mark pieces as in-the-wash; the stylist skips them and tells you when the clean closet can't make an outfit
- 🔜 Fabric/care metadata and washing-day reminders
- 🧭 Fit notes per garment ("runs small", "great with high-waist") that inform styling
- 🧭 Declutter coach: pieces unworn for N months → donate suggestions

## 4. Styling quality (the stylist brain)

- ✅ Color-harmony, silhouette-balance, and statement-piece rules in the curation prompt
- ✅ Duplicate-combination avoidance across the lookbook
- ✅ Layering plausibility rules (no invented zippers/closures in renders)
- ✅ Provider choice: Claude or OpenAI for the stylist; gpt-image for rendering
- ✅ Visual curation: garment thumbnails go to the stylist (auto for closets ≤20 pieces, `WARDROBE_VISUAL_STYLIST` to override)
- ✅ Personal style profile: persistent style notes + hard rules the stylist reads on every look (🔜 guided quiz to seed it)
- 🔜 Body-type and comfort preferences ("never crop tops", "prefer high rise") as hard constraints
- ✅ Occasion presets (interview, date night, wedding guest, office, weekend, travel)
- ✅ Inspiration-photo styling: attach any photo and the stylist channels its vibe from your own closet
- 🧭 Trend awareness: optional feed of current trends mapped onto owned pieces
- 🧭 Human-stylist marketplace: share a look privately with a professional for feedback

## 5. Sharing & multiplayer

- 🔜 Share a look as an image (rendered photo + garment strip) without exposing the wardrobe
- 🔜 Ask-a-friend: send two candidate looks, friend taps a winner
- 🧭 Household wardrobes: multiple people, one instance, strict per-person separation
- 🧭 Community lookbooks: opt-in anonymous outfit inspiration from similar wardrobes

## 6. Tech & platform

- ✅ Local-first: photos, cutouts, renders, and the database never leave your machine except the specific API calls you configured
- ✅ Provider-agnostic structured vision layer (OpenAI Responses / Anthropic Messages behind one interface)
- ✅ Async generation jobs with resumable state, retry, and interruption recovery on restart
- ✅ Responsive image pipeline (sharp + ipx) for fast gallery loads
- 🔜 SQLite instead of JSON files once libraries exceed a few hundred pieces
- ✅ Daily cost budget: `WARDROBE_DAILY_BUDGET` hard-stops image generation when today's estimated spend would exceed it (🔜 per-provider rate limiting)
- ✅ Cost meter: daily render/vision counts and estimated spend shown in the studio
- 🔜 Model pluggability for rendering (Gemini image models as an alternative to gpt-image)
- ✅ Automated test suite: node:test unit coverage of chroma/framing math, hashing, and prompt builders, wired into CI (🔜 recorded API-fixture contract tests)
- 🔜 Electron/Tauri desktop build so "npm run dev" isn't the install story
- 🧭 Mobile app with on-device capture and background sync to the desktop instance
- 🧭 Local vision models (e.g. SAM for cutouts) to cut API cost to near zero

## 7. Security & privacy

- ✅ Dev server binds to 127.0.0.1 by default (LAN exposure is opt-in via WARDROBE_HOST)
- ✅ OAuth uses the narrow Photos *Picker* scope only — the app can never sweep your whole Google library, only what you pick
- ✅ OAuth CSRF protection (state parameter), tokens stored locally with 0600 permissions, never in Git
- ✅ Face filter runs inside your configured vision call — no third-party face-recognition service
- ✅ Input validation and size caps on every endpoint; batch limits on folder (40) and Google (24) imports
- ✅ Generation concurrency cap to bound API spend from a stuck client
- ✅ All personal data (`data/`, `.env`) gitignored
- 🔜 Optional app passphrase when LAN exposure is enabled
- 🔜 Encrypt the Google token at rest with a key derived from a local secret
- ✅ "Export & wipe": `npm run backup` archives data/, `npm run backup:wipe` archives then deletes everything local
- ✅ Audit log: every external AI call recorded in `data/audit.jsonl` (provider, purpose, image count, timestamp)
- 🧭 E2E-encrypted sync between your own devices

## 8. Ease of use / first-run

- ✅ Setup status surfaced in-app with exact missing steps (key, reference photo, minimum wardrobe)
- ✅ Every AI step reviewable before it touches your closet; regeneration with plain-language corrections
- ✅ Clear provider notes in the UI (what runs where)
- 🔜 Guided first-run wizard: reference capture is in-app now; key entry and first-import walkthrough still to come
- ✅ Reference photo capture in-app: webcam or upload, saved locally — no manual file placement
- 🔜 Demo wardrobe mode to explore the product before adding keys
- 🔜 One-line installer script and prebuilt desktop binaries
- 🧭 Hosted version with per-user encryption for people who won't run anything locally
