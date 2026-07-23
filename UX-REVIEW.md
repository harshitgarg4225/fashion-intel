# UX Review — Mira as a daily clothing journal

**Method**: walked the full product as its target user — early 20s, dresses with intention, lives on her phone, screenshots outfits she likes, sends OOTDs to the group chat. The question for every step: *would she come back tomorrow, and is this the easiest possible way to do the job?* The job to be done: **"remember what I wore, feel good about how I dress, and get help getting dressed."**

Legend: ✅ fixed in this pass · 📌 documented, on the roadmap

## The verdict before the fixes

She would try Mira once (the renders are the hook), but she would not *journal* here — because the product had no way to journal. The Journal was a read-only report of AI-generated looks: the only path to an entry was generate a render → tap "Wear it". Her actual daily behavior — *"here's what I actually wore today"* — had no home. That's fatal for a product whose promise is "Your AI clothing journal": the habit-forming action must be the cheapest one, and it didn't exist.

## What was missing, step by step

### 1. Journaling real outfits — the core job ✅
- **Finding**: no way to log what you actually wore. Every journal entry required an AI render, which costs a credit and a minute of waiting. A journal you can only write with credits is a journal you stop writing.
- **Fixed**: **"Log what I wore today"** — one tap in the Journal, add a photo (mirror selfie), optional one-line note ("first day at the internship") and feeling, save. Pure photo save: **no AI call, no render credits, ever** — journaling is free forever, and the UI says so. Photo logs lead the day in the week view and the shareable collage; styled looks follow.
- **Fixed**: empty past days in the week are now tappable — missed Tuesday? Tap the tile and backfill (up to two months). Missed days are the #1 reason journaling habits die.
- **Fixed**: journal days count toward the **streak** and week stats (days dressed, feelings of the week), so the streak rewards the honest habit, not just render usage.
- **Fixed**: entries are removable (photo deleted too) — a bad mirror photo shouldn't be forever.
- **Fixed**: a **"Log today" PWA shortcut** (`/?view=journal&log=1`) — long-press the home-screen icon, camera, done.

### 2. First-run in hosted mode ✅
- **Finding**: a signed-in customer's empty closet said *"paste your AI keys"* — developer-speak that would end the session on the spot. Hosted users never touch keys.
- **Fixed**: the welcome card now detects hosted mode and shows her steps: your photo → first pieces → get dressed & journal.

### 3. The product didn't say what it is ✅
- **Finding**: sign-in, masthead, share pages, link previews all said variations of "dress how you feel" — the studio's promise, not the journal's.
- **Fixed**: **"Your AI clothing journal"** adopted everywhere: masthead eyebrow, sign-in card, `<title>`/OG meta, PWA manifest, public share page footers, README. "Dress how you feel" remains the Outfit Studio's opening question, where it belongs.

### 4. Copy that assumed the old model ✅
- **Fixed**: journal empty state now leads with "snap what you wore" before "tap Wear it on a styled look".

## Friction that remains (ranked) 📌

1. **"Wear it" can't backfill** — a styled look always logs to *today*. If she wore Thursday's render on Friday, the journal is wrong. (Photo logging covers the gap, but the styled path should take a date.)
2. **No reminder loop** — a daily "what did you wear?" nudge (notification at a chosen hour) is the single biggest retention lever not yet built. Needs service-worker push; scoped for a native/PWA-push pass.
3. **Journal photos aren't garment-aware** — an OOTD photo could feed the same detection pipeline as imports ("tag the pieces in this photo"), closing the loop between journal and closet, powering true most-worn stats from real life.
4. **Month view** — after ~6 weeks of entries, week-by-week paging back gets tedious; a month grid (Instagram-profile feel) is the natural evolution.
5. **Render latency is masked, not short** — notification-on-ready helps, but the first-look experience still depends on provider speed; needs real-key validation.
6. **Mobile nav ergonomics** — the view switcher sits at the top; on tall phones a bottom tab bar would be one-thumb friendly.

## What already works for her

Feelings-first styling (the mood chips read like her group chat, not a menswear catalog), occasion presets including "Date night" and "Wedding guest", the weekly collage (made for stories), streaks, honest share states, wishlist and laundry flags, price-per-wear — and now the journal costs nothing to keep.
