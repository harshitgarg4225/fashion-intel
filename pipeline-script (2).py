"""
Fashion Intelligence Pipeline v2
=================================
Multi-user. Supabase backend. Gmail alerts. Self-learning.
Deploy on Railway with cron: 0 6 * * *

Environment variables:
- SUPABASE_URL          → from Supabase dashboard → Settings → API
- SUPABASE_SERVICE_KEY  → from Supabase dashboard → Settings → API → service_role key
- CLAUDE_API_KEY        → from console.anthropic.com
- FIRECRAWL_API_KEY     → from firecrawl.dev
- GMAIL_CREDENTIALS     → Base64-encoded Google service account JSON (see setup guide)
- GMAIL_ADDRESS         → your central alerts Gmail address
- TWILIO_SID            → from twilio.com (optional)
- TWILIO_AUTH           → from twilio.com (optional)
- TWILIO_FROM           → whatsapp:+14155238886 (optional)
- APP_URL               → your Vercel app URL (for links in WhatsApp messages)
"""

import os
import json
import time
import base64
import re
import requests
from datetime import datetime, timezone, timedelta

# ============================================================
# CONFIG
# ============================================================

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service_role key bypasses RLS
CLAUDE_KEY = os.environ["CLAUDE_API_KEY"]
FIRECRAWL_KEY = os.environ["FIRECRAWL_API_KEY"]
GMAIL_CREDS = os.environ.get("GMAIL_CREDENTIALS", "")
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
TWILIO_SID = os.environ.get("TWILIO_SID")
TWILIO_AUTH = os.environ.get("TWILIO_AUTH")
TWILIO_FROM = os.environ.get("TWILIO_FROM")
APP_URL = os.environ.get("APP_URL", "")

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

CLAUDE_HEADERS = {
    "x-api-key": CLAUDE_KEY,
    "anthropic-version": "2023-06-01",
    "Content-Type": "application/json"
}

STYLE_TAXONOMY = """
1. Quiet Luxury — Understated elegance, premium fabrics, no visible branding, neutral earth tones
2. Clean Minimalist — Pared-back, monochrome, geometric cuts, zero embellishment
3. Modern Tailoring — Soft-structured suiting, relaxed blazers, deconstructed, breathable
4. Smart Casual / Elevated Basics — Polo-chino-loafer territory, well-made essentials
5. Linen & Warm Climate — Breathable, relaxed, resort-adjacent, natural textures
6. Workwear / Utility — Patch pockets, sturdy fabrics, tool-inspired but elevated
7. Streetwear Minimal — Clean sneaker culture, oversized but controlled, monochrome
8. Prep / Ivy — Ivy League heritage, OCBDs, chinos, collegiate spirit
9. Artisanal / Handcraft — Handwoven, natural dyes, visible craft, slow fashion
10. Dopamine / Color-Forward — Confident saturated tones, color blocking, mood-lifting
11. Athleisure / Performance Casual — Technical fabrics, minimal design, comfort-first
12. Vintage / Retro Revival — Decade-specific references, archival reproductions
13. Avant-Garde / Experimental — Deconstructed, asymmetric, conceptual, anti-fashion
14. Indian Contemporary — Indian textile craft meets global silhouettes, kurta-adjacent
15. Coastal / Resort — Vacation dressing, tropical, swim-adjacent, island energy
16. Sustainable / Conscious — Sustainability as design identity, organic, circular
"""

RSS_FEEDS = [
    "https://www.highsnobiety.com/feed/",
    "https://hypebeast.com/feed",
    "https://www.gq.com/feed/rss",
]

# ============================================================
# HELPERS
# ============================================================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def sb_query(table, params=""):
    """GET from Supabase table with optional query params."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}?{params}",
        headers=SUPABASE_HEADERS
    )
    resp.raise_for_status()
    return resp.json()


def sb_insert(table, data):
    """INSERT into Supabase table. Data can be dict or list of dicts."""
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SUPABASE_HEADERS,
        json=data if isinstance(data, list) else [data]
    )
    if resp.status_code not in [200, 201]:
        log(f"  ⚠ Insert to {table} failed: {resp.status_code} — {resp.text[:200]}")
    return resp.json() if resp.status_code in [200, 201] else []


def sb_update(table, match_col, match_val, data):
    """UPDATE rows in Supabase table matching a condition."""
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}?{match_col}=eq.{match_val}",
        headers=SUPABASE_HEADERS,
        json=data
    )
    if resp.status_code not in [200, 204]:
        log(f"  ⚠ Update {table} failed: {resp.status_code} — {resp.text[:200]}")
    return resp


def call_claude(system_prompt, user_message, max_tokens=4000):
    """Call Claude API and return text response."""
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=CLAUDE_HEADERS,
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_message}]
        }
    )
    resp.raise_for_status()
    return "".join(b["text"] for b in resp.json()["content"] if b["type"] == "text")


def parse_json_response(text):
    """Parse JSON from Claude, handling markdown fences."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return json.loads(cleaned.strip())


def firecrawl_scrape(url):
    """Scrape a URL via Firecrawl. Returns markdown."""
    resp = requests.post(
        "https://api.firecrawl.dev/v1/scrape",
        headers={"Authorization": f"Bearer {FIRECRAWL_KEY}", "Content-Type": "application/json"},
        json={"url": url, "formats": ["markdown"]}
    )
    if resp.status_code != 200:
        log(f"  ⚠ Firecrawl failed for {url}: {resp.status_code}")
        return None
    return resp.json().get("data", {}).get("markdown", "")


# ============================================================
# STEP 0 — Load user configs + scrape sources
# ============================================================

def load_configs():
    """Load all user configs from Supabase."""
    log("Loading user configs...")
    configs = sb_query("user_configs", "select=*,users(email)")
    log(f"  Found {len(configs)} user(s)")
    return configs


def load_scrape_sources():
    """Load active scrape sources."""
    return sb_query("scrape_sources", "active=eq.true")


def get_memory(agent_type, user_id=None, limit=30):
    """Read agent memories from Supabase."""
    params = f"agent_type=eq.{agent_type}&order=created_at.desc&limit={limit}"
    if user_id:
        params += f"&user_id=eq.{user_id}"
    results = sb_query("agent_memories", params)
    if not results:
        return "No learnings yet. This is the first run."
    return "\n".join(f"- {r['learning']}: {r.get('evidence', '')}" for r in results)


# ============================================================
# STEP 1 — FETCH: Websites + RSS + Gmail
# ============================================================

def step_fetch_websites():
    """Scrape all active brand URLs."""
    log("STEP 1a: Scraping brand websites...")
    sources = load_scrape_sources()
    items_added = 0

    for source in sources:
        log(f"  Scraping {source['brand_name']}...")
        content = firecrawl_scrape(source["url"])
        if not content:
            continue

        sb_insert("items", {
            "date_fetched": TODAY,
            "source_type": source["source_type"],
            "brand_name": source["brand_name"],
            "item_title": f"{source['brand_name']} — New Arrivals {TODAY}",
            "description": content[:5000],
            "source_url": source["url"],
            "processed": False
        })

        # Update last_scraped
        sb_update("scrape_sources", "id", source["id"], {"last_scraped": datetime.now(timezone.utc).isoformat()})
        items_added += 1
        time.sleep(1)

    log(f"  ✓ Fetched from {items_added} brand websites")


def step_fetch_rss():
    """Fetch RSS feeds."""
    log("STEP 1b: Fetching RSS...")
    try:
        import feedparser
    except ImportError:
        log("  ⚠ feedparser not installed. Skipping.")
        return

    items_added = 0
    for feed_url in RSS_FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries[:5]:
            sb_insert("items", {
                "date_fetched": TODAY,
                "source_type": "editorial",
                "brand_name": "Editorial",
                "item_title": entry.get("title", "Untitled")[:200],
                "description": entry.get("summary", "")[:5000],
                "source_url": entry.get("link", ""),
                "processed": False
            })
            items_added += 1
            time.sleep(0.3)

    log(f"  ✓ Added {items_added} RSS items")


def step_fetch_gmail():
    """Read Google Alerts from central Gmail inbox."""
    log("STEP 1c: Reading Gmail alerts...")

    if not GMAIL_CREDS:
        log("  Gmail not configured. Skipping.")
        return

    try:
        # Decode service account credentials
        creds_json = json.loads(base64.b64decode(GMAIL_CREDS))

        # Use Google's OAuth2 to get access token
        import jwt as pyjwt  # PyJWT library
        now = int(time.time())
        payload = {
            "iss": creds_json["client_email"],
            "sub": GMAIL_ADDRESS,
            "scope": "https://www.googleapis.com/auth/gmail.readonly",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now,
            "exp": now + 3600
        }
        signed_jwt = pyjwt.encode(payload, creds_json["private_key"], algorithm="RS256")

        token_resp = requests.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt
        })
        access_token = token_resp.json()["access_token"]

        # Fetch recent messages from Google Alerts
        gmail_headers = {"Authorization": f"Bearer {access_token}"}
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y/%m/%d")
        search_resp = requests.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages?q=from:googlealerts-noreply@google.com after:{yesterday}&maxResults=20",
            headers=gmail_headers
        )
        messages = search_resp.json().get("messages", [])

        items_added = 0
        for msg_meta in messages[:20]:
            msg_resp = requests.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_meta['id']}?format=full",
                headers=gmail_headers
            )
            msg = msg_resp.json()

            # Extract subject
            subject = ""
            for header in msg.get("payload", {}).get("headers", []):
                if header["name"].lower() == "subject":
                    subject = header["value"]
                    break

            # Extract body snippet
            snippet = msg.get("snippet", "")

            sb_insert("items", {
                "date_fetched": TODAY,
                "source_type": "editorial",
                "brand_name": "Google Alert",
                "item_title": subject[:200],
                "description": snippet[:5000],
                "processed": False
            })
            items_added += 1
            time.sleep(0.3)

        log(f"  ✓ Added {items_added} Gmail alert items")

    except Exception as e:
        log(f"  ⚠ Gmail fetch failed: {e}")


# ============================================================
# STEP 2 — CATEGORIZE
# ============================================================

def step_categorize(configs):
    """Categorize all unprocessed items."""
    log("STEP 2: Running Categorizer Agent...")

    raw_items = sb_query("items", "processed=eq.false&select=id,item_title,brand_name,description,source_url")

    if not raw_items:
        log("  No unprocessed items. Skipping.")
        return

    log(f"  Found {len(raw_items)} unprocessed items")

    memory = get_memory("categorizer")

    # Combine all user styles for a broad categorization lens
    all_styles = set()
    for c in configs:
        all_styles.update(c.get("styles", []))
    style_focus = ", ".join(all_styles) if all_styles else "all styles"

    items_text = ""
    item_ids = []
    for r in raw_items:
        items_text += f"\n---\nTitle: {r['item_title']}\nBrand: {r['brand_name']}\nDescription: {(r.get('description') or '')[:500]}\nURL: {r.get('source_url', '')}\n"
        item_ids.append(r["id"])

    system_prompt = f"""You are a fashion categorization engine. Users of this system focus on: {style_focus}.

Assign each item to exactly ONE primary style category. Optionally assign ONE secondary.

THE 16 STYLE CATEGORIES:
{STYLE_TAXONOMY}

RULES:
- If raw text contains multiple products from one brand page, extract and categorize EACH separately (up to 10 per brand).
- Be decisive. Pick the most relevant category.

ACCUMULATED LEARNINGS:
{memory}

RESPOND ONLY IN JSON:
{{"items": [{{"item_title": "...", "brand": "...", "description": "one line", "primary_style": "...", "secondary_style": null}}]}}"""

    response = call_claude(system_prompt, f"Categorize:\n{items_text}")
    parsed = parse_json_response(response)
    categorized = parsed.get("items", [])

    log(f"  Claude extracted {len(categorized)} items")

    # Update existing items with style tags, or insert new extracted items
    for item in categorized:
        # Try to find matching raw item to update
        matched = False
        for raw_id in item_ids:
            raw = next((r for r in raw_items if r["id"] == raw_id and r["brand_name"] == item.get("brand")), None)
            if raw and not matched:
                sb_update("items", "id", raw_id, {
                    "primary_style": item["primary_style"],
                    "secondary_style": item.get("secondary_style"),
                    "description": item.get("description", "")[:5000],
                    "processed": True
                })
                matched = True
                break

        if not matched:
            # Claude extracted a sub-item from a multi-product page
            sb_insert("items", {
                "date_fetched": TODAY,
                "source_type": "drop",
                "brand_name": item.get("brand", "Unknown"),
                "item_title": item["item_title"][:200],
                "description": item.get("description", "")[:5000],
                "primary_style": item["primary_style"],
                "secondary_style": item.get("secondary_style"),
                "processed": True
            })
        time.sleep(0.2)

    # Mark any remaining unmatched raw items as processed
    for raw_id in item_ids:
        sb_update("items", "id", raw_id, {"processed": True})

    log(f"  ✓ Categorized {len(categorized)} items")


# ============================================================
# STEP 3 — SUB-FINDER (per user)
# ============================================================

def step_subfinder(configs):
    """Find lookalikes for today's items, informed by each user's preferences."""
    log("STEP 3: Running Sub-finder Agent...")

    # Get today's items without lookalikes
    items = sb_query("items", f"date_fetched=eq.{TODAY}&processed=eq.true&lookalike_1=is.null&primary_style=not.is.null")

    if not items:
        log("  No items need lookalikes. Skipping.")
        return

    log(f"  Finding lookalikes for {len(items)} items")

    # Aggregate all competitor brands to exclude
    all_competitors = set()
    all_styles = set()
    for c in configs:
        all_competitors.update(c.get("competitors", []))
        all_styles.update(c.get("styles", []))

    memory = get_memory("subfinder")
    competitors_str = ", ".join(all_competitors)
    styles_str = ", ".join(all_styles)

    items_text = ""
    item_map = {}
    for r in items:
        items_text += f"\n---\nTitle: {r['item_title']}\nBrand: {r['brand_name']}\nStyle: {r.get('primary_style', '')}\nDescription: {(r.get('description') or '')[:300]}\n"
        item_map[r["item_title"]] = r["id"]

    system_prompt = f"""You are a fashion market researcher for menswear.
For each item, suggest exactly 5 similar brands.

USER BASE CONTEXT:
- Users are interested in: {styles_str}
- Competitors already tracked (DO NOT suggest these): {competitors_str}

RULES:
1. At least 2 of 5 must be lesser-known brands
2. At least 1 accessible/shippable to India
3. Match on SILHOUETTE and AESTHETIC primarily
4. Provide actual website URLs. Mark uncertain with (verify)
5. Prefer independent brands over mainstream

LEARNINGS:
{memory}

RESPOND ONLY IN JSON:
{{"items": [{{"item_title": "...", "lookalikes": [{{"brand_name": "...", "url": "https://...", "why_similar": "one line"}}]}}]}}"""

    response = call_claude(system_prompt, f"Find lookalikes:\n{items_text}")
    parsed = parse_json_response(response)

    for item in parsed.get("items", []):
        item_id = item_map.get(item["item_title"])
        if not item_id:
            continue

        update = {}
        for i, la in enumerate(item.get("lookalikes", [])[:5]):
            update[f"lookalike_{i+1}"] = f"{la['brand_name']} — {la['why_similar']} — {la['url']}"

        sb_update("items", "id", item_id, update)
        time.sleep(0.2)

    log(f"  ✓ Updated {len(parsed.get('items', []))} items with lookalikes")


# ============================================================
# STEP 4 — WHATSAPP SUMMARY (per user)
# ============================================================

def step_send_summaries(configs):
    """Generate and send daily WhatsApp summary per user."""
    log("STEP 4: Sending daily summaries...")

    today_items = sb_query("items", f"date_fetched=eq.{TODAY}&processed=eq.true&primary_style=not.is.null")

    if not today_items:
        log("  No items today. Skipping.")
        return

    for config in configs:
        user_styles = set(config.get("styles", []))
        whatsapp = config.get("whatsapp", "")
        if not whatsapp:
            continue

        # Filter items relevant to this user's styles
        relevant = [i for i in today_items if i.get("primary_style") in user_styles or i.get("secondary_style") in user_styles]
        if not relevant:
            relevant = today_items[:10]  # Fallback: show top 10

        items_text = "\n".join(f"- {i['brand_name']}: {i['item_title']} [{i.get('primary_style', '')}]" for i in relevant)

        summary = call_claude(
            """You write daily WhatsApp messages for a fashion brand owner. Max 5 lines. 
Start with item count. Highlight the most interesting pattern or drop. 
End with "Tap to explore →" and the link. Tone: sharp, no emojis, direct.
RESPOND WITH ONLY THE MESSAGE TEXT.""",
            f"Items:\n{items_text}\n\nDashboard: {APP_URL}/dashboard",
            max_tokens=300
        )

        if TWILIO_SID and TWILIO_AUTH:
            resp = requests.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                auth=(TWILIO_SID, TWILIO_AUTH),
                data={"From": TWILIO_FROM, "To": f"whatsapp:{whatsapp}", "Body": summary.strip()}
            )
            status = "sent" if resp.status_code in [200, 201] else f"failed ({resp.status_code})"
            log(f"  {config.get('brand_name', 'User')}: WhatsApp {status}")
        else:
            log(f"  {config.get('brand_name', 'User')}: {summary.strip()[:100]}...")


# ============================================================
# STEP 5 — REFLECTION
# ============================================================

def step_reflect():
    """Run reflection agents based on engagement data."""
    log("STEP 5: Running reflection...")

    engagement = sb_query("engagement", "order=created_at.desc&limit=50")
    if len(engagement) < 3:
        log("  Not enough engagement data yet. Skipping.")
        return

    eng_text = "\n".join(
        f"- item {e.get('item_id', '?')}: {e['action']}, lookalike: {e.get('lookalike_clicked', 'none')}"
        for e in engagement
    )

    for agent_type in ["categorizer", "subfinder"]:
        memory = get_memory(agent_type)
        prompt = f"""Review the {agent_type}'s performance based on user engagement.
ENGAGEMENT:\n{eng_text}\nPREVIOUS LEARNINGS:\n{memory}
RESPOND ONLY IN JSON:
{{"learnings": [{{"learning": "...", "evidence": "...", "confidence": "high/medium/low"}}]}}
If inconclusive: {{"learnings": []}}"""

        response = call_claude(prompt, "What should change?")
        parsed = parse_json_response(response)

        for l in parsed.get("learnings", []):
            sb_insert("agent_memories", {
                "agent_type": agent_type,
                "learning": l["learning"],
                "evidence": l.get("evidence", ""),
                "confidence": l.get("confidence", "medium")
            })

        log(f"  {agent_type}: {len(parsed.get('learnings', []))} new learnings")


# ============================================================
# STEP 6 — AUTO-RESOLVE NEW BRAND URLs
# ============================================================

def step_resolve_new_brands(configs):
    """Check if any user's competitors are missing from scrape_sources. Auto-add."""
    log("STEP 6: Checking for new brands to track...")

    existing = sb_query("scrape_sources", "select=brand_name")
    existing_brands = {s["brand_name"].lower() for s in existing}

    new_brands = set()
    for c in configs:
        for comp in c.get("competitors", []):
            if comp.lower() not in existing_brands:
                new_brands.add(comp)

    if not new_brands:
        log("  No new brands to resolve.")
        return

    log(f"  Found {len(new_brands)} new brands: {', '.join(new_brands)}")

    for brand in new_brands:
        # Ask Claude for the brand's new arrivals URL
        response = call_claude(
            "You help find fashion brand website URLs. Return ONLY a JSON object, nothing else.",
            f'What is the new arrivals or latest collection page URL for the menswear brand "{brand}"? '
            f'Return: {{"brand": "{brand}", "url": "https://...", "confidence": "high/medium/low"}}'
        )
        try:
            parsed = parse_json_response(response)
            url = parsed.get("url", "")
            if url and url.startswith("http"):
                # Verify URL works via Firecrawl test
                test = firecrawl_scrape(url)
                if test and len(test) > 100:
                    sb_insert("scrape_sources", {
                        "brand_name": brand,
                        "url": url,
                        "source_type": "drop",
                        "active": True,
                        "signal_quality": "medium"
                    })
                    log(f"  ✓ Added {brand}: {url}")
                else:
                    log(f"  ⚠ {brand}: URL returned empty content, skipping")
            else:
                log(f"  ⚠ {brand}: Claude couldn't find a valid URL")
        except Exception as e:
            log(f"  ⚠ {brand}: Failed to resolve — {e}")

        time.sleep(1)


# ============================================================
# MAIN
# ============================================================

def main():
    log("=" * 60)
    log(f"FASHION INTEL PIPELINE v2 — {TODAY}")
    log("=" * 60)

    try:
        configs = load_configs()
        if not configs:
            log("✗ No user configs found. Users need to sign up first.")
            return
    except Exception as e:
        log(f"✗ Failed to load configs: {e}")
        return

    steps = [
        ("Resolve new brands", lambda: step_resolve_new_brands(configs)),
        ("Fetch websites", step_fetch_websites),
        ("Fetch RSS", step_fetch_rss),
        ("Fetch Gmail", step_fetch_gmail),
        ("Categorize", lambda: step_categorize(configs)),
        ("Sub-finder", lambda: step_subfinder(configs)),
        ("Send summaries", lambda: step_send_summaries(configs)),
        ("Reflect", step_reflect),
    ]

    for name, func in steps:
        try:
            func()
        except Exception as e:
            log(f"✗ {name} failed: {e}")

    log("=" * 60)
    log("PIPELINE COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()
