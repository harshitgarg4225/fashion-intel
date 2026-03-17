"""
Fashion Intelligence Pipeline
==============================
One script. Runs daily. Fills your Notion with categorized fashion drops + lookalikes.

Deploy on Railway with a cron schedule: 0 6 * * * (runs at 6 AM daily)

Required environment variables (set in Railway dashboard):
- NOTION_API_KEY        → from notion.so/my-integrations
- NOTION_DB_CONFIG      → Database ID for "User Config"
- NOTION_DB_RAW         → Database ID for "Raw Fetched Items"
- NOTION_DB_CATEGORIZED → Database ID for "Categorized Items"
- NOTION_DB_CAT_MEMORY  → Database ID for "Categorizer Memory"
- NOTION_DB_SUB_MEMORY  → Database ID for "Sub-finder Memory"
- NOTION_DB_FETCH_MEMORY→ Database ID for "Fetcher Memory"
- NOTION_DB_ENGAGEMENT  → Database ID for "User Engagement Log"
- CLAUDE_API_KEY        → from console.anthropic.com
- FIRECRAWL_API_KEY     → from firecrawl.dev
- TWILIO_SID            → from twilio.com (optional)
- TWILIO_AUTH           → from twilio.com (optional)
- TWILIO_FROM           → whatsapp:+14155238886 (optional)
- WHATSAPP_TO           → whatsapp:+91XXXXXXXXXX (optional)
- NOTION_LINK           → your Categorized Items Notion URL
"""

import os
import json
import time
import requests
from datetime import datetime, timezone

# ============================================================
# CONFIG — All from environment variables
# ============================================================

NOTION_KEY = os.environ["NOTION_API_KEY"]
NOTION_DB_RAW = os.environ["NOTION_DB_RAW"]
NOTION_DB_CATEGORIZED = os.environ["NOTION_DB_CATEGORIZED"]
NOTION_DB_CAT_MEMORY = os.environ["NOTION_DB_CAT_MEMORY"]
NOTION_DB_SUB_MEMORY = os.environ["NOTION_DB_SUB_MEMORY"]
NOTION_DB_FETCH_MEMORY = os.environ.get("NOTION_DB_FETCH_MEMORY", "")
NOTION_DB_ENGAGEMENT = os.environ.get("NOTION_DB_ENGAGEMENT", "")
CLAUDE_KEY = os.environ["CLAUDE_API_KEY"]
FIRECRAWL_KEY = os.environ["FIRECRAWL_API_KEY"]
TWILIO_SID = os.environ.get("TWILIO_SID")
TWILIO_AUTH = os.environ.get("TWILIO_AUTH")
TWILIO_FROM = os.environ.get("TWILIO_FROM")
WHATSAPP_TO = os.environ.get("WHATSAPP_TO")
NOTION_LINK = os.environ.get("NOTION_LINK", "")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

CLAUDE_HEADERS = {
    "x-api-key": CLAUDE_KEY,
    "anthropic-version": "2023-06-01",
    "Content-Type": "application/json"
}

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ============================================================
# STYLE TAXONOMY
# ============================================================

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

# ============================================================
# BRAND URLs TO SCRAPE
# ============================================================

SCRAPE_URLS = [
    {"url": "https://www.uniqlo.com/us/en/men/uniqlo-u", "brand": "Uniqlo U", "source_type": "Drop"},
    {"url": "https://www.ralphlauren.com/men-new-arrivals", "brand": "Ralph Lauren", "source_type": "Drop"},
    {"url": "https://www.auralee.jp/en", "brand": "Auralee", "source_type": "Drop"},
    {"url": "https://www.armani.com/en-us/giorgio-armani/men/new-in", "brand": "Armani", "source_type": "Drop"},
    {"url": "https://www.cos.com/en/men/new-arrivals.html", "brand": "COS", "source_type": "Drop"},
    {"url": "https://www.lemaire.fr/en/men-new-arrivals", "brand": "Lemaire", "source_type": "Drop"},
    {"url": "https://www.apcstore.com/men/new-arrivals", "brand": "A.P.C.", "source_type": "Drop"},
    {"url": "https://www.studionicholson.com/collections/mens-new-in", "brand": "Studio Nicholson", "source_type": "Drop"},
    {"url": "https://www.sunspel.com/collections/mens-new-arrivals", "brand": "Sunspel", "source_type": "Drop"},
]

NOTION_DB_CONFIG = os.environ["NOTION_DB_CONFIG"]

RSS_FEEDS = [
    {"url": "https://www.highsnobiety.com/feed/", "source_type": "Editorial"},
    {"url": "https://hypebeast.com/feed", "source_type": "Editorial"},
    {"url": "https://www.gq.com/feed/rss", "source_type": "Editorial"},
]

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def log(msg):
    """Simple timestamped logging."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def notion_query(database_id, filter_obj=None, sorts=None, page_size=100):
    """Query a Notion database. Returns list of results."""
    body = {"page_size": page_size}
    if filter_obj:
        body["filter"] = filter_obj
    if sorts:
        body["sorts"] = sorts

    resp = requests.post(
        f"https://api.notion.com/v1/databases/{database_id}/query",
        headers=NOTION_HEADERS,
        json=body
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def notion_create_page(database_id, properties):
    """Create a new page (row) in a Notion database."""
    body = {
        "parent": {"database_id": database_id},
        "properties": properties
    }
    resp = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json=body
    )
    if resp.status_code != 200:
        log(f"  ⚠ Notion create failed: {resp.status_code} — {resp.text[:200]}")
    return resp


def notion_update_page(page_id, properties):
    """Update an existing Notion page."""
    resp = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties}
    )
    if resp.status_code != 200:
        log(f"  ⚠ Notion update failed: {resp.status_code} — {resp.text[:200]}")
    return resp


def call_claude(system_prompt, user_message, max_tokens=4000):
    """Call Claude API and return the text response."""
    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}]
    }
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=CLAUDE_HEADERS,
        json=body
    )
    resp.raise_for_status()
    data = resp.json()
    text = "".join(block["text"] for block in data["content"] if block["type"] == "text")
    return text


def parse_json_response(text):
    """Parse JSON from Claude's response, handling markdown fences."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()
    return json.loads(cleaned)


def firecrawl_scrape(url):
    """Scrape a URL using Firecrawl. Returns markdown content."""
    resp = requests.post(
        "https://api.firecrawl.dev/v1/scrape",
        headers={
            "Authorization": f"Bearer {FIRECRAWL_KEY}",
            "Content-Type": "application/json"
        },
        json={"url": url, "formats": ["markdown"]}
    )
    if resp.status_code != 200:
        log(f"  ⚠ Firecrawl failed for {url}: {resp.status_code}")
        return None
    data = resp.json()
    return data.get("data", {}).get("markdown", "")


def get_memory(database_id, limit=30):
    """Read the last N entries from a memory database."""
    results = notion_query(
        database_id,
        sorts=[{"property": "Date", "direction": "descending"}],
        page_size=limit
    )
    learnings = []
    for r in results:
        props = r["properties"]
        title_arr = props.get("Learning", props.get("Source", {})).get("title", [])
        title = title_arr[0]["plain_text"] if title_arr else ""
        evidence_arr = props.get("Evidence", props.get("Notes", {})).get("rich_text", [])
        evidence = evidence_arr[0]["plain_text"] if evidence_arr else ""
        learnings.append(f"- {title}: {evidence}")
    return "\n".join(learnings) if learnings else "No learnings yet. This is the first run."


def load_user_configs():
    """Load all user configs from the User Config database.
    Returns list of dicts with brand_name, styles, competitors, whatsapp."""
    log("Loading user configs from Notion...")
    results = notion_query(NOTION_DB_CONFIG)
    configs = []
    for r in results:
        props = r["properties"]
        name = props["User Name"]["title"][0]["plain_text"] if props["User Name"]["title"] else "Unknown"
        brand = props["Brand Name"]["rich_text"][0]["plain_text"] if props["Brand Name"]["rich_text"] else ""
        styles = [s["name"] for s in props["Brand Styles"].get("multi_select", [])]
        competitors = props["Competitor Brands"]["rich_text"][0]["plain_text"] if props["Competitor Brands"]["rich_text"] else ""
        # Parse WhatsApp from a "WhatsApp" column if it exists, otherwise skip
        wa_field = props.get("WhatsApp", {}).get("rich_text", [])
        whatsapp = wa_field[0]["plain_text"] if wa_field else ""

        configs.append({
            "user_name": name,
            "brand_name": brand,
            "styles": styles,
            "competitors": [c.strip() for c in competitors.split(",") if c.strip()],
            "whatsapp": whatsapp
        })
        log(f"  Loaded config for {name} — {brand} — {len(styles)} styles, {len(configs[-1]['competitors'])} competitors")

    return configs


# ============================================================
# STEP 1 — FETCH: Scrape brand websites
# ============================================================

def step_fetch_websites():
    """Scrape brand new arrivals pages and write to Raw Fetched Items."""
    log("STEP 1a: Scraping brand websites via Firecrawl...")
    items_added = 0

    for source in SCRAPE_URLS:
        log(f"  Scraping {source['brand']}...")
        content = firecrawl_scrape(source["url"])
        if not content:
            continue

        # Write raw content as a single entry per brand
        # The categorizer will extract individual items
        notion_create_page(NOTION_DB_RAW, {
            "Item Title": {"title": [{"text": {"content": f"{source['brand']} — New Arrivals {TODAY}"}}]},
            "Date Fetched": {"date": {"start": TODAY}},
            "Source Type": {"select": {"name": source["source_type"]}},
            "Brand Name": {"rich_text": [{"text": {"content": source["brand"]}}]},
            "Description": {"rich_text": [{"text": {"content": content[:2000]}}]},  # Notion limit
            "Source URL": {"url": source["url"]},
            "Processed": {"checkbox": False}
        })
        items_added += 1
        time.sleep(1)  # Rate limiting

    log(f"  ✓ Added {items_added} raw entries from websites")
    return items_added


# ============================================================
# STEP 1b — FETCH: Read RSS feeds
# ============================================================

def step_fetch_rss():
    """Fetch RSS feeds and write to Raw Fetched Items."""
    log("STEP 1b: Fetching RSS feeds...")
    items_added = 0

    try:
        import feedparser
    except ImportError:
        log("  ⚠ feedparser not installed. Skipping RSS. Add 'feedparser' to requirements.txt")
        return 0

    for feed_source in RSS_FEEDS:
        log(f"  Reading {feed_source['url']}...")
        feed = feedparser.parse(feed_source["url"])

        for entry in feed.entries[:5]:  # Last 5 items per feed
            title = entry.get("title", "Untitled")
            description = entry.get("summary", entry.get("description", ""))[:2000]
            link = entry.get("link", "")

            notion_create_page(NOTION_DB_RAW, {
                "Item Title": {"title": [{"text": {"content": title[:100]}}]},
                "Date Fetched": {"date": {"start": TODAY}},
                "Source Type": {"select": {"name": feed_source["source_type"]}},
                "Brand Name": {"rich_text": [{"text": {"content": "Editorial"}}]},
                "Description": {"rich_text": [{"text": {"content": description}}]},
                "Source URL": {"url": link if link else None},
                "Processed": {"checkbox": False}
            })
            items_added += 1
            time.sleep(0.5)  # Notion rate limit: 3 req/sec

    log(f"  ✓ Added {items_added} items from RSS")
    return items_added


# ============================================================
# STEP 2 — CATEGORIZE: Claude assigns styles to raw items
# ============================================================

def step_categorize(user_config):
    """Read unprocessed items, categorize with Claude, write to Categorized Items."""
    log("STEP 2: Running Categorizer Agent...")

    # Get unprocessed items
    raw_items = notion_query(NOTION_DB_RAW, filter_obj={
        "property": "Processed",
        "checkbox": {"equals": False}
    })

    if not raw_items:
        log("  No unprocessed items found. Skipping.")
        return []

    log(f"  Found {len(raw_items)} unprocessed items")

    # Get categorizer memory
    memory = get_memory(NOTION_DB_CAT_MEMORY)

    # Format items for Claude
    items_text = ""
    raw_map = {}  # Map title to page_id for marking processed
    for r in raw_items:
        props = r["properties"]
        title = props["Item Title"]["title"][0]["plain_text"] if props["Item Title"]["title"] else "Untitled"
        brand = props["Brand Name"]["rich_text"][0]["plain_text"] if props["Brand Name"]["rich_text"] else "Unknown"
        desc = props["Description"]["rich_text"][0]["plain_text"] if props["Description"]["rich_text"] else ""
        source_url = props["Source URL"].get("url", "")

        items_text += f"\n---\nTitle: {title}\nBrand: {brand}\nDescription: {desc[:500]}\nURL: {source_url}\n"
        raw_map[title] = r["id"]

    user_styles = ", ".join(user_config["styles"])

    # Call Claude
    system_prompt = f"""You are a fashion categorization engine for {user_config['brand_name']}, a brand positioned in: {user_styles}.

Assign each item to exactly ONE primary style category. Optionally assign ONE secondary style.

THE 16 STYLE CATEGORIES:
{STYLE_TAXONOMY}

DECISION RULES:
- Prioritize categories that align with the user's styles ({user_styles}) — items in these categories should be categorized with extra care.
- If an item could go in two categories, pick the one a brand owner in the {user_styles} space would most want to see.
- If raw text contains multiple products from one brand page, extract and categorize EACH product separately (up to 10 per brand).

ACCUMULATED LEARNINGS:
{memory}

RESPOND ONLY IN JSON. No preamble, no markdown backticks:
{{"items": [{{"item_title": "...", "brand": "...", "description": "one line", "primary_style": "...", "secondary_style": null}}]}}"""

    response = call_claude(system_prompt, f"Categorize these items:\n{items_text}")
    parsed = parse_json_response(response)
    categorized = parsed.get("items", [])

    log(f"  Claude categorized {len(categorized)} items")

    # Write to Categorized Items DB
    for item in categorized:
        props = {
            "Item Title": {"title": [{"text": {"content": item["item_title"][:100]}}]},
            "Date": {"date": {"start": TODAY}},
            "Brand Name": {"rich_text": [{"text": {"content": item.get("brand", "Unknown")}}]},
            "Description": {"rich_text": [{"text": {"content": item.get("description", "")[:2000]}}]},
            "Primary Style": {"select": {"name": item["primary_style"]}},
        }
        if item.get("secondary_style"):
            props["Secondary Style"] = {"select": {"name": item["secondary_style"]}}

        notion_create_page(NOTION_DB_CATEGORIZED, props)
        time.sleep(0.4)

    # Mark raw items as processed
    for title, page_id in raw_map.items():
        notion_update_page(page_id, {"Processed": {"checkbox": True}})
        time.sleep(0.4)

    log(f"  ✓ Wrote {len(categorized)} categorized items to Notion")
    return categorized


# ============================================================
# STEP 3 — SUB-FINDER: Claude finds 5 lookalike brands per item
# ============================================================

def step_subfinder(user_config):
    """Read today's categorized items, find lookalikes, update Notion."""
    log("STEP 3: Running Sub-finder Agent...")

    # Get today's items without lookalikes
    items = notion_query(NOTION_DB_CATEGORIZED, filter_obj={
        "and": [
            {"property": "Date", "date": {"equals": TODAY}},
            {"property": "Lookalike 1", "rich_text": {"is_empty": True}}
        ]
    })

    if not items:
        log("  No items need lookalikes. Skipping.")
        return

    log(f"  Finding lookalikes for {len(items)} items")

    # Get sub-finder memory
    memory = get_memory(NOTION_DB_SUB_MEMORY)

    competitors_str = ", ".join(user_config["competitors"])
    styles_str = ", ".join(user_config["styles"])

    # Format items
    items_text = ""
    item_map = {}  # Map title to page_id
    for r in items:
        props = r["properties"]
        title = props["Item Title"]["title"][0]["plain_text"] if props["Item Title"]["title"] else "Untitled"
        brand = props["Brand Name"]["rich_text"][0]["plain_text"] if props["Brand Name"]["rich_text"] else ""
        style = props["Primary Style"]["select"]["name"] if props["Primary Style"].get("select") else ""
        desc = props["Description"]["rich_text"][0]["plain_text"] if props["Description"]["rich_text"] else ""

        items_text += f"\n---\nTitle: {title}\nBrand: {brand}\nStyle: {style}\nDescription: {desc[:300]}\n"
        item_map[title] = r["id"]

    system_prompt = f"""You are a fashion market researcher specializing in menswear.
For each item, suggest exactly 5 brands doing something similar.

USER CONTEXT:
- Brand: {user_config['brand_name']}
- Positioning: {styles_str}
- Competitors already tracked: {competitors_str}
- Do NOT suggest any of these as lookalikes: {competitors_str}

RULES:
1. At least 2 of 5 must be lesser-known brands (small Japanese, Scandinavian, Korean, Indian labels)
2. At least 1 must be accessible/shippable to India
3. Match on SILHOUETTE and AESTHETIC primarily
4. Provide actual website URLs. Mark uncertain URLs with (verify)
5. Prefer independent brands over mainstream

ACCUMULATED LEARNINGS:
{memory}

RESPOND ONLY IN JSON:
{{"items": [{{"item_title": "...", "lookalikes": [{{"brand_name": "...", "url": "https://...", "why_similar": "one line"}}]}}]}}"""

    response = call_claude(system_prompt, f"Find 5 lookalikes for each:\n{items_text}")
    parsed = parse_json_response(response)

    for item in parsed.get("items", []):
        title = item["item_title"]
        page_id = item_map.get(title)
        if not page_id:
            continue

        lookalikes = item.get("lookalikes", [])
        update_props = {}
        for i, la in enumerate(lookalikes[:5]):
            field = f"Lookalike {i+1}"
            value = f"{la['brand_name']} — {la['why_similar']} — {la['url']}"
            update_props[field] = {"rich_text": [{"text": {"content": value[:2000]}}]}

        notion_update_page(page_id, update_props)
        time.sleep(0.4)

    log(f"  ✓ Updated {len(parsed.get('items', []))} items with lookalikes")


# ============================================================
# STEP 4 — SUMMARY + WHATSAPP: Generate and send daily brief
# ============================================================

def step_send_summary():
    """Generate daily summary and send via WhatsApp."""
    log("STEP 4: Generating daily summary...")

    # Get today's categorized items
    items = notion_query(NOTION_DB_CATEGORIZED, filter_obj={
        "property": "Date",
        "date": {"equals": TODAY}
    })

    if not items:
        log("  No items today. Skipping summary.")
        return

    # Format for Claude
    items_text = ""
    for r in items:
        props = r["properties"]
        title = props["Item Title"]["title"][0]["plain_text"] if props["Item Title"]["title"] else ""
        brand = props["Brand Name"]["rich_text"][0]["plain_text"] if props["Brand Name"]["rich_text"] else ""
        style = props["Primary Style"]["select"]["name"] if props["Primary Style"].get("select") else ""
        items_text += f"- {brand}: {title} [{style}]\n"

    system_prompt = """You are writing a daily WhatsApp message for a menswear brand owner in India.
He runs a mid-range quiet luxury / minimalist brand.

Write a message that is:
- Maximum 5 lines
- Starts with item count and style count
- Highlights the single most interesting pattern or drop
- Ends with "Tap to explore →" and the link
- Tone: sharp friend texting. No emojis. No hype. Clean and direct.

RESPOND WITH ONLY THE MESSAGE TEXT."""

    summary = call_claude(system_prompt, f"Today's items:\n{items_text}\n\nNotion link: {NOTION_LINK}")
    summary = summary.strip()

    log(f"  Summary: {summary[:100]}...")

    # Send via WhatsApp (Twilio)
    if TWILIO_SID and TWILIO_AUTH and WHATSAPP_TO:
        log("  Sending WhatsApp via Twilio...")
        resp = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
            auth=(TWILIO_SID, TWILIO_AUTH),
            data={
                "From": TWILIO_FROM,
                "To": WHATSAPP_TO,
                "Body": summary
            }
        )
        if resp.status_code in [200, 201]:
            log("  ✓ WhatsApp sent!")
        else:
            log(f"  ⚠ WhatsApp failed: {resp.status_code} — {resp.text[:200]}")
    else:
        log("  Twilio not configured. Summary printed above — send manually.")


# ============================================================
# STEP 5 — REFLECTION: Learn from user engagement (runs if data exists)
# ============================================================

def step_reflect():
    """Run reflection agents if engagement data exists."""
    log("STEP 5: Running reflection loop...")

    if not NOTION_DB_ENGAGEMENT:
        log("  Engagement DB not configured. Skipping.")
        return

    # Check if there's any engagement data
    engagement = notion_query(NOTION_DB_ENGAGEMENT, page_size=10)
    if len(engagement) < 3:
        log("  Not enough engagement data yet (need at least 3 entries). Skipping.")
        return

    # Format engagement data
    eng_text = ""
    for r in engagement:
        props = r["properties"]
        item = props["Item"]["title"][0]["plain_text"] if props["Item"]["title"] else ""
        action = props["Action"]["select"]["name"] if props["Action"].get("select") else ""
        style = props["Style Category"]["select"]["name"] if props["Style Category"].get("select") else ""
        la_clicked = props["Lookalike Clicked"]["rich_text"][0]["plain_text"] if props["Lookalike Clicked"]["rich_text"] else "none"
        eng_text += f"- {item} [{style}]: {action}, lookalike clicked: {la_clicked}\n"

    # ---- Categorizer Reflection ----
    cat_memory = get_memory(NOTION_DB_CAT_MEMORY)
    cat_prompt = """You are reviewing a fashion categorization system's performance.
Based on engagement data, identify items that were likely miscategorized.

RESPOND ONLY IN JSON:
{"learnings": [{"learning": "...", "evidence": "...", "confidence": "high/medium/low"}]}
If inconclusive: {"learnings": []}"""

    cat_response = call_claude(cat_prompt, f"ENGAGEMENT:\n{eng_text}\n\nPREVIOUS LEARNINGS:\n{cat_memory}")
    cat_parsed = parse_json_response(cat_response)

    for learning in cat_parsed.get("learnings", []):
        notion_create_page(NOTION_DB_CAT_MEMORY, {
            "Learning": {"title": [{"text": {"content": learning["learning"][:100]}}]},
            "Date": {"date": {"start": TODAY}},
            "Evidence": {"rich_text": [{"text": {"content": learning.get("evidence", "")[:2000]}}]},
            "Confidence": {"select": {"name": learning.get("confidence", "medium").capitalize()}}
        })
    log(f"  ✓ Categorizer: {len(cat_parsed.get('learnings', []))} new learnings")

    # ---- Sub-finder Reflection ----
    sub_memory = get_memory(NOTION_DB_SUB_MEMORY)
    sub_prompt = """You are reviewing a fashion lookalike recommendation system.
Based on which lookalike links the user clicked vs ignored, identify preference patterns.

RESPOND ONLY IN JSON:
{"learnings": [{"learning": "...", "evidence": "...", "confidence": "high/medium/low"}]}
If inconclusive: {"learnings": []}"""

    sub_response = call_claude(sub_prompt, f"ENGAGEMENT:\n{eng_text}\n\nPREVIOUS LEARNINGS:\n{sub_memory}")
    sub_parsed = parse_json_response(sub_response)

    for learning in sub_parsed.get("learnings", []):
        notion_create_page(NOTION_DB_SUB_MEMORY, {
            "Learning": {"title": [{"text": {"content": learning["learning"][:100]}}]},
            "Date": {"date": {"start": TODAY}},
            "Evidence": {"rich_text": [{"text": {"content": learning.get("evidence", "")[:2000]}}]},
            "Confidence": {"select": {"name": learning.get("confidence", "medium").capitalize()}}
        })
    log(f"  ✓ Sub-finder: {len(sub_parsed.get('learnings', []))} new learnings")


# ============================================================
# MAIN — Run the full pipeline
# ============================================================

def main():
    log("=" * 60)
    log(f"FASHION INTEL PIPELINE — {TODAY}")
    log("=" * 60)

    # Load user configs from Notion
    try:
        configs = load_user_configs()
        if not configs:
            log("✗ No user configs found. Add a row to User Config DB first.")
            return
        user_config = configs[0]  # For v1, use the first (primary) user
    except Exception as e:
        log(f"✗ Failed to load user configs: {e}")
        return

    try:
        step_fetch_websites()
    except Exception as e:
        log(f"  ✗ Fetch websites failed: {e}")

    try:
        step_fetch_rss()
    except Exception as e:
        log(f"  ✗ Fetch RSS failed: {e}")

    try:
        step_categorize(user_config)
    except Exception as e:
        log(f"  ✗ Categorizer failed: {e}")

    try:
        step_subfinder(user_config)
    except Exception as e:
        log(f"  ✗ Sub-finder failed: {e}")

    try:
        step_send_summary()
    except Exception as e:
        log(f"  ✗ Summary/WhatsApp failed: {e}")

    try:
        step_reflect()
    except Exception as e:
        log(f"  ✗ Reflection failed: {e}")

    log("=" * 60)
    log("PIPELINE COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()
