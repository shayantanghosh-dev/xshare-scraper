"""
Devfolio scraper — Next.js site, data is in window.__NEXT_DATA__.
Strategy:
  1. Load the page with Playwright
  2. Pull hackathon data from window.__NEXT_DATA__ (fastest)
  3. If that's empty, fall back to scraping DOM card elements directly
  4. Scroll to trigger infinite-scroll pagination
  5. Runs on both /open and /upcoming tabs
"""

import asyncio
import sys
import os
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_client import supabase
from playwright.async_api import async_playwright

SOURCE = "devfolio"
BATCH_SIZE = 500


# ════════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════════

def parse_deadline(date_str):
    """Handle ISO datetimes and DD/MM/YY card format."""
    if not date_str:
        return ""
    s = str(date_str)
    # ISO format: 2026-07-25T00:00:00Z
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass
    # Card format: "25/07/26"
    try:
        dt = datetime.strptime(s, "%d/%m/%y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    return s


def normalize_mode(raw):
    if not raw:
        return "online"
    r = raw.lower()
    if "offline" in r or "in-person" in r or "onsite" in r:
        return "offline"
    return "online"


def build_record_from_api(h):
    """Build DB record from a __NEXT_DATA__ hackathon object."""
    slug = h.get("slug", "")
    url = f"https://{slug}.devfolio.co" if slug else ""

    themes = h.get("themes") or []
    tags = ", ".join(
        (t.get("name", "") if isinstance(t, dict) else str(t)) for t in themes
    )

    prize_pool = h.get("prize_pool") or h.get("prize_amount") or 0
    try:
        prize = f"${int(prize_pool):,}" if int(prize_pool) > 0 else "Certificates/Others"
    except (ValueError, TypeError):
        prize = "Certificates/Others"

    setting = h.get("hackathon_setting") or {}
    mode = normalize_mode(
        setting.get("hackathon_mode") or h.get("mode") or h.get("hackathon_type") or ""
    )

    cover = (
        h.get("cover_image_url") or h.get("banner_image_url") or
        h.get("logo_url") or h.get("favicon") or ""
    )

    org = h.get("organization") or {}
    organizer = org.get("name", "") if isinstance(org, dict) else h.get("org_name", "")

    return {
        "title":      (h.get("title") or h.get("name") or "").strip(),
        "organizer":  organizer,
        "deadline":   parse_deadline(h.get("ends_at") or h.get("end_date") or ""),
        "prize":      prize,
        "mode":       mode,
        "tags":       tags,
        "source_url": url,
        "source":     SOURCE,
        "image_url":  cover,
        "status":     "pending",
    }


def build_record_from_dom(card):
    """Build DB record from a DOM-scraped card dict."""
    return {
        "title":      card.get("title", "").strip(),
        "organizer":  card.get("organizer", ""),
        "deadline":   parse_deadline(card.get("deadline", "")),
        "prize":      "See Devfolio",
        "mode":       normalize_mode(card.get("mode", "")),
        "tags":       card.get("tags", ""),
        "source_url": card.get("url", ""),
        "source":     SOURCE,
        "image_url":  "",
        "status":     "pending",
    }


# ════════════════════════════════════════════════════════════════════
#  STRATEGY 1 — window.__NEXT_DATA__
# ════════════════════════════════════════════════════════════════════

async def extract_next_data(page):
    """Walk the Next.js embedded JSON to find hackathon arrays."""
    try:
        raw = await page.evaluate("() => JSON.stringify(window.__NEXT_DATA__ || {})")
        data = json.loads(raw)
        page_props = data.get("props", {}).get("pageProps", {})

        found = []

        def walk(obj, depth=0):
            """Recursively find any list that looks like hackathons."""
            if depth > 6:
                return
            if isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict) and ("slug" in item or "title" in item):
                        found.append(item)
            elif isinstance(obj, dict):
                for v in obj.values():
                    walk(v, depth + 1)

        walk(page_props)
        return found

    except Exception as e:
        print(f"  __NEXT_DATA__ error: {e}")
        return []


# ════════════════════════════════════════════════════════════════════
#  STRATEGY 2 — DOM card scraping
# ════════════════════════════════════════════════════════════════════

async def scrape_dom_cards(page):
    """
    Devfolio renders hackathon cards as anchor elements.
    Extract title, URL, mode, start date from visible card text.
    """
    try:
        cards = await page.evaluate("""
        () => {
            const results = [];
            const seen = new Set();

            document.querySelectorAll('a[href]').forEach(a => {
                const href = a.href || '';
                // Only devfolio subdomain links (actual hackathon pages)
                if (!href.match(/https:\\/\\/[a-z0-9-]+\\.devfolio\\.co/)) return;
                if (seen.has(href)) return;
                seen.add(href);

                const text = a.innerText || '';
                const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);
                if (lines.length < 1 || !lines[0]) return;

                // Mode
                const modeMatch = text.match(/\\b(OFFLINE|ONLINE)\\b/i);

                // Date shown on card e.g. "STARTS 25/07/26" or "LIVE"
                const dateMatch = text.match(/STARTS\\s+(\\d{2}\\/\\d{2}\\/\\d{2})/i);

                // Tags
                const tagMatch = text.match(/(NO RESTRICTIONS|AI\\/ML|FINTECH|BLOCKCHAIN|WEB3|HEALTH|EDU|GAMING)/i);

                results.push({
                    title:    lines[0],
                    url:      href,
                    mode:     modeMatch ? modeMatch[1] : '',
                    deadline: dateMatch ? dateMatch[1] : '',
                    tags:     tagMatch  ? tagMatch[1]  : '',
                    organizer: '',
                });
            });

            return results;
        }
        """)
        return cards or []
    except Exception as e:
        print(f"  DOM scrape error: {e}")
        return []


# ════════════════════════════════════════════════════════════════════
#  FETCH
# ════════════════════════════════════════════════════════════════════

async def fetch_hackathons(max_scrolls=25):
    all_items = []   # list of ("api"|"dom", dict)
    seen_ids = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        for tab_url in [
            "https://devfolio.co/hackathons/open",
            "https://devfolio.co/hackathons/upcoming",
        ]:
            print(f"\n  ── Loading {tab_url}")
            try:
                await page.goto(tab_url, wait_until="networkidle", timeout=35000)
            except Exception:
                try:
                    await page.goto(tab_url, wait_until="domcontentloaded", timeout=35000)
                except Exception as e:
                    print(f"  ⚠ Could not load {tab_url}: {e}")
                    continue
            await page.wait_for_timeout(3000)

            stable = 0
            prev = len(all_items)

            for i in range(max_scrolls + 1):   # +1 so we scan before first scroll too
                # Try __NEXT_DATA__
                next_items = await extract_next_data(page)
                for h in next_items:
                    uid = h.get("slug") or h.get("id") or h.get("title", "")
                    if uid and uid not in seen_ids:
                        seen_ids.add(uid)
                        all_items.append(("api", h))

                # Try DOM cards
                dom_items = await scrape_dom_cards(page)
                for c in dom_items:
                    uid = c.get("url", "")
                    if uid and uid not in seen_ids:
                        seen_ids.add(uid)
                        all_items.append(("dom", c))

                print(f"  Scroll {i}: total captured = {len(all_items)}")

                # Stop scrolling if no progress
                if len(all_items) == prev:
                    stable += 1
                    if stable >= 3:
                        print(f"  Stable for 3 rounds — done with this tab.")
                        break
                else:
                    stable = 0
                    prev = len(all_items)

                if i < max_scrolls:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2500)

        await browser.close()

    return all_items


# ════════════════════════════════════════════════════════════════════
#  SAVE
# ════════════════════════════════════════════════════════════════════

def save_to_supabase(raw_items):
    if not raw_items:
        return 0

    records = []
    for source_type, item in raw_items:
        try:
            rec = build_record_from_api(item) if source_type == "api" else build_record_from_dom(item)
            if rec["source_url"] and rec["title"]:
                records.append(rec)
        except Exception as e:
            print(f"  ⚠ Record error: {e}")

    # Batch dedup
    existing = supabase.table("hackathons")\
        .select("source_url").eq("source", SOURCE).execute()
    existing_urls = {r["source_url"] for r in (existing.data or [])}

    new_records = [r for r in records if r["source_url"] not in existing_urls]
    print(f"  New: {len(new_records)} | Skipped: {len(records) - len(new_records)}")

    saved = 0
    for i in range(0, len(new_records), BATCH_SIZE):
        chunk = new_records[i : i + BATCH_SIZE]
        try:
            supabase.table("hackathons").insert(chunk).execute()
            saved += len(chunk)
            print(f"  ✓ Batch {i // BATCH_SIZE + 1}: {len(chunk)} records inserted")
        except Exception as e:
            print(f"  ✗ Batch error: {e}")

    return saved


# ════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 50)
    print("DEVFOLIO SCRAPER")
    print("=" * 50)
    items = asyncio.run(fetch_hackathons(max_scrolls=25))
    print(f"\nTotal captured: {len(items)}")
    if items:
        saved = save_to_supabase(items)
        print(f"\n✅ Done — {saved} new hackathons saved.")
    else:
        print("❌ No hackathons captured — check Railway logs.")