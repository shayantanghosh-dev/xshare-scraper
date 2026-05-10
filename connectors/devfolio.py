"""
Devfolio scraper — two-phase approach:
  Phase 1: Load listing pages (/open, /upcoming), scroll to collect all hackathon URLs
  Phase 2: Visit each hackathon's own page, extract full details from __NEXT_DATA__
"""

import asyncio
import sys
import os
import json
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_client import supabase
from playwright.async_api import async_playwright

SOURCE     = "devfolio"
BATCH_SIZE = 500


# ════════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════════

def parse_date(date_str):
    """Normalize any date format → date object."""
    if not date_str:
        return None
    s = str(date_str).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%y",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(s.replace("Z", ""), fmt.replace("%z", "")).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None


def format_date_range(start_str, end_str):
    """Build a human-readable date range like 'May 1 - May 20, 2026'."""
    start = parse_date(start_str)
    end   = parse_date(end_str)
    if start and end:
        if start.year == end.year:
            return f"{start.strftime('%b')} {start.day} - {end.strftime('%b')} {end.day}, {end.year}"
        return f"{start.strftime('%b')} {start.day}, {start.year} - {end.strftime('%b')} {end.day}, {end.year}"
    if end:
        return f"{end.strftime('%b')} {end.day}, {end.year}"
    return ""


def compute_days_left(end_date):
    """Return a human-readable days-left string."""
    if not end_date:
        return "Unknown"
    delta = (end_date - date.today()).days
    if delta < 0:
        return "Ended"
    if delta == 0:
        return "Ends today"
    return f"{delta} days left"


def normalize_location(raw):
    """Return (location_str, is_online)."""
    if not raw:
        return "Online", True
    r = str(raw).lower()
    if "offline" in r or "in-person" in r or "onsite" in r:
        return raw.strip(), False
    return "Online", True


# ════════════════════════════════════════════════════════════════════
#  PHASE 1 — collect hackathon URLs from listing pages
# ════════════════════════════════════════════════════════════════════

async def collect_urls(page, max_scrolls=30):
    urls = set()

    for tab in ["https://devfolio.co/hackathons/open",
                "https://devfolio.co/hackathons/upcoming"]:
        print(f"\n  Phase 1 — {tab}")
        try:
            await page.goto(tab, wait_until="networkidle", timeout=35000)
        except Exception:
            try:
                await page.goto(tab, wait_until="domcontentloaded", timeout=35000)
            except Exception as e:
                print(f"  ⚠ Could not load {tab}: {e}")
                continue

        await page.wait_for_timeout(3000)
        stable = prev = 0

        for i in range(max_scrolls + 1):
            found = await page.evaluate("""
            () => {
                const hrefs = new Set();
                document.querySelectorAll('a[href]').forEach(a => {
                    const h = a.href || '';
                    if (/^https:\\/\\/[a-z0-9-]+\\.devfolio\\.co\\/?$/.test(h)) {
                        hrefs.add(h.replace(/\\/$/, ''));
                    }
                });
                return [...hrefs];
            }
            """)

            for u in found:
                urls.add(u)

            print(f"    Scroll {i}: {len(urls)} unique URLs so far")

            if len(urls) == prev:
                stable += 1
                if stable >= 3:
                    print(f"    Stable — done with this tab.")
                    break
            else:
                stable = 0
                prev = len(urls)

            if i < max_scrolls:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)

    return list(urls)


# ════════════════════════════════════════════════════════════════════
#  PHASE 2 — visit each hackathon page, extract __NEXT_DATA__
# ════════════════════════════════════════════════════════════════════

async def extract_details(page, url):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await page.wait_for_timeout(1500)

        raw  = await page.evaluate("() => JSON.stringify(window.__NEXT_DATA__ || {})")
        data = json.loads(raw)
        pp   = data.get("props", {}).get("pageProps", {})

        h = (
            pp.get("hackathon") or
            pp.get("data") or
            pp.get("hackathonData") or
            {}
        )
        if not h:
            for v in pp.values():
                if isinstance(v, dict) and ("slug" in v or "title" in v or "ends_at" in v):
                    h = v
                    break

        # ── Title ─────────────────────────────────────────────────
        title = (h.get("title") or h.get("name") or "").strip()
        if not title:
            title = await page.evaluate("""
            () => {
                const og = document.querySelector('meta[property="og:title"]');
                return og ? og.content : document.title || '';
            }
            """) or ""
            title = title.replace(" | Devfolio", "").strip()
        if not title:
            return None

        # ── Tagline ───────────────────────────────────────────────
        tagline = (
            h.get("tagline") or h.get("short_description") or h.get("description") or ""
        ).strip()
        # Fall back to og:description
        if not tagline:
            tagline = await page.evaluate("""
            () => {
                const m = document.querySelector('meta[name="description"], meta[property="og:description"]');
                return m ? m.content : '';
            }
            """) or ""
        tagline = tagline.strip()

        # ── Dates ─────────────────────────────────────────────────
        starts_at = h.get("starts_at") or h.get("start_date") or ""
        ends_at   = h.get("ends_at")   or h.get("end_date")   or h.get("submission_deadline") or ""
        dates_str = format_date_range(starts_at, ends_at)
        end_date  = parse_date(ends_at)

        # ── Prize ─────────────────────────────────────────────────
        prize_raw = h.get("prize_pool") or h.get("prize_amount") or h.get("total_prizes") or 0
        try:
            prize = f"${int(prize_raw):,} in prizes" if int(prize_raw) > 0 else "Prizes available"
        except (ValueError, TypeError):
            prize_str = str(prize_raw).strip()
            prize = prize_str if prize_str else "Prizes available"

        # ── Participants ──────────────────────────────────────────
        participants_raw = (
            h.get("registrations_count") or
            h.get("participants_count") or
            h.get("total_registrations") or 0
        )
        participants = f"{int(participants_raw):,}" if participants_raw else "0"

        # ── Themes / Tags ─────────────────────────────────────────
        themes_raw = h.get("themes") or h.get("tags") or h.get("categories") or []
        if isinstance(themes_raw, list):
            themes = [
                (t.get("name", "") if isinstance(t, dict) else str(t))
                for t in themes_raw if t
            ]
        else:
            themes = [str(themes_raw)] if themes_raw else []

        # ── Location ──────────────────────────────────────────────
        setting  = h.get("hackathon_setting") or {}
        mode_raw = (
            setting.get("hackathon_mode") or
            h.get("mode") or
            h.get("hackathon_type") or ""
        )
        location_str, is_online = normalize_location(mode_raw)

        return {
            "source":       SOURCE,
            "title":        title,
            "url":          url,
            "tagline":      tagline,
            "dates":        dates_str,
            "prize":        prize,
            "participants": participants,
            "location":     location_str,
            "themes":       themes,
            "isOnline":     is_online,
            "daysLeft":     compute_days_left(end_date),
        }

    except Exception as e:
        print(f"  ⚠ Error on {url}: {e}")
        return None


# ════════════════════════════════════════════════════════════════════
#  MAIN FETCH
# ════════════════════════════════════════════════════════════════════

async def fetch_hackathons(max_scrolls=30):
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

        print("=" * 50)
        print("Phase 1: Collecting hackathon URLs...")
        urls = await collect_urls(page, max_scrolls=max_scrolls)
        print(f"\n  Found {len(urls)} unique hackathon URLs")

        print("\nPhase 2: Extracting details from each page...")
        records = []
        for i, url in enumerate(urls, 1):
            print(f"  [{i}/{len(urls)}] {url}")
            rec = await extract_details(page, url)
            if rec:
                records.append(rec)
                print(f"    ✓ {rec['title']} | {rec['dates']} | {rec['prize']} | {rec['daysLeft']}")
            else:
                print(f"    ✗ Skipped (no data)")

        await browser.close()

    return records


# ════════════════════════════════════════════════════════════════════
#  SAVE
# ════════════════════════════════════════════════════════════════════

def save_to_supabase(records):
    if not records:
        return 0

    records = [r for r in records if r.get("url") and r.get("title")]

    existing = supabase.table("hackathons") \
        .select("url").eq("source", SOURCE).execute()
    existing_urls = {r["url"] for r in (existing.data or [])}

    new_records = [r for r in records if r["url"] not in existing_urls]
    print(f"\n  New: {len(new_records)} | Duplicates skipped: {len(records) - len(new_records)}")

    saved = 0
    for i in range(0, len(new_records), BATCH_SIZE):
        chunk = new_records[i : i + BATCH_SIZE]
        try:
            supabase.table("hackathons").insert(chunk).execute()
            saved += len(chunk)
            print(f"  ✓ Batch {i // BATCH_SIZE + 1}: {len(chunk)} inserted")
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
    records = asyncio.run(fetch_hackathons(max_scrolls=30))
    print(f"\nTotal with full details: {len(records)}")
    if records:
        saved = save_to_supabase(records)
        print(f"\n✅ Done — {saved} new hackathons saved.")
    else:
        print("❌ No hackathons extracted.")
