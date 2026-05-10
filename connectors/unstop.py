import asyncio
import sys
import os
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from playwright.async_api import async_playwright
from utils.supabase_client import supabase

SOURCE     = "unstop"
BATCH_SIZE = 500


def parse_date(date_str):
    """Normalize Unstop date strings → date object."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str[:19], fmt).date()
        except ValueError:
            continue
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


def normalize_location(region):
    """Return (location_str, is_online) from Unstop's region field."""
    if not region:
        return "Online", True
    r = str(region).lower().strip()
    if r in ("online", "virtual", "remote", ""):
        return "Online", True
    return region.strip(), False


async def fetch_hackathons(max_pages=20):
    all_hackathons = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()

        print("Loading Unstop and getting session...")
        await page.goto("https://unstop.com/hackathons?oppstatus=open", wait_until="networkidle")
        await page.wait_for_timeout(3000)

        for page_num in range(1, max_pages + 1):
            url = (
                f"https://unstop.com/api/public/opportunity/search-result"
                f"?opportunity=hackathons"
                f"&page={page_num}"
                f"&per_page=18"
                f"&oppstatus=open"
                f"&sortBy=&orderBy=&filter_condition=&undefined=true"
            )

            print(f"\nFetching page {page_num}...")
            response = await page.evaluate(f"""
                async () => {{
                    const res = await fetch("{url}", {{
                        headers: {{
                            "Accept": "application/json",
                            "Referer": "https://unstop.com/hackathons"
                        }}
                    }});
                    return await res.json();
                }}
            """)

            items     = response.get("data", {}).get("data", [])
            last_page = response.get("data", {}).get("last_page", 1)
            total     = response.get("data", {}).get("total", 0)

            print(f"  → {len(items)} hackathons (Page {page_num}/{last_page}, Total: {total})")

            if not items:
                print("  → No more data.")
                break

            all_hackathons.extend(items)

            if page_num >= last_page:
                print("  → Reached last page!")
                break

            await page.wait_for_timeout(1000)

        await browser.close()

    return all_hackathons


def save_to_supabase(hackathons):
    if not hackathons:
        return 0

    # ── 1. Build records ─────────────────────────────────────────────
    records = []
    for h in hackathons:
        seo_url = h.get("seo_url", "")
        if not seo_url:
            continue

        # Prize — sum cash prizes excluding participation prizes
        total_prize = sum(
            p.get("cash") or 0
            for p in h.get("prizes", [])
            if p.get("rank") not in ["All Participants"]
        )
        prize_str = f"₹{total_prize:,} in prizes" if total_prize > 0 else "Prizes available"

        # Themes from workfunction tags
        themes = [w.get("name", "") for w in h.get("workfunction", []) if w.get("name")]

        # Dates
        start_str = h.get("start_date", "") or h.get("starts_at", "")
        end_str   = h.get("end_date", "")   or h.get("ends_at", "")
        end_date  = parse_date(end_str)

        # Location
        region_raw = h.get("region", "") or h.get("location", "")
        location_str, is_online = normalize_location(region_raw)

        # Participants
        participants_raw = h.get("registrations_count") or h.get("total_registrations") or 0
        participants = f"{int(participants_raw):,}" if participants_raw else "0"

        records.append({
            "source":       SOURCE,
            "title":        h.get("title", "").strip(),
            "url":          seo_url,
            "tagline":      (h.get("tagline") or h.get("short_description") or "").strip(),
            "dates":        format_date_range(start_str, end_str),
            "prize":        prize_str,
            "participants": participants,
            "location":     location_str,
            "themes":       themes,
            "isOnline":     is_online,
            "daysLeft":     compute_days_left(end_date),
        })

    # ── 2. Fetch all existing URLs in ONE query ───────────────────────
    existing_result = supabase.table("hackathons") \
        .select("url") \
        .eq("source", SOURCE) \
        .execute()
    existing_urls = {r["url"] for r in (existing_result.data or [])}

    # ── 3. Filter to only new records ─────────────────────────────────
    new_records = [r for r in records if r["url"] not in existing_urls]
    skipped = len(records) - len(new_records)
    print(f"  New: {len(new_records)} | Duplicates skipped: {skipped}")

    # ── 4. Batch insert ───────────────────────────────────────────────
    saved = 0
    for i in range(0, len(new_records), BATCH_SIZE):
        chunk = new_records[i : i + BATCH_SIZE]
        try:
            supabase.table("hackathons").insert(chunk).execute()
            saved += len(chunk)
            print(f"  ✓ Batch {i // BATCH_SIZE + 1}: inserted {len(chunk)} records")
        except Exception as e:
            print(f"  ✗ Batch insert error: {e}")

    return saved


async def main():
    print("=" * 50)
    print("UNSTOP HACKATHON SCRAPER")
    print("=" * 50)

    hackathons = await fetch_hackathons(max_pages=20)
    print(f"\nTotal fetched: {len(hackathons)}")

    if hackathons:
        print("\nSaving to Supabase...")
        save_to_supabase(hackathons)
    else:
        print("No hackathons found.")


if __name__ == "__main__":
    asyncio.run(main())
