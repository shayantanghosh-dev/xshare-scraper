import requests
import re
import sys
import os
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_client import supabase

SOURCE = "devpost"
BATCH_SIZE = 500


def clean_prize(prize_html):
    """Strip HTML tags from prize amount."""
    clean = re.sub(r'<[^>]+>', '', prize_html).strip()
    if not clean or re.fullmatch(r'[\$£€₹][\w\s]*0', clean):
        return "Prizes available"
    return clean


def parse_end_date(period_str):
    """
    Extract the END date from a Devpost date range like 'Apr 11 - Jun 19, 2026'.
    Returns a date object or None.
    """
    if not period_str:
        return None
    period_str = period_str.replace("–", "-").strip()
    end_part = period_str.split(" - ", 1)[-1].strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %Y"):
        try:
            return datetime.strptime(end_part, fmt).date()
        except ValueError:
            continue
    return None


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


def normalize_location(location):
    """
    Return (location_str, is_online) tuple.
    Online/virtual → ("Online", True); else keep city name and mark offline.
    """
    if not location:
        return "Online", True
    loc = location.strip()
    if loc.lower() in ("online", "virtual", "remote", "anywhere", ""):
        return "Online", True
    return loc, False


def fetch_hackathons(max_pages=60):
    """Fetch open + upcoming hackathons from Devpost API."""
    all_hackathons = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://devpost.com/hackathons",
    }

    session = requests.Session()
    session.get("https://devpost.com/hackathons", headers=headers, timeout=15)

    for page_num in range(1, max_pages + 1):
        params = {
            "page": page_num,
            "per_page": 24,
            "status[]": ["open", "upcoming"],
            "order_by": "recently-added",
        }
        try:
            response = session.get(
                "https://devpost.com/api/hackathons",
                headers=headers,
                params=params,
                timeout=15,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"  ⚠ Page {page_num} request failed: {e}")
            break

        data = response.json()
        hackathons = data.get("hackathons", [])
        meta = data.get("meta", {})
        total = meta.get("total_count", 0)
        per_page = meta.get("per_page", 24)
        last_page = max(1, (total + per_page - 1) // per_page)

        print(f"  Page {page_num}/{last_page} → {len(hackathons)} items (total: {total})")

        if not hackathons:
            break

        all_hackathons.extend(hackathons)

        if page_num >= last_page:
            print("  Reached last page.")
            break

    return all_hackathons


def save_to_supabase(hackathons):
    if not hackathons:
        return 0

    # ── 1. Build records ─────────────────────────────────────────────
    records = []
    for h in hackathons:
        url = h.get("url", "")
        if not url:
            continue

        raw_location = h.get("displayed_location", {}).get("location", "")
        location_str, is_online = normalize_location(raw_location)

        dates_str = h.get("submission_period_dates", "").replace("–", "-").strip()
        end_date = parse_end_date(dates_str)

        participants_raw = h.get("registrations_count", 0) or 0
        participants_str = f"{participants_raw:,}" if participants_raw else "0"

        themes = [
            t.get("name", "") for t in h.get("themes", []) if t.get("name")
        ]

        records.append({
            "source":       SOURCE,
            "title":        h.get("title", "").strip(),
            "url":          url,
            "tagline":      (h.get("tagline") or h.get("description") or "").strip(),
            "dates":        dates_str,
            "prize":        clean_prize(h.get("prize_amount", "")),
            "participants": participants_str,
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


if __name__ == "__main__":
    print("=" * 50)
    print("DEVPOST SCRAPER")
    print("=" * 50)
    items = fetch_hackathons(max_pages=60)
    print(f"\nTotal fetched: {len(items)}")
    if items:
        saved = save_to_supabase(items)
        print(f"\n✅ Done — {saved} new hackathons saved.")