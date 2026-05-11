import requests
import sys
import os
from datetime import datetime, date, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_client import supabase

SOURCE     = "hackerearth"
BATCH_SIZE = 500


def parse_date(date_str):
    """Normalize HackerEarth ISO datetimes → date object."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).date()
    except ValueError:
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


def fetch_hackathons():
    """Fetch upcoming/live hackathons from HackerEarth community API."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.hackerearth.com/challenges/",
    }

    session = requests.Session()
    session.get("https://www.hackerearth.com/challenges/", headers=headers, timeout=15)

    try:
        response = session.get(
            "https://www.hackerearth.com/api/community/challenges/compete/",
            headers=headers,
            timeout=15,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  ⚠ Request failed: {e}")
        return []

    data       = response.json()
    challenges = data.get("data", [])
    total      = data.get("total", 0)
    print(f"  Total challenges from API: {total}")

    now    = datetime.now(timezone.utc)
    result = []

    for c in challenges:
        if c.get("type") != "Hackathon":
            continue
        end_str = c.get("end", "")
        try:
            end_dt = datetime.fromisoformat(end_str)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            if end_dt < now:
                continue
        except (ValueError, TypeError):
            pass
        result.append(c)

    print(f"  Upcoming/live hackathons: {len(result)}")
    return result


def save_to_supabase(hackathons):
    if not hackathons:
        return 0

    # ── 1. Build records ─────────────────────────────────────────────
    records = []
    for h in hackathons:
        url = h.get("url", "")
        if url.startswith("/"):
            url = f"https://www.hackerearth.com{url}"
        if not url:
            continue

        start_str = h.get("start", "")
        end_str   = h.get("end", "")
        end_date  = parse_date(end_str)

        # HackerEarth challenges are always online
        # Tags come from challenge type + any sub-type
        themes = [t for t in [h.get("type"), h.get("challenge_type")] if t]

        participants_raw = h.get("registrations_count") or h.get("total_participants") or 0
        participants = f"{int(participants_raw):,}" if participants_raw else "0"

        records.append({
            "source":       SOURCE,
            "title":        h.get("title", "").strip(),
            "url":          url,
            "tagline":      (h.get("description") or h.get("short_description") or "").strip(),
            "dates":        format_date_range(start_str, end_str),
            "prize":        "See HackerEarth",
            "participants": participants,
            "location":     "Online",
            "themes":       themes,
            "isOnline":     True,
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
    print("HACKEREARTH SCRAPER")
    print("=" * 50)
    items = fetch_hackathons()
    print(f"\nTotal fetched: {len(items)}")
    if items:
        saved = save_to_supabase(items)
        print(f"\n✅ Done — {saved} new hackathons saved.")