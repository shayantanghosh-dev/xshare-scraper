import requests
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_client import supabase

SOURCE = "hackerearth"
BATCH_SIZE = 500


def parse_deadline(end_str):
    """Normalize HackerEarth ISO datetimes to YYYY-MM-DD."""
    if not end_str:
        return ""
    try:
        return datetime.fromisoformat(end_str).strftime("%Y-%m-%d")
    except ValueError:
        return end_str


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

    data = response.json()
    challenges = data.get("data", [])
    total = data.get("total", 0)
    print(f"  Total challenges from API: {total}")

    now = datetime.now(timezone.utc)
    result = []

    for c in challenges:
        if c.get("type") != "Hackathon":
            continue
        end_str = c.get("end", "")
        try:
            end_date = datetime.fromisoformat(end_str)
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)
            if end_date < now:
                continue  # skip expired
        except (ValueError, TypeError):
            pass  # include if date is unparseable
        result.append(c)

    print(f"  Upcoming/live hackathons: {len(result)}")
    return result


def save_to_supabase(hackathons):
    """
    Batch-deduplication: one query to load all existing URLs,
    then bulk-insert only new records.
    """
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
        records.append({
            "title":      h.get("title", "").strip(),
            "organizer":  h.get("company_name", "").strip(),
            "deadline":   parse_deadline(h.get("end", "")),
            "prize":      "See HackerEarth",
            "mode":       "online",
            "tags":       h.get("type", "Hackathon"),
            "source_url": url,
            "source":     SOURCE,
            "image_url":  h.get("listing_image", ""),
            "status":     "pending",
        })

    # ── 2. Fetch all existing source_urls in ONE query ────────────────
    existing_result = supabase.table("hackathons")\
        .select("source_url")\
        .eq("source", SOURCE)\
        .execute()
    existing_urls = {r["source_url"] for r in (existing_result.data or [])}

    # ── 3. Filter to only new records ─────────────────────────────────
    new_records = [r for r in records if r["source_url"] not in existing_urls]
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