import requests
import re
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_client import supabase

SOURCE = "devpost"
BATCH_SIZE = 500  # Supabase safe insert limit


def clean_prize(prize_html):
    """Strip HTML tags from prize amount."""
    clean = re.sub(r'<[^>]+>', '', prize_html).strip()
    if not clean or re.fullmatch(r'[\$£€₹][\w\s]*0', clean):
        return "Certificates/Others"
    return clean


def parse_deadline(period_str):
    """
    Convert Devpost date strings like 'Apr 11 - Jun 19, 2026' → 'YYYY-MM-DD'.
    Always extracts the END date.
    """
    if not period_str:
        return ""
    period_str = period_str.replace("–", "-").strip()
    end_part = period_str.split(" - ", 1)[-1].strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %Y"):
        try:
            return datetime.strptime(end_part, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return end_part  # return raw if unparseable


def normalize_mode(location):
    """Map raw Devpost location string to 'online' or 'offline'."""
    if not location:
        return "online"
    loc = location.lower().strip()
    if loc in ("online", "virtual", "remote", "anywhere", ""):
        return "online"
    return "offline"  # city names, country names = in-person


def fetch_hackathons(max_pages=60):
    """
    Fetch open + upcoming hackathons from Devpost API.
    Previously only fetched 'open' (~63). Now fetches both statuses (~1300+).
    """
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
    """
    Batch-deduplication: one query to load all existing URLs,
    then bulk-insert only new records in chunks of BATCH_SIZE.
    Avoids the old N×SELECT pattern (was 2600+ queries for devpost).
    """
    if not hackathons:
        return 0

    # ── 1. Build records ─────────────────────────────────────────────
    records = []
    for h in hackathons:
        url = h.get("url", "")
        if not url:
            continue
        location = h.get("displayed_location", {}).get("location", "")
        records.append({
            "title":      h.get("title", "").strip(),
            "organizer":  h.get("organization_name", "").strip(),
            "deadline":   parse_deadline(h.get("submission_period_dates", "")),
            "prize":      clean_prize(h.get("prize_amount", "")),
            "mode":       normalize_mode(location),
            "tags":       ", ".join(t.get("name", "") for t in h.get("themes", [])),
            "source_url": url,
            "source":     SOURCE,
            "image_url":  h.get("thumbnail_url", ""),
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
    print("DEVPOST SCRAPER")
    print("=" * 50)
    items = fetch_hackathons(max_pages=60)
    print(f"\nTotal fetched: {len(items)}")
    if items:
        saved = save_to_supabase(items)
        print(f"\n✅ Done — {saved} new hackathons saved.")