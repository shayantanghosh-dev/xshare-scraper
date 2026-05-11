"""
Unstop Internship Scraper
Fetches all open internships from Unstop via the public search API.
Uses Playwright to borrow session cookies, then calls the API directly.

Fields captured per internship:
  source, title, company, url, tagline, dates, deadline,
  stipend, duration, location, isRemote, skills, domain,
  applicants, daysLeft
"""

import asyncio
import sys
import os
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from playwright.async_api import async_playwright
from utils.supabase_client import supabase

SOURCE     = "unstop"
TABLE      = "internships"
BATCH_SIZE = 500

# ──────────────────────────────────────────────────────────────────
# DATE HELPERS  (identical logic to hackathon scraper)
# ──────────────────────────────────────────────────────────────────

def parse_date(date_str):
    """Normalise Unstop date strings → date object."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str[:19], fmt).date()
        except ValueError:
            continue
    return None


def format_date_range(start_str, end_str):
    """Build 'May 1 – May 20, 2026' style label."""
    start = parse_date(start_str)
    end   = parse_date(end_str)
    if start and end:
        if start.year == end.year:
            return f"{start.strftime('%b')} {start.day} – {end.strftime('%b')} {end.day}, {end.year}"
        return (
            f"{start.strftime('%b')} {start.day}, {start.year} "
            f"– {end.strftime('%b')} {end.day}, {end.year}"
        )
    if end:
        return f"Deadline: {end.strftime('%b')} {end.day}, {end.year}"
    return ""


def compute_days_left(end_date):
    """'21 days left' / 'Ends today' / 'Ended' / 'Unknown'."""
    if not end_date:
        return "Unknown"
    delta = (end_date - date.today()).days
    if delta < 0:
        return "Ended"
    if delta == 0:
        return "Ends today"
    return f"{delta} days left"


# ──────────────────────────────────────────────────────────────────
# LOCATION
# ──────────────────────────────────────────────────────────────────

def normalize_location(region):
    """Return (location_str, is_remote)."""
    if not region:
        return "Remote", True
    r = str(region).lower().strip()
    if r in ("online", "virtual", "remote", "work from home", "wfh", ""):
        return "Remote", True
    return region.strip(), False


# ──────────────────────────────────────────────────────────────────
# STIPEND PARSING
# ──────────────────────────────────────────────────────────────────

def parse_stipend(internship: dict) -> str:
    """
    Try various field paths Unstop uses for stipend/salary.
    Returns a human-readable string like '₹10,000/mo' or 'Unpaid'.
    """
    # Path 1: explicit stipend object
    stipend_obj = internship.get("stipend") or {}
    if isinstance(stipend_obj, dict):
        amount = stipend_obj.get("amount") or stipend_obj.get("value") or stipend_obj.get("max")
        if amount and int(amount) > 0:
            currency = stipend_obj.get("currency", "₹")
            period   = stipend_obj.get("period", "mo")
            return f"{currency}{int(amount):,}/{period}"

    # Path 2: flat salary / stipend fields
    for key in ("salary", "stipend_amount", "max_salary", "min_salary"):
        val = internship.get(key)
        if val and int(val) > 0:
            return f"₹{int(val):,}/mo"

    # Path 3: free-text compensation field
    comp = internship.get("compensation") or internship.get("salary_range") or ""
    if comp and str(comp).strip():
        return str(comp).strip()

    return "Unpaid / Not disclosed"


# ──────────────────────────────────────────────────────────────────
# DURATION PARSING
# ──────────────────────────────────────────────────────────────────

def parse_duration(internship: dict) -> str:
    """Return duration string like '3 Months' or '' if not available."""
    for key in ("duration", "internship_duration", "duration_in_months"):
        val = internship.get(key)
        if val:
            s = str(val).strip()
            if s.isdigit():
                return f"{s} Month{'s' if int(s) != 1 else ''}"
            return s
    return ""


# ──────────────────────────────────────────────────────────────────
# FETCH — Playwright borrows session, API does the work
# ──────────────────────────────────────────────────────────────────

async def fetch_internships(max_pages: int = 50):
    """
    Fetch all open internships from Unstop.
    Returns a flat list of raw API dicts.
    """
    all_internships = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await browser.new_page()

        print("Loading Unstop internships page (getting session)…")
        await page.goto(
            "https://unstop.com/internships?oppstatus=open",
            wait_until="networkidle",
        )
        await page.wait_for_timeout(3000)

        for page_num in range(1, max_pages + 1):
            url = (
                "https://unstop.com/api/public/opportunity/search-result"
                f"?opportunity=internships"
                f"&page={page_num}"
                f"&per_page=18"
                f"&oppstatus=open"
                f"&sortBy=&orderBy=&filter_condition=&undefined=true"
            )

            print(f"\n  Fetching page {page_num}…")
            response = await page.evaluate(f"""
                async () => {{
                    const res = await fetch("{url}", {{
                        headers: {{
                            "Accept": "application/json",
                            "Referer": "https://unstop.com/internships"
                        }}
                    }});
                    return await res.json();
                }}
            """)

            items     = response.get("data", {}).get("data", [])
            last_page = response.get("data", {}).get("last_page", 1)
            total     = response.get("data", {}).get("total", 0)

            print(f"  → {len(items)} internships  (page {page_num}/{last_page}, total API: {total})")

            if not items:
                print("  → Empty page — stopping.")
                break

            all_internships.extend(items)

            if page_num >= last_page:
                print("  → Last page reached.")
                break

            await page.wait_for_timeout(1200)   # polite delay

        await browser.close()

    return all_internships


# ──────────────────────────────────────────────────────────────────
# TRANSFORM + SAVE
# ──────────────────────────────────────────────────────────────────

def save_to_supabase(internships: list) -> int:
    if not internships:
        return 0

    # ── 1. Build records ──────────────────────────────────────────
    records = []
    for item in internships:
        seo_url = item.get("seo_url", "")
        if not seo_url:
            continue

        # Company / organisation name
        org = item.get("organisation") or item.get("organization") or {}
        company = ""
        if isinstance(org, dict):
            company = org.get("name", "") or org.get("org_name", "")
        if not company:
            company = item.get("company_name") or item.get("org_name") or ""

        # Skills / domain tags — same field as hackathon 'themes'
        skills = [
            w.get("name", "")
            for w in item.get("workfunction", [])
            if w.get("name")
        ]
        # Also pick up explicit skill tags if present
        for s in item.get("skills", []) or []:
            name = s.get("name") or s.get("skill_name") or ""
            if name and name not in skills:
                skills.append(name)

        # Primary domain — first workfunction tag or 'General'
        domain = skills[0] if skills else "General"

        # Dates
        start_str = item.get("start_date") or item.get("starts_at") or ""
        end_str   = item.get("end_date")   or item.get("ends_at")   or ""
        end_date  = parse_date(end_str)

        # Location
        region_raw = item.get("region") or item.get("location") or ""
        location_str, is_remote = normalize_location(region_raw)

        # Applicant count
        applicants_raw = (
            item.get("registrations_count")
            or item.get("total_registrations")
            or 0
        )
        applicants = f"{int(applicants_raw):,}" if applicants_raw else "0"

        records.append({
            "source":     SOURCE,
            "title":      item.get("title", "").strip(),
            "company":    company.strip(),
            "url":        seo_url,
            "tagline":    (item.get("tagline") or item.get("short_description") or "").strip(),
            "dates":      format_date_range(start_str, end_str),
            "stipend":    parse_stipend(item),
            "duration":   parse_duration(item),
            "location":   location_str,
            "isRemote":   is_remote,
            "skills":     skills,          # text[]  in Supabase
            "domain":     domain,
            "applicants": applicants,
            "daysLeft":   compute_days_left(end_date),
        })

    # ── 2. Fetch existing URLs in one query ───────────────────────
    existing_result = (
        supabase.table(TABLE)
        .select("url")
        .eq("source", SOURCE)
        .execute()
    )
    existing_urls = {r["url"] for r in (existing_result.data or [])}

    # ── 3. Keep only new records ──────────────────────────────────
    new_records = [r for r in records if r["url"] not in existing_urls]
    skipped     = len(records) - len(new_records)
    print(f"  New: {len(new_records)} | Duplicates skipped: {skipped}")

    # ── 4. Batch insert ───────────────────────────────────────────
    saved = 0
    for i in range(0, len(new_records), BATCH_SIZE):
        chunk = new_records[i : i + BATCH_SIZE]
        try:
            supabase.table(TABLE).insert(chunk).execute()
            saved += len(chunk)
            print(f"  ✓ Batch {i // BATCH_SIZE + 1}: inserted {len(chunk)} records")
        except Exception as e:
            print(f"  ✗ Batch insert error: {e}")

    return saved


# ──────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────

async def main():
    print("=" * 54)
    print("  UNSTOP INTERNSHIP SCRAPER")
    print("=" * 54)

    internships = await fetch_internships(max_pages=50)
    print(f"\nTotal fetched: {len(internships)}")

    if internships:
        print("\nSaving to Supabase…")
        save_to_supabase(internships)
    else:
        print("No internships found.")


if __name__ == "__main__":
    asyncio.run(main())