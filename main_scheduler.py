"""
xShare Master Scheduler  (v2)
==============================
Single entry point — runs all 6 scrapers every 6 hours.

  Hackathons   : Unstop · Devfolio · Devpost · HackerEarth
  Internships  : Unstop
  Scholarships : Buddy4Study  (OPEN · CLOSED · ALWAYS_OPEN)

──────────────────────────────────────────────────────────────
⚠  One-time Supabase schema change required for UPSERT to work.
   Run these in the Supabase SQL editor before first launch:

   ALTER TABLE hackathons
     ADD CONSTRAINT hackathons_url_unique UNIQUE (url);

   ALTER TABLE internships
     ADD CONSTRAINT internships_url_unique UNIQUE (url);

   (scholarships already has UNIQUE on detail_url — no change needed)
──────────────────────────────────────────────────────────────
"""

import asyncio
import logging
import os
import sys
import traceback
from datetime import datetime, timezone

# ── Force all output to stdout so Railway streams it in real time ──────────────
# Without this, Python buffers output and Railway shows nothing until the process ends.
sys.stdout.reconfigure(line_buffering=True)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ── Fetch functions ────────────────────────────────────────────────────────────
from connectors.unstop             import fetch_hackathons  as unstop_fetch
from connectors.devfolio           import fetch_hackathons  as devfolio_fetch
from connectors.devpost            import fetch_hackathons  as devpost_fetch
from connectors.hackerearth        import fetch_hackathons  as he_fetch
from connectors.unstop_internships import fetch_internships as unstop_intern_fetch
from connectors.buddy              import main              as buddy_main

# ── Save functions from connectors (handle field mapping for their own sources) ─
from connectors.unstop             import save_to_supabase  as unstop_hack_save
from connectors.devpost            import save_to_supabase  as devpost_save
from connectors.hackerearth        import save_to_supabase  as he_save
from connectors.unstop_internships import save_to_supabase  as unstop_intern_save

from utils.supabase_client import supabase

BATCH_SIZE = 500
W          = 56
_run_count = 0


# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _banner(text: str, char: str = "═") -> str:
    return f"{char * W}\n  {text}\n{char * W}"

def _section(n: int, total: int, source: str, category: str) -> str:
    label = f"[{n}/{total}]  {source}  ·  {category}"
    return f"\n{'─' * W}\n  {label}\n{'─' * W}"

def _elapsed(t0: datetime) -> float:
    return round((datetime.now() - t0).total_seconds(), 1)


# ══════════════════════════════════════════════════════════════════════════════
#  CLEANUP HELPER  — deletes expired/delisted entries from any table
# ══════════════════════════════════════════════════════════════════════════════

def _cleanup(table: str, source: str, current_urls: set) -> int:
    """
    Deletes rows from `table` where source=`source` and url is no longer
    in `current_urls` (i.e. the listing was removed or expired at the source).
    Returns count of deleted rows.
    """
    try:
        res           = supabase.table(table).select("url").eq("source", source).execute()
        existing_urls = {r["url"] for r in (res.data or [])}
        to_delete     = list(existing_urls - current_urls)
        if not to_delete:
            return 0
        deleted = 0
        for i in range(0, len(to_delete), 200):
            chunk = to_delete[i : i + 200]
            supabase.table(table).delete() \
                .eq("source", source).in_("url", chunk).execute()
            deleted += len(chunk)
        print(f"    🗑  Removed {deleted} expired / delisted entries")
        return deleted
    except Exception as e:
        print(f"    ⚠  Cleanup failed (non-fatal): {e}")
        return 0


# ══════════════════════════════════════════════════════════════════════════════
#  DEVFOLIO SAVE — UPSERT + CLEANUP
#  Devfolio's fetch already returns shaped records with a `url` field,
#  so we can UPSERT directly (updates existing + inserts new).
# ══════════════════════════════════════════════════════════════════════════════

def _upsert_and_clean_devfolio(records: list) -> tuple:
    """Full UPSERT + cleanup for Devfolio. Returns (new, updated, deleted)."""
    records = [r for r in records if r.get("url")]
    if not records:
        print("    ⚠  No valid records to save.")
        return 0, 0, 0

    current_urls = {r["url"] for r in records}

    try:
        res           = supabase.table("hackathons").select("url").eq("source", "devfolio").execute()
        existing_urls = {r["url"] for r in (res.data or [])}
    except Exception as e:
        print(f"    ⚠  Could not read existing rows: {e}")
        existing_urls = set()

    new_count     = len(current_urls - existing_urls)
    updated_count = len(current_urls & existing_urls)

    # Delete expired
    to_delete = list(existing_urls - current_urls)
    deleted   = 0
    if to_delete:
        try:
            for i in range(0, len(to_delete), 200):
                chunk = to_delete[i : i + 200]
                supabase.table("hackathons").delete() \
                    .eq("source", "devfolio").in_("url", chunk).execute()
                deleted += len(chunk)
            print(f"    🗑  Removed {deleted} expired / delisted entries")
        except Exception as e:
            print(f"    ⚠  Cleanup failed (non-fatal): {e}")

    # UPSERT
    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        try:
            supabase.table("hackathons").upsert(chunk, on_conflict="url").execute()
        except Exception as e:
            print(f"    ✗  Upsert batch {i // BATCH_SIZE + 1} failed: {e}")

    print(f"    ✓  {new_count} new  ·  {updated_count} updated  ·  {deleted} removed")
    return new_count, updated_count, deleted


# ══════════════════════════════════════════════════════════════════════════════
#  SCHOLARSHIP SAVE — UPSERT + CLEANUP
# ══════════════════════════════════════════════════════════════════════════════

def _upsert_scholarships(df: pd.DataFrame) -> tuple:
    """Full UPSERT + cleanup for scholarships. Returns (new, updated, deleted)."""
    if df is None or df.empty:
        print("    ⚠  No scholarship data to save.")
        return 0, 0, 0

    df = df.copy()
    df["scraped_at"] = datetime.now(timezone.utc).isoformat()
    df = df.where(pd.notna(df), other=None)

    keep_cols = [
        "name", "amount", "eligibility", "last_date",
        "category", "provider", "logo_url", "detail_url",
        "mode", "scraped_at",
    ]
    df = df[[c for c in keep_cols if c in df.columns]]
    records = df.to_dict(orient="records")

    cleaned = [
        {k: (None if isinstance(v, float) and v != v else v) for k, v in row.items()}
        for row in records
    ]
    cleaned = [r for r in cleaned if r.get("detail_url")]

    if not cleaned:
        print("    ⚠  All records missing detail_url.")
        return 0, 0, 0

    current_urls = {r["detail_url"] for r in cleaned}

    try:
        res           = supabase.table("scholarships").select("detail_url").execute()
        existing_urls = {r["detail_url"] for r in (res.data or [])}
    except Exception as e:
        print(f"    ⚠  Could not read existing scholarships: {e}")
        existing_urls = set()

    new_count     = len(current_urls - existing_urls)
    updated_count = len(current_urls & existing_urls)

    # Delete expired
    to_delete = list(existing_urls - current_urls)
    deleted   = 0
    if to_delete:
        try:
            for i in range(0, len(to_delete), 200):
                chunk = to_delete[i : i + 200]
                supabase.table("scholarships").delete().in_("detail_url", chunk).execute()
                deleted += len(chunk)
            print(f"    🗑  Removed {deleted} expired scholarships")
        except Exception as e:
            print(f"    ⚠  Scholarship cleanup failed (non-fatal): {e}")

    # UPSERT
    for i in range(0, len(cleaned), BATCH_SIZE):
        chunk = cleaned[i : i + BATCH_SIZE]
        try:
            supabase.table("scholarships").upsert(chunk, on_conflict="detail_url").execute()
        except Exception as e:
            print(f"    ✗  Upsert batch {i // BATCH_SIZE + 1} failed: {e}")

    print(f"    ✓  {new_count} new  ·  {updated_count} updated  ·  {deleted} removed")
    return new_count, updated_count, deleted


# ══════════════════════════════════════════════════════════════════════════════
#  INDIVIDUAL SCRAPER RUNNERS
# ══════════════════════════════════════════════════════════════════════════════

def _run_unstop_hackathons() -> tuple:
    # Unstop raw items use `seo_url` — connector's save handles the mapping.
    # We add cleanup separately using the same seo_url values.
    async def job():
        data = await unstop_fetch(max_pages=20)
        if not data:
            return 0, 0, 0
        saved   = unstop_hack_save(data)
        current = {h.get("seo_url", "") for h in data if h.get("seo_url")}
        deleted = _cleanup("hackathons", "unstop", current)
        print(f"    ✓  {saved} inserted  ·  {deleted} removed")
        return saved, 0, deleted
    return asyncio.run(job())


def _run_devfolio() -> tuple:
    # Devfolio fetch already returns shaped records with `url` — full UPSERT.
    async def job():
        data = await devfolio_fetch(max_scrolls=20)
        return _upsert_and_clean_devfolio(data)
    return asyncio.run(job())


def _run_devpost() -> tuple:
    # Devpost raw items have extra API fields — connector's save builds clean records.
    data = devpost_fetch(max_pages=60)
    if not data:
        return 0, 0, 0
    saved   = devpost_save(data)
    current = {h.get("url", "") for h in data if h.get("url")}
    deleted = _cleanup("hackathons", "devpost", current)
    print(f"    ✓  {saved} inserted  ·  {deleted} removed")
    return saved, 0, deleted


def _run_hackerearth() -> tuple:
    # HackerEarth URLs may be relative — connector's save normalises them.
    data = he_fetch()
    if not data:
        return 0, 0, 0
    saved = he_save(data)
    current = set()
    for h in data:
        url = h.get("url", "")
        if url.startswith("/"):
            url = f"https://www.hackerearth.com{url}"
        if url:
            current.add(url)
    deleted = _cleanup("hackathons", "hackerearth", current)
    print(f"    ✓  {saved} inserted  ·  {deleted} removed")
    return saved, 0, deleted


def _run_unstop_internships() -> tuple:
    # Same as Unstop hackathons — raw items use `seo_url`.
    async def job():
        data = await unstop_intern_fetch(max_pages=50)
        if not data:
            return 0, 0, 0
        saved   = unstop_intern_save(data)
        current = {item.get("seo_url", "") for item in data if item.get("seo_url")}
        deleted = _cleanup("internships", "unstop", current)
        print(f"    ✓  {saved} inserted  ·  {deleted} removed")
        return saved, 0, deleted
    return asyncio.run(job())


def _run_buddy4study() -> tuple:
    async def job():
        return await buddy_main(
            modes    = ["OPEN", "CLOSED", "ALWAYS_OPEN"],
            output   = None,
            headless = True,
            debug    = False,
        )
    df = asyncio.run(job())
    if df is not None and not df.empty and "mode" in df.columns:
        breakdown = df["mode"].value_counts().to_dict()
        print(f"    Fetched {len(df)} scholarships — {breakdown}")
    return _upsert_scholarships(df)


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPER REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

SCRAPERS = [
    ("Unstop",       "Hackathons",   _run_unstop_hackathons),
    ("Devfolio",     "Hackathons",   _run_devfolio),
    ("Devpost",      "Hackathons",   _run_devpost),
    ("HackerEarth",  "Hackathons",   _run_hackerearth),
    ("Unstop",       "Internships",  _run_unstop_internships),
    ("Buddy4Study",  "Scholarships", _run_buddy4study),
]


# ══════════════════════════════════════════════════════════════════════════════
#  ISOLATED RUNNER WRAPPER
#  One scraper crashing will NEVER stop the others or kill the scheduler.
# ══════════════════════════════════════════════════════════════════════════════

def _run_isolated(index: int, total: int, name: str, category: str, fn) -> tuple | None:
    print(_section(index, total, name, category))
    t0 = datetime.now()
    try:
        result = fn()
        print(f"    ⏱  {_elapsed(t0)}s")
        return result
    except Exception as exc:
        secs = _elapsed(t0)
        print(f"\n    ❌  {name} ({category}) FAILED after {secs}s")
        print(f"    Error : {exc}")
        print("    Traceback:")
        traceback.print_exc()
        print()
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  MASTER JOB
# ══════════════════════════════════════════════════════════════════════════════

def run_all_scrapers():
    global _run_count
    _run_count += 1

    start = datetime.now()
    ts    = start.strftime("%Y-%m-%d  %H:%M:%S")
    print(f"\n{_banner(f'RUN #{_run_count}  ·  {ts}')}")

    category_totals: dict = {}
    failed_scrapers: list = []

    for i, (name, category, fn) in enumerate(SCRAPERS, 1):
        result = _run_isolated(i, len(SCRAPERS), name, category, fn)

        if category not in category_totals:
            category_totals[category] = {"new": 0, "updated": 0, "removed": 0}

        if result is None:
            failed_scrapers.append(f"{name} ({category})")
        else:
            new, updated, removed = result
            category_totals[category]["new"]     += new
            category_totals[category]["updated"] += updated
            category_totals[category]["removed"] += removed

    elapsed = round((datetime.now() - start).total_seconds())
    col_w   = max(len(c) for c in category_totals)

    print(f"\n{_banner(f'DONE  ·  {elapsed}s total')}")
    for category, t in category_totals.items():
        print(
            f"  {category:<{col_w}}  "
            f"{t['new']:>4} new  ·  "
            f"{t['updated']:>4} updated  ·  "
            f"{t['removed']:>3} removed"
        )
    if failed_scrapers:
        print(f"\n  ⚠  Failed scrapers (all others ran fine):")
        for s in failed_scrapers:
            print(f"     • {s}")
    print(f"\n  Next run in 6 hours.")
    print(f"{'═' * W}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(_banner("XSHARE MASTER SCHEDULER"))
    print()
    print("  Sources")
    print("    Hackathons   : Unstop · Devfolio · Devpost · HackerEarth")
    print("    Internships  : Unstop")
    print("    Scholarships : Buddy4Study  (Live · Upcoming · Always Open)")
    print()
    print("  Schedule : every 6 hours · Asia/Kolkata")
    print()
    print("  ⚠  One-time schema change — run in Supabase SQL editor if not done:")
    print("     ALTER TABLE hackathons  ADD CONSTRAINT hackathons_url_unique  UNIQUE (url);")
    print("     ALTER TABLE internships ADD CONSTRAINT internships_url_unique UNIQUE (url);")
    print()

    run_all_scrapers()

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_all_scrapers,
        IntervalTrigger(hours=6),
        id            = "xshare_master_6h",
        name          = "xShare Master Scraper — Every 6 Hours",
        max_instances = 1,
        coalesce      = True,
    )

    print("Scheduler armed — press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\nScheduler stopped.")
        scheduler.shutdown()