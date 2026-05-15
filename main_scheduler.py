"""
xShare Master Scheduler  (v7)
==============================
Single entry point — runs all scrapers on a fixed schedule.

  Every 6 hours:
    Hackathons   → Unstop, Devfolio, Devpost, HackerEarth
    Internships  → Unstop
    Scholarships → Buddy4Study
    Jobs         → LinkedIn (4 role tasks merged into 1 save),
                   Indeed  (via Apify — requires APIFY_TOKEN)

v7 change: LinkedIn jobs are now deduped across the 4 role tasks
in-memory before being written. Each job stores the list of roles
it matched in the `matched_roles` text[] column, so the frontend
keeps per-role filtering while the DB stops thrashing.

Environment variables required:
  SUPABASE_URL
  SUPABASE_KEY
  APIFY_TOKEN   ← LinkedIn & Indeed jobs (skipped gracefully if absent)
"""

import asyncio
import logging
import os
import sys
import traceback
from datetime import datetime, timezone

# ── Flush stdout immediately so Railway streams logs in real time ──────────────
sys.stdout.reconfigure(line_buffering=True)

# ── Silence noisy third-party loggers ─────────────────────────────────────────
for _noisy in (
    "httpx", "httpcore",
    "apscheduler.scheduler", "apscheduler.executors.default",
    "supabase", "postgrest",
    "anthropic",
):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.WARNING,
    format="%(levelname)s  %(name)s  %(message)s",
    force=True,
)

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ── Connector imports ──────────────────────────────────────────────────────────
from connectors.unstop             import fetch_hackathons  as unstop_fetch
from connectors.devfolio           import fetch_hackathons  as devfolio_fetch
from connectors.devpost            import fetch_hackathons  as devpost_fetch
from connectors.hackerearth        import fetch_hackathons  as he_fetch
from connectors.unstop_internships import fetch_internships as unstop_intern_fetch
from connectors.buddy              import main              as buddy_main
from connectors.linkedin_jobs      import fetch_jobs        as linkedin_fetch
from connectors.indeed_jobs        import fetch_jobs        as indeed_fetch

from connectors.unstop             import save_to_supabase  as unstop_hack_save
from connectors.devpost            import save_to_supabase  as devpost_save
from connectors.hackerearth        import save_to_supabase  as he_save
from connectors.unstop_internships import save_to_supabase  as unstop_intern_save
from connectors.linkedin_jobs      import save_to_supabase  as linkedin_save
from connectors.indeed_jobs        import save_to_supabase  as indeed_save

from utils.supabase_client import supabase

BATCH_SIZE = 500
W          = 60
_run_count = 0

# ── LinkedIn Apify task IDs ────────────────────────────────────────────────────
LINKEDIN_TASKS = {
    "Software Engineer":    "shayantan_ghosh-dev~linkedin-jobs-software-engineer-india-daily",
    "Full Stack Developer": "shayantan_ghosh-dev~linkedin-jobs-full-stack-developer-india-daily",
    "Data Analyst":         "shayantan_ghosh-dev~linkedin-jobs-data-analyst-india-daily",
    "Frontend Developer":   "shayantan_ghosh-dev~linkedin-jobs-frontend-developer-india-daily",
}

# Combined slug under which all merged LinkedIn jobs live in the DB.
# Cleanup is scoped to this slug only — won't touch Indeed or anything else.
LINKEDIN_COMBINED_SLUG = "linkedin-india-all"


# ══════════════════════════════════════════════════════════════════════════════
#  OUTPUT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _line(char: str = "═") -> str:
    return char * W

def _banner(text: str) -> None:
    print(f"\n{_line()}")
    print(f"  {text}")
    print(_line())

def _section(n: int, total: int, source: str, category: str) -> None:
    print(f"\n{_line('-')}")
    print(f"  [{n}/{total}]  {source}  —  {category}")
    print(_line('-'))

def _ok(msg: str) -> None:
    print(f"  ✓  {msg}")

def _warn(msg: str) -> None:
    print(f"  ⚠  {msg}")

def _fail(msg: str) -> None:
    print(f"  ✗  {msg}")

def _elapsed(t0: datetime) -> str:
    secs = round((datetime.now() - t0).total_seconds(), 1)
    return f"{secs}s"


# ══════════════════════════════════════════════════════════════════════════════
#  STARTUP CHECKS
# ══════════════════════════════════════════════════════════════════════════════

def _check_env() -> dict[str, bool]:
    checks = {
        "SUPABASE_URL": bool(os.environ.get("SUPABASE_URL")),
        "SUPABASE_KEY": bool(os.environ.get("SUPABASE_KEY")),
        "APIFY_TOKEN":  bool(os.environ.get("APIFY_TOKEN")),
    }
    missing = [k for k, ok in checks.items() if not ok]
    if missing:
        print()
        for k in missing:
            _warn(f"Env var not set: {k}")
        if "APIFY_TOKEN" in missing:
            _warn("LinkedIn & Indeed job scrapers will be SKIPPED until APIFY_TOKEN is set")
        print()
    return checks


# ══════════════════════════════════════════════════════════════════════════════
#  SUPABASE CLEANUP HELPER
# ══════════════════════════════════════════════════════════════════════════════

def _cleanup(table: str, source: str, current_urls: set) -> int:
    """Delete rows from `table` for `source` whose URLs are no longer live."""
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
        print(f"  🗑  Removed {deleted} expired entries")
        return deleted
    except Exception as e:
        _warn(f"Cleanup failed (non-fatal): {e}")
        return 0


# ══════════════════════════════════════════════════════════════════════════════
#  DEVFOLIO SAVE  (upsert + cleanup)
# ══════════════════════════════════════════════════════════════════════════════

def _save_devfolio(records: list) -> tuple[int, int, int]:
    records = [r for r in records if r.get("url")]
    if not records:
        _warn("No valid records to save.")
        return 0, 0, 0

    current_urls = {r["url"] for r in records}

    try:
        res           = supabase.table("hackathons").select("url").eq("source", "devfolio").execute()
        existing_urls = {r["url"] for r in (res.data or [])}
    except Exception as e:
        _warn(f"Could not read existing rows: {e}")
        existing_urls = set()

    new_count     = len(current_urls - existing_urls)
    updated_count = len(current_urls & existing_urls)

    to_delete = list(existing_urls - current_urls)
    deleted   = 0
    if to_delete:
        try:
            for i in range(0, len(to_delete), 200):
                chunk = to_delete[i : i + 200]
                supabase.table("hackathons").delete() \
                    .eq("source", "devfolio").in_("url", chunk).execute()
                deleted += len(chunk)
            print(f"  🗑  Removed {deleted} expired entries")
        except Exception as e:
            _warn(f"Cleanup failed (non-fatal): {e}")

    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        try:
            supabase.table("hackathons").upsert(chunk, on_conflict="url").execute()
        except Exception as e:
            _fail(f"Upsert batch {i // BATCH_SIZE + 1} failed: {e}")

    _ok(f"{new_count} new  ·  {updated_count} updated  ·  {deleted} removed")
    return new_count, updated_count, deleted


# ══════════════════════════════════════════════════════════════════════════════
#  SCHOLARSHIP SAVE  (upsert + cleanup)
# ══════════════════════════════════════════════════════════════════════════════

def _save_scholarships(df: pd.DataFrame) -> tuple[int, int, int]:
    if df is None or df.empty:
        _warn("No scholarship data to save.")
        return 0, 0, 0

    df = df.copy()
    df["scraped_at"] = datetime.now(timezone.utc).isoformat()
    df = df.where(pd.notna(df), other=None)

    keep_cols = [
        "name", "amount", "eligibility", "last_date",
        "category", "provider", "logo_url", "detail_url",
        "mode", "scraped_at",
    ]
    df      = df[[c for c in keep_cols if c in df.columns]]
    records = df.to_dict(orient="records")

    cleaned = [
        {k: (None if isinstance(v, float) and v != v else v) for k, v in row.items()}
        for row in records
    ]
    cleaned = [r for r in cleaned if r.get("detail_url")]

    if not cleaned:
        _warn("All records missing detail_url — nothing to save.")
        return 0, 0, 0

    current_urls = {r["detail_url"] for r in cleaned}

    try:
        res           = supabase.table("scholarships").select("detail_url").execute()
        existing_urls = {r["detail_url"] for r in (res.data or [])}
    except Exception as e:
        _warn(f"Could not read existing scholarships: {e}")
        existing_urls = set()

    new_count     = len(current_urls - existing_urls)
    updated_count = len(current_urls & existing_urls)

    to_delete = list(existing_urls - current_urls)
    deleted   = 0
    if to_delete:
        try:
            for i in range(0, len(to_delete), 200):
                chunk = to_delete[i : i + 200]
                supabase.table("scholarships").delete().in_("detail_url", chunk).execute()
                deleted += len(chunk)
            print(f"  🗑  Removed {deleted} expired scholarships")
        except Exception as e:
            _warn(f"Scholarship cleanup failed (non-fatal): {e}")

    for i in range(0, len(cleaned), BATCH_SIZE):
        chunk = cleaned[i : i + BATCH_SIZE]
        try:
            supabase.table("scholarships").upsert(chunk, on_conflict="detail_url").execute()
        except Exception as e:
            _fail(f"Upsert batch {i // BATCH_SIZE + 1} failed: {e}")

    _ok(f"{new_count} new  ·  {updated_count} updated  ·  {deleted} removed")
    return new_count, updated_count, deleted


# ══════════════════════════════════════════════════════════════════════════════
#  INDIVIDUAL SCRAPER RUNNERS
# ══════════════════════════════════════════════════════════════════════════════

def _run_unstop_hackathons() -> tuple[int, int, int]:
    async def job():
        data = await unstop_fetch(max_pages=20)
        if not data:
            return 0, 0, 0
        saved   = unstop_hack_save(data)
        current = {h.get("seo_url", "") for h in data if h.get("seo_url")}
        deleted = _cleanup("hackathons", "unstop", current)
        _ok(f"{saved} inserted  ·  {deleted} removed")
        return saved, 0, deleted
    return asyncio.run(job())


def _run_devfolio() -> tuple[int, int, int]:
    async def job():
        data = await devfolio_fetch(max_scrolls=20)
        return _save_devfolio(data)
    return asyncio.run(job())


def _run_devpost() -> tuple[int, int, int]:
    data = devpost_fetch(max_pages=60)
    if not data:
        return 0, 0, 0
    saved   = devpost_save(data)
    current = {h.get("url", "") for h in data if h.get("url")}
    deleted = _cleanup("hackathons", "devpost", current)
    _ok(f"{saved} inserted  ·  {deleted} removed")
    return saved, 0, deleted


def _run_hackerearth() -> tuple[int, int, int]:
    data = he_fetch()
    if not data:
        return 0, 0, 0
    saved   = he_save(data)
    current = set()
    for h in data:
        url = h.get("url", "")
        if url.startswith("/"):
            url = f"https://www.hackerearth.com{url}"
        if url:
            current.add(url)
    deleted = _cleanup("hackathons", "hackerearth", current)
    _ok(f"{saved} inserted  ·  {deleted} removed")
    return saved, 0, deleted


def _run_unstop_internships() -> tuple[int, int, int]:
    async def job():
        data = await unstop_intern_fetch(max_pages=50)
        if not data:
            return 0, 0, 0
        saved   = unstop_intern_save(data)
        current = {item.get("seo_url", "") for item in data if item.get("seo_url")}
        deleted = _cleanup("internships", "unstop", current)
        _ok(f"{saved} inserted  ·  {deleted} removed")
        return saved, 0, deleted
    return asyncio.run(job())


def _run_buddy4study() -> tuple[int, int, int]:
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
        print(f"  Fetched {len(df)} scholarships  {breakdown}")
    return _save_scholarships(df)


# ── LinkedIn: combined runner ─────────────────────────────────────────────────
#
# Fetches all 4 role tasks, dedupes by URL across them, and tags each surviving
# job with the list of roles it matched. The save is done once under a single
# combined task_slug so cleanup is consistent and the DB no longer thrashes the
# task_slug field for jobs that appear in multiple role searches.

def _run_linkedin_all() -> tuple[int, int, int]:
    if not os.environ.get("APIFY_TOKEN"):
        _warn("Skipped — APIFY_TOKEN not set")
        return 0, 0, 0

    combined: dict[str, tuple[dict, list[str]]] = {}   # url → (raw_item, [roles])
    fetched_total = 0

    for role, task_id in LINKEDIN_TASKS.items():
        print(f"  Task: {role}")
        try:
            raw = linkedin_fetch(task_id=task_id)
        except Exception as e:
            _warn(f"    {role} fetch failed: {e}")
            continue

        if not raw:
            _warn(f"    {role}: no data returned")
            continue

        fetched_total += len(raw)
        added = 0
        for item in raw:
            url = (
                item.get("jobUrl")
                or item.get("url")
                or item.get("applyUrl")
                or ""
            )
            url = url.strip() if isinstance(url, str) else ""
            if not url:
                continue
            if url in combined:
                # Existing job — just record that this role also matched it
                if role not in combined[url][1]:
                    combined[url][1].append(role)
            else:
                combined[url] = (item, [role])
                added += 1
        print(f"    → {len(raw)} fetched, {added} unique-after-merge")

    if not combined:
        _warn("  No LinkedIn jobs across any task")
        return 0, 0, 0

    # Attach matched_roles to each raw item — linkedin_jobs._normalise reads it
    raw_items = []
    for url, (item, roles) in combined.items():
        item["_matched_roles"] = roles
        raw_items.append(item)

    print(
        f"  Combined: {len(raw_items)} unique jobs "
        f"from {fetched_total} fetched across {len(LINKEDIN_TASKS)} tasks "
        f"({fetched_total - len(raw_items)} cross-task duplicates merged)"
    )

    new, updated, deleted = linkedin_save(raw_items, task_id=LINKEDIN_COMBINED_SLUG)
    _ok(f"{new} new  ·  {updated} updated  ·  {deleted} removed")
    return new, updated, deleted


# ── Indeed ─────────────────────────────────────────────────────────────────────

def _run_indeed_jobs() -> tuple[int, int, int]:
    if not os.environ.get("APIFY_TOKEN"):
        _warn("Skipped — APIFY_TOKEN not set")
        return 0, 0, 0
    raw = indeed_fetch()
    if not raw:
        _warn("No data returned from Apify")
        return 0, 0, 0
    new, updated, deleted = indeed_save(raw)
    _ok(f"{new} new  ·  {updated} updated  ·  {deleted} removed")
    return new, updated, deleted


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPER REGISTRY  (runs every 6 hours)
# ══════════════════════════════════════════════════════════════════════════════

SCRAPERS = [
    ("Unstop",          "Hackathons",   _run_unstop_hackathons),
    ("Devfolio",        "Hackathons",   _run_devfolio),
    ("Devpost",         "Hackathons",   _run_devpost),
    ("HackerEarth",     "Hackathons",   _run_hackerearth),
    ("Unstop",          "Internships",  _run_unstop_internships),
    ("Buddy4Study",     "Scholarships", _run_buddy4study),
    ("LinkedIn (all)",  "Jobs",         _run_linkedin_all),
    ("Indeed",          "Jobs",         _run_indeed_jobs),
]


# ══════════════════════════════════════════════════════════════════════════════
#  ISOLATED RUNNER  (catches exceptions so one failure doesn't abort the rest)
# ══════════════════════════════════════════════════════════════════════════════

def _run_isolated(
    index: int, total: int, name: str, category: str, fn
) -> tuple[int, int, int] | None:
    _section(index, total, name, category)
    t0 = datetime.now()
    try:
        result = fn()
        print(f"  Done in {_elapsed(t0)}")
        return result
    except Exception as exc:
        _fail(f"{name} ({category}) failed after {_elapsed(t0)}: {exc}")
        traceback.print_exc()
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  MASTER JOB
# ══════════════════════════════════════════════════════════════════════════════

def run_all_scrapers() -> None:
    global _run_count
    _run_count += 1

    ts    = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    start = datetime.now()
    _banner(f"RUN #{_run_count}   {ts}")

    category_totals: dict[str, dict] = {}
    for _, cat, _ in SCRAPERS:
        if cat not in category_totals:
            category_totals[cat] = {"new": 0, "updated": 0, "removed": 0}

    failed: list[str] = []

    for i, (name, category, fn) in enumerate(SCRAPERS, 1):
        result = _run_isolated(i, len(SCRAPERS), name, category, fn)
        if result is None:
            failed.append(f"{name} ({category})")
        else:
            new, updated, removed = result
            category_totals[category]["new"]     += new
            category_totals[category]["updated"] += updated
            category_totals[category]["removed"] += removed

    elapsed = round((datetime.now() - start).total_seconds())
    col_w   = max(len(c) for c in category_totals)

    _banner(f"SUMMARY   {elapsed}s total")
    for cat, t in category_totals.items():
        print(
            f"  {cat:<{col_w}}   "
            f"{t['new']:>4} new   "
            f"{t['updated']:>4} updated   "
            f"{t['removed']:>3} removed"
        )

    if failed:
        print(f"\n  Failed scrapers:")
        for s in failed:
            print(f"    ✗  {s}")

    print(f"\n  Next run in 6 hours.\n")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    _banner("xShare Scheduler  v7")
    print()
    print("  Schedule:  every 6 hours")
    print("  Sources:   Unstop · Devfolio · Devpost · HackerEarth")
    print("             Unstop Internships · Buddy4Study Scholarships")
    print("             LinkedIn Jobs (4 roles merged) · Indeed Jobs  (Apify)")
    print()
    print("  Timezone:  Asia/Kolkata")

    _check_env()

    run_all_scrapers()

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_all_scrapers,
        IntervalTrigger(hours=6),
        id            = "xshare_6h",
        name          = "xShare — Every 6 Hours",
        max_instances = 1,
        coalesce      = True,
    )

    print(f"  Scheduler armed.  Press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\nStopped.")
        scheduler.shutdown()
