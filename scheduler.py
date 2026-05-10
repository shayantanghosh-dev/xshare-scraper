"""
xShare Hackathon Scheduler
Runs all scrapers every 6 hours.
Playwright scrapers: Unstop, Devfolio (require Chromium)
Requests scrapers:   Devpost, HackerEarth (no browser needed)
"""

import asyncio
import sys
sys.path.insert(0, '.')

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

# ── Connector imports ────────────────────────────────────────────────
from connectors.unstop      import fetch_hackathons as unstop_fetch,  save_to_supabase as unstop_save
from connectors.devfolio    import fetch_hackathons as devfolio_fetch, save_to_supabase as devfolio_save
from connectors.devpost     import fetch_hackathons as devpost_fetch,  save_to_supabase as devpost_save
from connectors.hackerearth import fetch_hackathons as he_fetch,       save_to_supabase as he_save


# ════════════════════════════════════════════════════════════════════
#  INDIVIDUAL RUNNERS
# ════════════════════════════════════════════════════════════════════

def run_unstop():
    print("\n── UNSTOP ──────────────────────────────────────")
    try:
        async def job():
            hackathons = await unstop_fetch(max_pages=20)
            print(f"  Fetched: {len(hackathons)}")
            return unstop_save(hackathons) if hackathons else 0
        saved = asyncio.run(job())
        print(f"  ✅ Unstop done — {saved} new records")
        return saved
    except Exception as e:
        print(f"  ❌ Unstop failed: {e}")
        return 0


def run_devfolio():
    print("\n── DEVFOLIO ────────────────────────────────────")
    try:
        async def job():
            hackathons = await devfolio_fetch(max_scrolls=20)
            print(f"  Fetched: {len(hackathons)}")
            return devfolio_save(hackathons) if hackathons else 0
        saved = asyncio.run(job())
        print(f"  ✅ Devfolio done — {saved} new records")
        return saved
    except Exception as e:
        print(f"  ❌ Devfolio failed: {e}")
        return 0


def run_devpost():
    print("\n── DEVPOST ─────────────────────────────────────")
    try:
        hackathons = devpost_fetch(max_pages=60)
        print(f"  Fetched: {len(hackathons)}")
        saved = devpost_save(hackathons) if hackathons else 0
        print(f"  ✅ Devpost done — {saved} new records")
        return saved
    except Exception as e:
        print(f"  ❌ Devpost failed: {e}")
        return 0


def run_hackerearth():
    print("\n── HACKEREARTH ─────────────────────────────────")
    try:
        hackathons = he_fetch()
        print(f"  Fetched: {len(hackathons)}")
        saved = he_save(hackathons) if hackathons else 0
        print(f"  ✅ HackerEarth done — {saved} new records")
        return saved
    except Exception as e:
        print(f"  ❌ HackerEarth failed: {e}")
        return 0


# ════════════════════════════════════════════════════════════════════
#  MASTER JOB — called every 6 hours
# ════════════════════════════════════════════════════════════════════

def run_all_scrapers():
    start = datetime.now()
    print(f"\n{'='*52}")
    print(f"  SCHEDULED RUN — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*52}")

    total = 0
    total += run_unstop()
    total += run_devfolio()
    total += run_devpost()
    total += run_hackerearth()

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*52}")
    print(f"  DONE — {total} new hackathons in {elapsed}s")
    print(f"  Next run in 6 hours.")
    print(f"{'='*52}\n")


# ════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 52)
    print("  XSHARE HACKATHON SCHEDULER")
    print("  Sources: Unstop · Devfolio · Devpost · HackerEarth")
    print("  Schedule: every 6 hours")
    print("=" * 52)
    print("\nRunning all scrapers now on startup...\n")

    run_all_scrapers()

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_all_scrapers,
        IntervalTrigger(hours=6),
        id="all_scrapers_6h",
        name="All Scrapers — Every 6 Hours",
        max_instances=1,
        coalesce=True,
    )

    print("Scheduler armed — running every 6 hours.")
    print("Press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\nScheduler stopped.")
        scheduler.shutdown()
