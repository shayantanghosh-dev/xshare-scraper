import asyncio
import sys
sys.path.insert(0, '.')

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from connectors.unstop import fetch_hackathons, save_to_supabase


def run_scraper():
    print(f"\n{'='*50}")
    print(f"SCHEDULED RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    async def job():
        hackathons = await fetch_hackathons(max_pages=20)
        print(f"Fetched: {len(hackathons)}")
        if hackathons:
            saved = save_to_supabase(hackathons)
            print(f"Done! Saved {saved} new hackathons.")
        else:
            print("No new hackathons found.")

    asyncio.run(job())


if __name__ == "__main__":
    print("=" * 50)
    print("XSHARE HACKATHON SCHEDULER")
    print("=" * 50)
    print("Runs daily at 8:00 AM automatically")
    print("Running once now on startup...\n")

    # Run immediately on startup
    run_scraper()

    # Schedule daily at 8 AM
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_scraper,
        CronTrigger(hour=8, minute=0),
        id="unstop_daily",
        name="Daily Unstop Scraper"
    )

    print("\nScheduler armed. Next run: tomorrow 8:00 AM IST")
    print("Press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\nScheduler stopped.")
        scheduler.shutdown()
