import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from playwright.async_api import async_playwright
from utils.supabase_client import supabase


async def fetch_hackathons(max_pages=20):
    all_hackathons = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        captured_pages = {}

        async def handle_response(response):
            if "search-result" in response.url and "hackathons" in response.url:
                try:
                    data = await response.json()
                    items = data.get("data", {}).get("data", [])
                    page_num = data.get("data", {}).get("current_page", 1)
                    last_page = data.get("data", {}).get("last_page", 1)
                    total = data.get("data", {}).get("total", 0)
                    if items:
                        captured_pages[page_num] = items
                        print(f"  → Page {page_num}/{last_page} | {len(items)} hackathons | Total: {total}")
                except Exception as e:
                    pass

        page.on("response", handle_response)

        # Load page first to get cookies
        print("Loading Unstop and getting session...")
        await page.goto("https://unstop.com/hackathons?oppstatus=open", wait_until="networkidle")
        await page.wait_for_timeout(3000)

        # Now fetch all pages using exact URL from DevTools
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

            items = response.get("data", {}).get("data", [])
            last_page = response.get("data", {}).get("last_page", 1)
            total = response.get("data", {}).get("total", 0)

            print(f"  → {len(items)} hackathons (Page {page_num}/{last_page}, Total: {total})")

            if not items:
                print("  → No more data.")
                break

            all_hackathons.extend(items)

            if page_num >= last_page:
                print(f"  → Reached last page!")
                break

            await page.wait_for_timeout(1000)

        await browser.close()

    return all_hackathons


def save_to_supabase(hackathons):
    saved = 0
    skipped = 0

    for h in hackathons:
        try:
            total_prize = sum(
                p.get("cash") or 0
                for p in h.get("prizes", [])
                if p.get("rank") not in ["All Participants"]
            )
            prize_str = f"₹{total_prize:,}" if total_prize > 0 else "Certificates/Others"
            tags = ", ".join([w.get("name", "") for w in h.get("workfunction", [])])

            record = {
                "title": h.get("title", ""),
                "organizer": h.get("organisation", {}).get("name", ""),
                "deadline": h.get("end_date", ""),
                "prize": prize_str,
                "mode": h.get("region", ""),
                "tags": tags,
                "source_url": h.get("seo_url", ""),
                "source": "unstop",
                "image_url": h.get("logoUrl2", ""),
                "status": "pending"
            }

            existing = supabase.table("hackathons")\
                .select("id")\
                .eq("source_url", record["source_url"])\
                .execute()

            if existing.data:
                skipped += 1
                continue

            supabase.table("hackathons").insert(record).execute()
            saved += 1
            print(f"  ✓ {record['title']} | {record['organizer']} | {record['prize']}")

        except Exception as e:
            print(f"  Error: {e}")

    print(f"\nSaved: {saved} | Skipped (duplicates): {skipped}")
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