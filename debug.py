import asyncio
import json
import sys
sys.path.insert(0, '.')

from playwright.async_api import async_playwright

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        captured = []
        
        async def handle_response(response):
            if "search-result" in response.url and "hackathon" in response.url:
                try:
                    data = await response.json()
                    items = data.get("data", {}).get("data", [])
                    captured.extend(items)
                except:
                    pass
        
        page.on("response", handle_response)
        await page.goto("https://unstop.com/hackathons?oppstatus=open", wait_until="networkidle")
        await page.wait_for_timeout(3000)
        await browser.close()
        
        if captured:
            print("ALL KEYS in first hackathon:")
            print(json.dumps(list(captured[0].keys()), indent=2))
            print("\nFULL FIRST HACKATHON:")
            print(json.dumps(captured[0], indent=2))

asyncio.run(debug())