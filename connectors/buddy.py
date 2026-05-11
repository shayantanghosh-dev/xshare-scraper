"""
Buddy4Study Full Scholarship Scraper  (v10)
============================================
Changes from v9:
  • main() now RETURNS the cleaned DataFrame in addition to writing CSV.
    This lets buddy_scheduler.py call it directly without a temp-file roundtrip.
  • Moved into connector/ package to match project folder structure.
  • CLI behaviour unchanged — still works as a standalone script.
"""

import asyncio
import argparse
import json
import logging
import math
import re
import sys
from datetime import datetime

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL     = "https://www.buddy4study.com"
API_URL      = "https://api.buddy4study.com/api/v1.0/ssms/scholarship/"
LISTINGS_URL = f"{BASE_URL}/scholarships"

PAGE_SIZE    = 100
SORT_ORDER   = "DEADLINE"
DELAY_S      = 0.9
MAX_RETRIES  = 3
RETRY_WAIT_S = 2.0

MODE_LABELS = {
    "OPEN":        "Live",
    "CLOSED":      "Upcoming",
    "ALWAYS_OPEN": "Always Open",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("b4s")


# ═══════════════════════════════════════════════════════════════════════════════
# Data helpers
# ═══════════════════════════════════════════════════════════════════════════════

def clean(v) -> str:
    if v is None:
        return "N/A"
    s = re.sub(r"\s+", " ", str(v)).strip()
    # Strip any stray HTML tags (providerName sometimes contains <p> tags)
    s = re.sub(r"<[^>]+>", "", s).strip()
    return s if s and s not in ("null", "None", "0") else "N/A"


def _multilingual(item: dict) -> dict:
    ml = item.get("scholarshipMultilinguals")
    if isinstance(ml, list) and ml and isinstance(ml[0], dict):
        return ml[0]
    return {}


def normalise(item: dict, mode: str) -> dict:
    ml = _multilingual(item)

    page_slug  = clean(item.get("pageSlug"))
    detail_url = f"{BASE_URL}/{page_slug}" if page_slug != "N/A" else None

    provider = clean(ml.get("providerName"))
    if provider == "N/A":
        provider = clean(item.get("postedBy"))

    raw_date = item.get("deadlineDate")

    return {
        "name":        clean(item.get("scholarshipName") or ml.get("title")),
        "amount":      clean(ml.get("purposeAward")),
        "eligibility": clean(ml.get("applicableFor")),
        "last_date":   raw_date,          # kept as raw string; cleaned later
        "category":    clean(item.get("oppurtunityType")),
        "provider":    provider,
        "logo_url":    clean(item.get("logoFid")),
        "detail_url":  detail_url,        # None instead of "N/A" for DB nullable
        "mode":        MODE_LABELS.get(mode, mode),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Response parser
# ═══════════════════════════════════════════════════════════════════════════════

def parse_response(body, debug: bool = False, label: str = "") -> tuple[list, int]:
    """Returns (scholarships_list, total_count). total == -1 if absent."""
    if isinstance(body, list):
        return body, len(body)
    if not isinstance(body, dict):
        return [], -1

    if debug and label:
        log.debug(f"[{label}] keys: { {k: type(v).__name__ for k, v in body.items()} }")

    total = body.get("total", -1)
    items = body.get("scholarships")
    if isinstance(items, list):
        return items, int(total) if isinstance(total, (int, float)) and total > 0 else -1

    log.warning("  'scholarships' key missing — scanning response …")
    fallback_lists  = ("data","result","results","items","records","list",
                       "content","scholarshipList","scholarshipData")
    fallback_totals = ("total","totalCount","totalRecords","count","totalItems")

    if not (isinstance(total, (int, float)) and total > 0):
        for k in fallback_totals:
            v = body.get(k)
            if isinstance(v, (int, float)) and v > 0:
                total = int(v)
                break

    for k in fallback_lists:
        v = body.get(k)
        if isinstance(v, list):
            return v, int(total) if total > 0 else -1
        if isinstance(v, dict):
            for kk in fallback_lists:
                inner = v.get(kk)
                if isinstance(inner, list):
                    return inner, int(total) if total > 0 else -1

    return [], int(total) if isinstance(total, (int, float)) and total > 0 else -1


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 1 — browser intercept
# ═══════════════════════════════════════════════════════════════════════════════

async def browser_intercept(page, debug: bool) -> tuple[dict, dict, list, int]:
    calls = []
    lock  = asyncio.Lock()

    async def on_response(resp):
        if (
            "api.buddy4study.com" in resp.url
            and "/scholarship/" in resp.url
            and resp.status == 200
            and resp.request.method == "POST"
        ):
            try:
                req = resp.request
                try:
                    body_dict = json.loads(req.post_data or "{}")
                except Exception:
                    body_dict = {}

                resp_json = await resp.json()
                items, total = parse_response(resp_json, debug=debug, label="intercept")

                log.info(
                    f"  Intercepted  body={body_dict}  "
                    f"items={len(items)}  total={total}"
                )
                async with lock:
                    calls.append({
                        "headers":   dict(req.headers),
                        "body_dict": body_dict,
                        "items":     items,
                        "total":     total,
                    })
            except Exception as e:
                log.debug(f"  Intercept error: {e}")

    page.on("response", on_response)

    log.info(f"Opening {LISTINGS_URL} …")
    try:
        await page.goto(LISTINGS_URL, wait_until="load", timeout=60_000)
    except PlaywrightTimeout:
        log.warning("Page load timed out — using whatever was captured")

    await asyncio.sleep(3.0)
    if not calls or max((c["total"] for c in calls), default=0) < 50:
        log.info("  Scrolling to trigger lazy-loaded API calls …")
        for _ in range(4):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(1.2)

    page.remove_listener("response", on_response)

    if not calls:
        return {}, {}, [], -1

    best = max(calls, key=lambda c: len(c["items"]))
    log.info(
        f"  Best intercept → items={len(best['items'])}  "
        f"total={best['total']}  body={best['body_dict']}"
    )
    return best["headers"], best["body_dict"], best["items"], best["total"]


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2 — direct POST for remaining pages
# ═══════════════════════════════════════════════════════════════════════════════

def _build_headers(captured: dict) -> dict:
    headers = {k: v for k, v in captured.items() if not k.startswith(":")}
    headers["content-type"] = "application/json"
    headers["accept"]       = "application/json, text/plain, */*"
    return headers


def _build_payload(body_template: dict, mode: str, page_num: int) -> dict:
    payload = dict(body_template)
    payload["page"]      = page_num
    payload["length"]    = PAGE_SIZE
    payload["mode"]      = mode
    payload["sortOrder"] = payload.get("sortOrder", SORT_ORDER)
    return payload


async def fetch_page(
    context, req_headers: dict, body_template: dict,
    mode: str, page_num: int, debug: bool,
) -> tuple[list, int]:
    payload = _build_payload(body_template, mode, page_num)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await context.request.post(
                API_URL, headers=req_headers,
                data=json.dumps(payload), timeout=30_000,
            )
            if not resp.ok:
                try:
                    err_body = await resp.text()
                except Exception:
                    err_body = ""
                raise RuntimeError(f"HTTP {resp.status} — {err_body[:200]}")
            body = await resp.json()
            items, total = parse_response(body, debug=debug, label=f"p{page_num}")
            log.info(f"    items={len(items)}  total={total}")
            return items, total

        except Exception as e:
            if attempt == MAX_RETRIES:
                log.warning(f"  Page {page_num} failed after {MAX_RETRIES} attempts: {e}")
                return [], -1
            wait = RETRY_WAIT_S * attempt
            log.warning(f"  Attempt {attempt} failed ({e}) — retry in {wait:.0f}s")
            await asyncio.sleep(wait)

    return [], -1


# ═══════════════════════════════════════════════════════════════════════════════
# Per-mode scrape loop
# ═══════════════════════════════════════════════════════════════════════════════

async def scrape_mode(
    context,
    req_headers:       dict,
    body_template:     dict,
    intercepted_items: list,
    intercepted_total: int,
    intercepted_mode:  str,
    intercepted_page:  int,
    mode:              str,
    debug:             bool,
) -> list[dict]:
    label = MODE_LABELS.get(mode, mode)
    log.info(f"\n{'─'*55}")
    log.info(f"Mode: {label}  ({mode})")

    all_items:   list[dict] = []
    known_total: int        = -1

    if mode == intercepted_mode and intercepted_items:
        log.info(
            f"  Page {intercepted_page} (reused from interception): "
            f"{len(intercepted_items)} items  total={intercepted_total}"
        )
        all_items.extend(intercepted_items)
        known_total = intercepted_total
        next_page   = intercepted_page + 1
    else:
        log.info(f"  Page {intercepted_page} (fresh fetch for mode={mode}) …")
        items, total = await fetch_page(
            context, req_headers, body_template, mode, intercepted_page, debug
        )
        if not items:
            log.warning(f"  [{label}] No items on first page — skipping mode")
            return []
        all_items.extend(items)
        known_total = total
        next_page   = intercepted_page + 1

    def have_all() -> bool:
        return known_total > 0 and len(all_items) >= known_total

    page_num = next_page
    while not have_all():
        if known_total > 0:
            total_pages   = math.ceil(known_total / PAGE_SIZE)
            pages_fetched = page_num - intercepted_page
            log.info(
                f"  Page {page_num}  "
                f"({len(all_items)}/{known_total} items, "
                f"page {pages_fetched + 1}/{total_pages}) …"
            )
        else:
            log.info(f"  Page {page_num} …")

        items, total = await fetch_page(
            context, req_headers, body_template, mode, page_num, debug
        )

        if not items:
            log.info(f"  [{label}] Empty page {page_num} — done")
            break

        all_items.extend(items)
        if total > 0:
            known_total = total

        page_num += 1
        await asyncio.sleep(DELAY_S)

        if page_num > 500:
            log.warning("500-page safety cap hit")
            break

    actual = len(all_items)
    if known_total > 0 and actual < known_total:
        log.warning(
            f"  [{label}] Collected {actual} but expected {known_total} "
            f"— missing {known_total - actual}"
        )
    else:
        log.info(f"  [{label}] ✓ {actual}/{known_total} collected")

    return [normalise(item, mode) for item in all_items]


# ═══════════════════════════════════════════════════════════════════════════════
# Data cleaning — applied before both CSV export and Supabase upsert
# ═══════════════════════════════════════════════════════════════════════════════

def build_dataframe(all_records: list[dict]) -> pd.DataFrame:
    """
    Takes the raw list of normalised dicts and returns a fully cleaned DataFrame
    ready for both CSV export and Supabase upsert.
    """
    cols = ["name", "amount", "eligibility", "last_date",
            "category", "provider", "logo_url", "detail_url", "mode"]

    df = pd.DataFrame(all_records)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    # ── String columns: strip whitespace, replace empty/"N/A" with None ────────
    str_cols = ["name", "amount", "eligibility", "category",
                "provider", "logo_url", "detail_url", "mode"]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"N/A": None, "nan": None, "None": None, "": None})

    # ── last_date: parse to ISO date string (YYYY-MM-DD), invalid → None ───────
    df["last_date"] = pd.to_datetime(df["last_date"], errors="coerce")
    df["last_date"] = df["last_date"].dt.strftime("%Y-%m-%d")
    df["last_date"] = df["last_date"].where(df["last_date"].notna(), None)

    # ── For Upcoming scholarships, a past tentative date is meaningless ──────────
    # Null it out so the frontend doesn't incorrectly show "Expired"
    today = pd.Timestamp.now().normalize()
    mask_upcoming = df["mode"] == "Upcoming"
    mask_past     = pd.to_datetime(df["last_date"], errors="coerce") < today
    df.loc[mask_upcoming & mask_past, "last_date"] = None

    # ── Drop rows with no name or no detail_url (can't upsert without a key) ───
    df = df[df["name"].notna() & (df["name"].str.len() > 3)]
    df = df[df["detail_url"].notna()]

    # ── Deduplicate on detail_url (keep first occurrence) ──────────────────────
    df = df.drop_duplicates(subset=["detail_url"]).reset_index(drop=True)

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Main — returns DataFrame AND writes CSV
# ═══════════════════════════════════════════════════════════════════════════════

async def main(
    modes:    list[str],
    output:   str | None = None,
    headless: bool       = True,
    debug:    bool       = False,
) -> pd.DataFrame:
    """
    Scrapes all requested modes and returns a cleaned DataFrame.
    If output path is given, also writes a CSV.
    """
    if debug:
        log.setLevel(logging.DEBUG)

    all_records: list[dict] = []
    seen: set[str] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        bpage = await context.new_page()

        raw_headers, body_template, page0_items, page0_total = \
            await browser_intercept(bpage, debug)

        if not raw_headers:
            log.error("Interception failed — try --headless false to debug.")
            await browser.close()
            return pd.DataFrame()

        intercepted_mode = body_template.get("mode", modes[0])
        intercepted_page = int(body_template.get("page", 0))
        req_headers      = _build_headers(raw_headers)
        log.info(
            f"  Page base detected: {intercepted_page}  "
            f"mode: {intercepted_mode}  "
            f"forwarding {len(req_headers)} headers"
        )

        for mode in modes:
            records = await scrape_mode(
                context            = context,
                req_headers        = req_headers,
                body_template      = body_template,
                intercepted_items  = page0_items,
                intercepted_total  = page0_total,
                intercepted_mode   = intercepted_mode,
                intercepted_page   = intercepted_page,
                mode               = mode,
                debug              = debug,
            )
            new = 0
            for r in records:
                key = r.get("detail_url") or r.get("name", "")
                if key and key not in seen:
                    seen.add(key)
                    all_records.append(r)
                    new += 1
            log.info(f"  Unique new records: {new}")

        await browser.close()

    if not all_records:
        log.error("No scholarships collected.")
        return pd.DataFrame()

    df = build_dataframe(all_records)

    if output:
        df.to_csv(output, index=False, encoding="utf-8-sig")

    print(f"\n{'='*60}")
    print(f"  Done!  {len(df)} scholarships")
    if output:
        print(f"  CSV  →  {output}")
    print(f"{'='*60}")
    if len(modes) > 1:
        print("\nBreakdown by mode:")
        print(df["mode"].value_counts().to_string())
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nPreview (first 3):\n")
    print(df[["name", "amount", "eligibility", "last_date", "mode"]].head(3).to_string())

    return df


# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Buddy4Study scraper — v10")
    p.add_argument(
        "--modes", default="OPEN",
        help="Comma-separated: OPEN, CLOSED, ALWAYS_OPEN. Default: OPEN.",
    )
    p.add_argument("--output",   default=None)
    p.add_argument("--headless", default="true", choices=["true", "false"])
    p.add_argument("--debug",    action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args  = parse_args()
    modes = [m.strip().upper() for m in args.modes.split(",") if m.strip()]
    asyncio.run(main(
        modes    = modes,
        output   = args.output,
        headless = args.headless == "true",
        debug    = args.debug,
    ))