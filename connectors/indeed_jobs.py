"""
Indeed Jobs — Apify connector
================================
Calls the saved Apify task  shayantan_ghosh-dev/indeed-jobs-india-daily,
polls until the run finishes, pulls all items from the default dataset,
normalises them, and upserts into the `jobs` Supabase table.

Env vars required:
  APIFY_TOKEN   — your Apify API token (Settings → Integrations)
  SUPABASE_URL
  SUPABASE_KEY
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.supabase_client import supabase

log = logging.getLogger(__name__)

SOURCE     = "indeed"
TABLE      = "jobs"
BATCH_SIZE = 500

# ── Apify task identifier (from your saved task URL) ───────────────────────────
TASK_ID    = "shayantan_ghosh-dev~indeed-jobs-india-daily"   # ~ replaces /
APIFY_BASE = "https://api.apify.com/v2"

POLL_INTERVAL = 10
MAX_WAIT_S    = 600   # 10 minutes


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _token() -> str:
    t = os.environ.get("APIFY_TOKEN", "")
    if not t:
        raise EnvironmentError("APIFY_TOKEN environment variable is not set.")
    return t


def _clean(v) -> str:
    return str(v).strip() if v else ""


def _is_remote(location: str, job_type: str) -> bool:
    combined = f"{location} {job_type}".lower()
    return any(k in combined for k in ("remote", "hybrid", "work from home", "virtual"))


# ──────────────────────────────────────────────────────────────────────────────
# APIFY — run task + fetch dataset
# ──────────────────────────────────────────────────────────────────────────────

def _start_run(token: str) -> dict:
    url  = f"{APIFY_BASE}/actor-tasks/{TASK_ID}/runs"
    resp = requests.post(url, params={"token": token}, timeout=30)
    resp.raise_for_status()
    run  = resp.json().get("data", {})
    log.info(f"  Run started  id={run.get('id')}  status={run.get('status')}")
    return run


def _poll_run(run_id: str, token: str) -> dict:
    url     = f"{APIFY_BASE}/actor-runs/{run_id}"
    elapsed = 0

    while elapsed < MAX_WAIT_S:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

        resp   = requests.get(url, params={"token": token}, timeout=30)
        resp.raise_for_status()
        run    = resp.json().get("data", {})
        status = run.get("status", "")

        log.info(f"  [{elapsed:>4}s] Run {run_id} → {status}")

        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            return run

    raise TimeoutError(f"Run {run_id} did not finish within {MAX_WAIT_S}s")


def _fetch_dataset(dataset_id: str, token: str) -> list[dict]:
    url  = f"{APIFY_BASE}/datasets/{dataset_id}/items"
    resp = requests.get(
        url,
        params={"token": token, "format": "json", "clean": "true"},
        timeout=120,
    )
    resp.raise_for_status()
    items = resp.json()
    log.info(f"  Dataset {dataset_id} → {len(items)} items")
    return items if isinstance(items, list) else []


def fetch_jobs() -> list[dict]:
    """
    Trigger the Apify task, wait for it to finish, and return raw item list.
    """
    token = _token()

    log.info("=" * 52)
    log.info("Indeed Jobs — Apify")
    log.info("=" * 52)

    run = _start_run(token)
    run_id = run.get("id")
    if not run_id:
        raise RuntimeError("No run ID returned from Apify.")

    final_run = _poll_run(run_id, token)
    if final_run.get("status") != "SUCCEEDED":
        raise RuntimeError(
            f"Apify run ended with status: {final_run.get('status')}"
        )

    dataset_id = final_run.get("defaultDatasetId")
    if not dataset_id:
        raise RuntimeError("No defaultDatasetId in completed run.")

    return _fetch_dataset(dataset_id, token)


# ──────────────────────────────────────────────────────────────────────────────
# NORMALISE
# ──────────────────────────────────────────────────────────────────────────────

def _normalise(item: dict) -> dict | None:
    """Map a raw Indeed Apify item to the jobs table schema."""
    # Indeed scraper field names (Apify's official Indeed Jobs Scraper actor)
    url = _clean(
        item.get("url")
        or item.get("jobUrl")
        or item.get("externalApplyLink", "")
    )
    if not url:
        return None

    title   = _clean(item.get("positionName") or item.get("title") or item.get("jobTitle", ""))
    company = _clean(item.get("company") or item.get("companyName", ""))
    if not title:
        return None

    location    = _clean(item.get("location") or item.get("jobLocation", ""))
    job_type    = _clean(item.get("jobType") or item.get("contractType", ""))
    salary      = _clean(item.get("salary") or item.get("salaryRange", ""))
    description = _clean(item.get("description") or item.get("descriptionText", ""))
    tagline     = description[:300].rsplit(" ", 1)[0] + "…" if len(description) > 300 else description

    posted_at = _clean(
        item.get("datePostedFormatted")
        or item.get("postedAt")
        or item.get("publishedAt", "")
    )

    # Indeed sometimes returns skills as a list, sometimes not at all
    skills_raw = item.get("skills") or []
    if isinstance(skills_raw, list):
        skills = [_clean(s) for s in skills_raw if s]
    else:
        skills = [s.strip() for s in str(skills_raw).split(",") if s.strip()]

    return {
        "source":           SOURCE,
        "title":            title,
        "company":          company,
        "location":         location or "India",
        "url":              url,
        "tagline":          tagline,
        "job_type":         job_type or "Full-time",
        "experience_level": _clean(item.get("experienceLevel", "")),
        "is_remote":        _is_remote(location, job_type),
        "salary":           salary or "Not disclosed",
        "skills":           skills,
        "date_posted":      posted_at,
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# SAVE
# ──────────────────────────────────────────────────────────────────────────────

def save_to_supabase(raw_items: list[dict]) -> tuple[int, int, int]:
    """
    Upsert jobs into Supabase `jobs` table.
    Returns (new, updated, deleted).
    """
    if not raw_items:
        return 0, 0, 0

    records = []
    for item in raw_items:
        rec = _normalise(item)
        if rec:
            records.append(rec)

    if not records:
        log.warning("  No valid records after normalisation.")
        return 0, 0, 0

    current_urls = {r["url"] for r in records}

    try:
        res           = supabase.table(TABLE).select("url").eq("source", SOURCE).execute()
        existing_urls = {r["url"] for r in (res.data or [])}
    except Exception as e:
        log.warning(f"  Could not read existing rows: {e}")
        existing_urls = set()

    new_count     = len(current_urls - existing_urls)
    updated_count = len(current_urls & existing_urls)

    to_delete = list(existing_urls - current_urls)
    deleted   = 0
    if to_delete:
        for i in range(0, len(to_delete), 200):
            chunk = to_delete[i : i + 200]
            try:
                supabase.table(TABLE).delete() \
                    .eq("source", SOURCE).in_("url", chunk).execute()
                deleted += len(chunk)
            except Exception as e:
                log.warning(f"  Delete chunk failed: {e}")
        log.info(f"  🗑  Removed {deleted} stale Indeed jobs")

    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        try:
            supabase.table(TABLE).upsert(chunk, on_conflict="url").execute()
        except Exception as e:
            log.warning(f"  Upsert batch {i // BATCH_SIZE + 1} failed: {e}")

    log.info(f"  ✓  {new_count} new  ·  {updated_count} updated  ·  {deleted} removed")
    return new_count, updated_count, deleted


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    items = fetch_jobs()
    print(f"\nFetched: {len(items)} raw items")
    if items:
        new, updated, deleted = save_to_supabase(items)
        print(f"✅ Done — {new} new  ·  {updated} updated  ·  {deleted} removed")
    else:
        print("❌ No items returned from Apify.")
