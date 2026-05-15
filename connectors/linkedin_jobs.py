"""
LinkedIn Jobs — Apify connector
=================================
Calls the saved Apify task  shayantan_ghosh-dev/linkedin-jobs-india-daily,
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

SOURCE     = "linkedin"
TABLE      = "jobs"
BATCH_SIZE = 500

# ── Apify task identifier (from your saved task URL) ───────────────────────────
TASK_ID    = "shayantan_ghosh-dev~linkedin-jobs-india-daily"   # ~ replaces /
APIFY_BASE = "https://api.apify.com/v2"

# How long to wait for the actor run to complete (seconds)
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


def _normalize_remote(location: str, work_type: str) -> bool:
    """Return True if the job is remote."""
    combined = f"{location} {work_type}".lower()
    return any(k in combined for k in ("remote", "hybrid", "virtual", "work from home"))


def _clean(v) -> str:
    """Return stripped string or empty string."""
    return str(v).strip() if v else ""


# ──────────────────────────────────────────────────────────────────────────────
# APIFY — run task + fetch dataset
# ──────────────────────────────────────────────────────────────────────────────

def _start_run(token: str) -> dict:
    """Start the Apify task and return the run object."""
    url = f"{APIFY_BASE}/actor-tasks/{TASK_ID}/runs"
    resp = requests.post(url, params={"token": token}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    run  = data.get("data", {})
    log.info(f"  Run started  id={run.get('id')}  status={run.get('status')}")
    return run


def _poll_run(run_id: str, token: str) -> dict:
    """Poll until the run reaches a terminal state. Returns final run data."""
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
    """Download all items from a dataset."""
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
    log.info("LinkedIn Jobs — Apify")
    log.info("=" * 52)

    # 1. Start
    run = _start_run(token)
    run_id = run.get("id")
    if not run_id:
        raise RuntimeError("No run ID returned from Apify.")

    # 2. Poll
    final_run = _poll_run(run_id, token)
    if final_run.get("status") != "SUCCEEDED":
        raise RuntimeError(
            f"Apify run ended with status: {final_run.get('status')}"
        )

    # 3. Fetch dataset
    dataset_id = final_run.get("defaultDatasetId")
    if not dataset_id:
        raise RuntimeError("No defaultDatasetId in completed run.")

    return _fetch_dataset(dataset_id, token)


# ──────────────────────────────────────────────────────────────────────────────
# NORMALISE
# ──────────────────────────────────────────────────────────────────────────────

def _normalise(item: dict) -> dict | None:
    """Map a raw LinkedIn Apify item to the jobs table schema."""
    # LinkedIn scraper field names (Apify's official LinkedIn Jobs Scraper actor)
    url = _clean(item.get("jobUrl") or item.get("url") or item.get("applyUrl", ""))
    if not url:
        return None

    title   = _clean(item.get("title") or item.get("jobTitle", ""))
    company = _clean(item.get("companyName") or item.get("company", ""))
    if not title:
        return None

    location      = _clean(item.get("location", ""))
    work_type     = _clean(item.get("workType") or item.get("workplaceType", ""))
    contract_type = _clean(item.get("contractType") or item.get("employmentType", ""))
    experience    = _clean(item.get("experienceLevel", ""))
    salary        = _clean(item.get("salary") or item.get("salaryRange", ""))
    description   = _clean(item.get("descriptionText") or item.get("description", ""))
    # Truncate description for tagline (first 300 chars)
    tagline = description[:300].rsplit(" ", 1)[0] + "…" if len(description) > 300 else description

    # Skills — some variants return a list, others a comma string
    skills_raw = item.get("skills") or []
    if isinstance(skills_raw, list):
        skills = [_clean(s) for s in skills_raw if s]
    else:
        skills = [s.strip() for s in str(skills_raw).split(",") if s.strip()]

    posted_at = _clean(
        item.get("postedAt") or item.get("postedDate") or item.get("publishedAt", "")
    )

    return {
        "source":           SOURCE,
        "title":            title,
        "company":          company,
        "location":         location or "India",
        "url":              url,
        "tagline":          tagline,
        "job_type":         contract_type or "Full-time",
        "experience_level": experience,
        "is_remote":        _normalize_remote(location, work_type),
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

    # Build records
    records = []
    for item in raw_items:
        rec = _normalise(item)
        if rec:
            records.append(rec)

    if not records:
        log.warning("  No valid records after normalisation.")
        return 0, 0, 0

    current_urls = {r["url"] for r in records}

    # Snapshot existing
    try:
        res           = supabase.table(TABLE).select("url").eq("source", SOURCE).execute()
        existing_urls = {r["url"] for r in (res.data or [])}
    except Exception as e:
        log.warning(f"  Could not read existing rows: {e}")
        existing_urls = set()

    new_count     = len(current_urls - existing_urls)
    updated_count = len(current_urls & existing_urls)

    # Remove stale jobs (no longer returned by the actor)
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
        log.info(f"  🗑  Removed {deleted} stale LinkedIn jobs")

    # Upsert
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
