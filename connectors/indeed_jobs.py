"""
Indeed Jobs — Apify connector
Triggers the saved Apify task, polls until done, pulls dataset,
normalises, and upserts into the `jobs` Supabase table.

The Apify Indeed actor returns several nested / dict fields:
  - company     → "employerName" string  (not "company")
  - location    → dict {city, state, countryName, ...}  OR plain string
  - description → dict {text: "...", html: "..."} OR plain string
  - salary      → dict {min, max, currency, period}  OR plain string
  - date_posted → "datePosted" (not "datePostedFormatted")

All of these are handled defensively in the extractor functions.

Env vars required:
  APIFY_TOKEN
  SUPABASE_URL
  SUPABASE_KEY
"""

import os
import re
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

TASK_ID    = "shayantan_ghosh-dev~indeed-jobs-india-daily"
APIFY_BASE = "https://api.apify.com/v2"

POLL_INTERVAL = 15
MAX_WAIT_S    = 600


# ── helpers ───────────────────────────────────────────────────────────────────

def _token() -> str:
    t = os.environ.get("APIFY_TOKEN", "")
    if not t:
        raise EnvironmentError("APIFY_TOKEN is not set.")
    return t

def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

def _clean(v) -> str:
    """Stringify and strip; return '' for None/empty."""
    return str(v).strip() if v else ""

def _clean_text(v) -> str:
    """
    Strip HTML tags and collapse all whitespace (including \\n \\t) to
    single spaces. Used for description → tagline extraction.
    """
    if not v:
        return ""
    s = str(v)
    s = re.sub(r"<[^>]+>", " ", s)      # strip HTML
    s = re.sub(r"\s+", " ", s).strip()  # collapse whitespace / newlines
    return s

def _is_remote(location: str, job_type: str) -> bool:
    combined = f"{location} {job_type}".lower()
    return any(k in combined for k in ("remote", "hybrid", "work from home", "virtual"))


# ── field extractors ─────────────────────────────────────────────────────────

def _extract_company(item: dict) -> str:
    """
    The Apify Indeed actor stores the company under 'employerName'.
    Fall back through several alternatives in case the schema varies.
    """
    for key in ("employerName", "employer", "company",
                "companyName", "company_name", "hiringOrganization"):
        val = item.get(key)
        if not val:
            continue
        if isinstance(val, dict):
            name = val.get("name") or val.get("companyName") or ""
            if name:
                return _clean(name)
        else:
            cleaned = _clean(val)
            if cleaned:
                return cleaned
    return ""


def _extract_location(item: dict) -> str:
    """
    Location can be:
      - a plain string  → use directly
      - a dict {city, state, countryName, countryCode, ...}
    """
    for key in ("location", "jobLocation", "locationName", "jobCity"):
        val = item.get(key)
        if not val:
            continue
        if isinstance(val, str):
            cleaned = val.strip()
            if cleaned:
                return cleaned
        if isinstance(val, dict):
            city    = _clean(val.get("city", ""))
            state   = _clean(val.get("state", ""))
            country = _clean(val.get("countryName", "") or val.get("countryCode", ""))
            parts   = [p for p in (city, state) if p]
            if parts:
                return ", ".join(parts)
            if country:
                return country
    return "India"


def _extract_description(item: dict) -> str:
    """
    Description can be a plain string or a dict {text: '...', html: '...'}.
    Returns cleaned plain text (no HTML, no raw newlines).
    """
    for key in ("description", "descriptionText", "jobDescription",
                "summary", "jobContent", "fullDescription"):
        val = item.get(key)
        if not val:
            continue
        if isinstance(val, dict):
            text = val.get("text") or val.get("html") or ""
            if text:
                return _clean_text(text)
        else:
            cleaned = _clean_text(val)
            if cleaned:
                return cleaned
    return ""


def _extract_salary(item: dict) -> str:
    """
    Salary can be:
      - a plain string
      - a dict {from/min, to/max, currency, period}
    """
    for key in ("salary", "salaryRange", "compensation",
                "salaryInfo", "salaryText", "pay"):
        val = item.get(key)
        if not val:
            continue
        if isinstance(val, str):
            cleaned = val.strip()
            if cleaned and cleaned.lower() not in ("null", "none", "0", ""):
                return cleaned
        if isinstance(val, dict):
            lo  = val.get("from") or val.get("min") or val.get("minimum")
            hi  = val.get("to")   or val.get("max") or val.get("maximum")
            cur = val.get("currency", "₹")
            per = (val.get("period") or val.get("type") or "").lower()
            suffix = {
                "monthly": "/mo", "yearly": "/yr",
                "annual":  "/yr", "hourly": "/hr",
            }.get(per, "")
            if lo and hi:
                return f"{cur}{int(lo):,} – {cur}{int(hi):,}{suffix}"
            if hi:
                return f"Up to {cur}{int(hi):,}{suffix}"
            if lo:
                return f"From {cur}{int(lo):,}{suffix}"
    return "Not disclosed"


def _extract_experience(item: dict) -> str:
    """Experience level — Indeed actors use varying field names."""
    for key in ("experienceLevel", "experience", "jobLevel",
                "seniorityLevel", "seniority", "careerLevel",
                "minimumExperience", "requiredExperience"):
        val = item.get(key)
        if not val:
            continue
        if isinstance(val, str):
            cleaned = val.strip()
            if cleaned and cleaned.lower() not in ("null", "none", ""):
                return cleaned
        if isinstance(val, (int, float)) and val > 0:
            return f"{int(val)}+ years"
    return ""


def _extract_skills(item: dict) -> list[str]:
    """Skills list — may be absent for Indeed; handle gracefully."""
    for key in ("skills", "requiredSkills", "preferredSkills",
                "jobSkills", "skillList", "qualifications"):
        raw = item.get(key)
        if not raw:
            continue
        if isinstance(raw, list) and raw:
            result = []
            for s in raw:
                if isinstance(s, dict):
                    name = _clean(s.get("name") or s.get("skill") or "")
                elif isinstance(s, str):
                    name = s.strip()
                else:
                    name = ""
                if name:
                    result.append(name)
            if result:
                return result
        if isinstance(raw, str) and raw.strip():
            return [s.strip() for s in raw.split(",") if s.strip()]
    return []


def _extract_posted_at(item: dict) -> str:
    """Date the job was posted; try every field name Indeed actors use."""
    for key in ("datePosted", "postedAt", "publishedAt", "date",
                "postedDate", "datePostedFormatted", "postDate",
                "created_at", "formattedDate"):
        val = _clean(item.get(key, ""))
        if val and val.lower() not in ("null", "none", ""):
            return val
    return ""


def _extract_applicants(item: dict) -> str:
    """Number of applicants (not always available on Indeed)."""
    for key in ("numberOfApplicants", "applicantCount", "applies",
                "totalApplicants", "applicants"):
        val = item.get(key)
        if val is None:
            continue
        try:
            n = int(val)
            if n > 0:
                return f"{n:,}"
        except (ValueError, TypeError):
            s = _clean(str(val))
            if s and s.lower() not in ("null", "none", "0"):
                return s
    return ""


# ── Apify API calls ───────────────────────────────────────────────────────────

def _start_run(token: str) -> dict:
    url  = f"{APIFY_BASE}/actor-tasks/{TASK_ID}/runs"
    resp = requests.post(url, headers=_headers(token), timeout=30)
    if not resp.ok:
        log.error(f"  Apify trigger failed: {resp.status_code} — {resp.text[:300]}")
    resp.raise_for_status()
    run = resp.json().get("data", {})
    log.info(f"  Run started  id={run.get('id')}  status={run.get('status')}")
    return run


def _poll_run(run_id: str, token: str) -> dict:
    url     = f"{APIFY_BASE}/actor-runs/{run_id}"
    elapsed = 0

    while elapsed < MAX_WAIT_S:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

        resp   = requests.get(url, headers=_headers(token), timeout=30)
        resp.raise_for_status()
        run    = resp.json().get("data", {})
        status = run.get("status", "")

        print(f"  [{elapsed:>4}s] Run status: {status}")

        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            return run

    raise TimeoutError(f"Run {run_id} did not finish within {MAX_WAIT_S}s")


def _fetch_dataset(dataset_id: str, token: str) -> list[dict]:
    url  = f"{APIFY_BASE}/datasets/{dataset_id}/items"
    resp = requests.get(
        url,
        headers=_headers(token),
        params={"format": "json", "clean": "true"},
        timeout=120,
    )
    resp.raise_for_status()
    items = resp.json()
    print(f"  Dataset {dataset_id} → {len(items)} items")
    return items if isinstance(items, list) else []


def fetch_jobs() -> list[dict]:
    token = _token()

    print("  Starting Indeed Apify run...")
    run    = _start_run(token)
    run_id = run.get("id")
    if not run_id:
        raise RuntimeError("No run ID returned from Apify.")

    final_run = _poll_run(run_id, token)
    if final_run.get("status") != "SUCCEEDED":
        raise RuntimeError(f"Apify run ended with status: {final_run.get('status')}")

    dataset_id = final_run.get("defaultDatasetId")
    if not dataset_id:
        raise RuntimeError("No defaultDatasetId in completed run.")

    return _fetch_dataset(dataset_id, token)


# ── normalise ─────────────────────────────────────────────────────────────────

def _normalise(item: dict) -> dict | None:
    url = _clean(
        item.get("url") or item.get("jobUrl") or item.get("externalApplyLink", "")
    )
    if not url:
        return None

    title = _clean(
        item.get("positionName") or item.get("title") or item.get("jobTitle", "")
    )
    if not title:
        return None

    company     = _extract_company(item)
    location    = _extract_location(item)
    description = _extract_description(item)
    salary      = _extract_salary(item)

    tagline = (
        description[:300].rsplit(" ", 1)[0] + "…"
        if len(description) > 300
        else description
    )

    job_type = _clean(item.get("jobType") or item.get("contractType", ""))

    return {
        "source":           SOURCE,
        "title":            title,
        "company":          company,
        "location":         location,
        "url":              url,
        "tagline":          tagline,
        "job_type":         job_type or "Full-time",
        "experience_level": _extract_experience(item),
        "is_remote":        _is_remote(location, job_type),
        "salary":           salary,
        "skills":           _extract_skills(item),
        "applicants":       _extract_applicants(item),
        "date_posted":      _extract_posted_at(item),
        "scraped_at":       datetime.now(timezone.utc).isoformat(),
    }


# ── save ──────────────────────────────────────────────────────────────────────

def save_to_supabase(raw_items: list[dict]) -> tuple[int, int, int]:
    if not raw_items:
        return 0, 0, 0

    records = [r for r in (_normalise(i) for i in raw_items) if r]
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
        print(f"  🗑  Removed {deleted} stale Indeed jobs")

    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]
        try:
            supabase.table(TABLE).upsert(chunk, on_conflict="url").execute()
        except Exception as e:
            log.warning(f"  Upsert batch {i // BATCH_SIZE + 1} failed: {e}")

    return new_count, updated_count, deleted


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    items = fetch_jobs()
    print(f"\nFetched: {len(items)} raw items")
    if items:
        new, updated, deleted = save_to_supabase(items)
        print(f"✅ Done — {new} new  ·  {updated} updated  ·  {deleted} removed")
    else:
        print("❌ No items returned from Apify.")
