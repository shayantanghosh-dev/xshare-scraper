"""
Indeed Jobs — Apify connector
Triggers the saved Apify task, polls until done, pulls dataset,
normalises, and upserts into the `jobs` Supabase table.

New fields extracted (v2):
  work_mode          — "Remote" / "Hybrid" / "On-site"  (from attributes/jobType/location)
  company_logo       — employer logo URL
  company_rating     — employer rating (float, e.g. 3.9)
  company_industry   — employer industry string
  company_size       — employee count string (e.g. "10,000+")
  company_website    — corporate website URL
  benefits           — list of benefit strings (health, 401k, PTO …)
  degree_required    — parsed from attributes (e.g. "Bachelor's degree")
  experience_years   — parsed from attributes (e.g. "6 years")
  description        — full plain-text job description
  tagline            — first ~300 chars of description (for card previews)

The Apify Indeed actor returns several nested / dict fields:
  - company     → employer dict  {name, logoUrl, ratingsValue, industry, …}
  - location    → dict {city, state, countryName, …}  OR plain string
  - description → dict {text, html}  OR plain string
  - baseSalary  → dict {min, max, unitOfWork, currencyCode}
  - attributes  → flat dict  {CODE: "label", …}  — skills, benefits, job type, degree, exp
  - benefits    → flat dict  {CODE: "label", …}
  - jobTypes    → flat dict  {CODE: "label", …}

All are handled defensively.

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

# ── Known attribute label patterns ────────────────────────────────────────────
# Indeed's `attributes` dict maps opaque codes to human-readable labels.
# We scan the values to pull out degree, experience, and work-mode hints.

_DEGREE_KEYWORDS = (
    "bachelor", "master", "phd", "doctorate", "associate",
    "high school", "diploma", "b.tech", "b.e.", "mba",
)
_EXPERIENCE_RE = re.compile(
    r"(\d+)\s*(?:\+\s*)?year", re.IGNORECASE
)
_REMOTE_KEYWORDS  = ("remote", "work from home", "virtual")
_HYBRID_KEYWORDS  = ("hybrid",)
_BENEFIT_KEYWORDS = (
    "insurance", "401(k)", "pension", "paid time", "paid holiday",
    "pto", "bonus", "stock", "maternity", "paternity", "leave",
    "reimbursement", "allowance", "provident fund", "esop", "gratuity",
)


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
    single spaces. Used for description and tagline fields.
    """
    if not v:
        return ""
    s = str(v)
    s = re.sub(r"<[^>]+>", " ", s)      # strip HTML tags
    s = re.sub(r"\s+", " ", s).strip()  # collapse whitespace / newlines
    return s


# ── field extractors ─────────────────────────────────────────────────────────

def _extract_company_fields(item: dict) -> dict:
    """
    Pulls all employer-related fields from the `employer` dict or top-level keys.
    Returns a flat dict with:
      company, company_logo, company_rating, company_industry,
      company_size, company_website
    """
    employer = item.get("employer") or {}

    # Company name
    name = ""
    for key in ("employerName", "company", "companyName"):
        val = _clean(item.get(key, ""))
        if val:
            name = val
            break
    if not name:
        name = _clean(employer.get("name", ""))

    logo    = _clean(employer.get("logoUrl", ""))
    rating  = employer.get("ratingsValue")          # float e.g. 3.9
    industry = _clean(employer.get("industry", ""))
    size    = _clean(employer.get("employeesCount", ""))
    website = _clean(employer.get("corporateWebsite", ""))

    # Normalise rating to float or None
    if rating is not None:
        try:
            rating = round(float(rating), 1)
        except (ValueError, TypeError):
            rating = None

    return {
        "company":          name,
        "company_logo":     logo,
        "company_rating":   rating,
        "company_industry": industry,
        "company_size":     size,
        "company_website":  website,
    }


def _extract_location(item: dict) -> str:
    """
    Location can be:
      - a plain string  → use directly
      - a dict {city, state, countryName, countryCode, …}
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
            state   = _clean(val.get("state", "") or val.get("admin1Code", ""))
            country = _clean(val.get("countryName", "") or val.get("countryCode", ""))
            parts   = [p for p in (city, state) if p]
            if parts:
                return ", ".join(parts)
            if country:
                return country
    return "India"


def _extract_description(item: dict) -> str:
    """
    Description can be a plain string or a dict {text, html}.
    Returns the full cleaned plain text.
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
    Prefers `baseSalary` dict {min, max, unitOfWork, currencyCode}.
    Falls back to plain-string salary fields.
    """
    # ── Structured baseSalary (Indeed's preferred field) ──
    bs = item.get("baseSalary")
    if isinstance(bs, dict) and (bs.get("min") or bs.get("max")):
        lo     = bs.get("min")
        hi     = bs.get("max")
        cur    = _clean(bs.get("currencyCode", "₹")) or "₹"
        period = _clean(bs.get("unitOfWork", "") or bs.get("period", "")).upper()
        suffix = {"YEAR": "/yr", "MONTH": "/mo", "HOUR": "/hr"}.get(period, "")
        if lo and hi:
            return f"{cur} {int(lo):,} – {cur} {int(hi):,}{suffix}"
        if hi:
            return f"Up to {cur} {int(hi):,}{suffix}"
        if lo:
            return f"From {cur} {int(lo):,}{suffix}"

    # ── Generic string / dict fallback ──
    for key in ("salary", "salaryRange", "compensation", "salaryInfo",
                "salaryText", "pay"):
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


def _extract_job_type(item: dict) -> str:
    """
    Job type / contract type.
    Also checks the `jobTypes` dict and `attributes` dict from Indeed.
    """
    # Direct fields
    for key in ("jobType", "contractType", "employmentType"):
        val = _clean(item.get(key, ""))
        if val and val.lower() not in ("null", "none"):
            return val

    # jobTypes dict  {CODE: "Full-time", …}
    jt = item.get("jobTypes")
    if isinstance(jt, dict) and jt:
        return next(iter(jt.values()))

    # attributes dict  — look for job-type-ish values
    attrs = item.get("attributes") or item.get("employerAttributes") or {}
    if isinstance(attrs, dict):
        for label in attrs.values():
            lc = _clean(label).lower()
            if any(k in lc for k in ("full-time", "part-time", "contract",
                                      "temporary", "internship", "permanent")):
                return _clean(label)

    return "Full-time"


def _extract_work_mode(item: dict, location: str) -> str:
    """
    Classify the job as Remote / Hybrid / On-site.
    Checks attributes, jobTypes, and the location string.
    """
    # Check attributes dict for remote/hybrid signals
    attrs = item.get("attributes") or item.get("employerAttributes") or {}
    if isinstance(attrs, dict):
        for label in attrs.values():
            lc = _clean(label).lower()
            if any(k in lc for k in _REMOTE_KEYWORDS):
                return "Remote"
            if any(k in lc for k in _HYBRID_KEYWORDS):
                return "Hybrid"

    # Check jobType field
    for key in ("jobType", "contractType", "workType", "remoteAllowed"):
        val = _clean(item.get(key, "")).lower()
        if "remote" in val:
            return "Remote"
        if "hybrid" in val:
            return "Hybrid"

    # Fall back to location string
    loc = location.lower()
    if "remote" in loc:
        return "Remote"
    if "hybrid" in loc:
        return "Hybrid"

    return "On-site"


def _extract_benefits(item: dict) -> list[str]:
    """
    Indeed provides a `benefits` dict {CODE: "label"}.
    Also scans `attributes` for benefit-like values.
    Returns a list of human-readable benefit strings.
    """
    seen:   set[str]  = set()
    result: list[str] = []

    def _add(label: str) -> None:
        clean = _clean(label)
        if clean and clean.lower() not in seen:
            seen.add(clean.lower())
            result.append(clean)

    # Dedicated benefits dict
    benefits = item.get("benefits")
    if isinstance(benefits, dict):
        for label in benefits.values():
            _add(label)

    # socialInsurance dict (subset of benefits on some actors)
    social = item.get("socialInsurance")
    if isinstance(social, dict):
        for label in social.values():
            _add(label)

    # Scan attributes for anything that looks like a benefit
    attrs = item.get("attributes") or {}
    if isinstance(attrs, dict):
        for label in attrs.values():
            lc = _clean(label).lower()
            if any(k in lc for k in _BENEFIT_KEYWORDS):
                _add(label)

    return result


def _extract_degree_and_experience(item: dict) -> tuple[str, str]:
    """
    Parses the `attributes` dict for:
      - degree_required  e.g. "Bachelor's degree", "Master's degree"
      - experience_years e.g. "6 years"
    Returns (degree, experience_years) — either may be ''.
    """
    degree = ""
    exp    = ""

    attrs = item.get("attributes") or {}
    if not isinstance(attrs, dict):
        return degree, exp

    for label in attrs.values():
        lc = _clean(label).lower()

        # Degree
        if not degree and any(k in lc for k in _DEGREE_KEYWORDS):
            degree = _clean(label)

        # Years of experience
        if not exp:
            m = _EXPERIENCE_RE.search(lc)
            if m:
                exp = _clean(label)   # e.g. "6 years"

    # Also check dedicated experience fields
    if not exp:
        for key in ("experienceLevel", "experience", "minimumExperience",
                    "requiredExperience", "jobLevel", "seniorityLevel"):
            val = item.get(key)
            if not val:
                continue
            if isinstance(val, (int, float)) and val > 0:
                exp = f"{int(val)}+ years"
                break
            cleaned = _clean(str(val))
            if cleaned and cleaned.lower() not in ("null", "none", ""):
                exp = cleaned
                break

    return degree, exp


def _extract_posted_at(item: dict) -> str:
    """Date the job was posted. Prefer ISO strings over relative strings."""
    for key in ("datePublished", "datePosted", "postedAt", "publishedAt",
                "date", "postedDate", "dateOnIndeed", "postDate",
                "created_at", "formattedDate", "datePostedFormatted"):
        val = _clean(item.get(key, ""))
        if val and val.lower() not in ("null", "none", ""):
            return val
    return ""


def _extract_applicants(item: dict) -> str:
    """Number of applicants (not always present on Indeed)."""
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


def _extract_skills(item: dict) -> list[str]:
    """Skills list — may be absent for Indeed; handled gracefully."""
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

    # Fall back: scan attributes for skill-ish values
    # (filter out degree, experience, benefit, job-type labels already captured)
    attrs = item.get("attributes") or {}
    if isinstance(attrs, dict):
        _non_skill = _DEGREE_KEYWORDS + _BENEFIT_KEYWORDS + (
            "full-time", "part-time", "contract", "temporary",
            "remote", "hybrid", "on-site", "year",
        )
        skills = []
        for label in attrs.values():
            lc = _clean(label).lower()
            if not any(k in lc for k in _non_skill):
                skills.append(_clean(label))
        if skills:
            return skills

    return []


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

    # ── aggregated extractions ─────────────────────────────────────────────
    company_fields               = _extract_company_fields(item)
    location                     = _extract_location(item)
    description                  = _extract_description(item)
    salary                       = _extract_salary(item)
    job_type                     = _extract_job_type(item)
    work_mode                    = _extract_work_mode(item, location)
    benefits                     = _extract_benefits(item)
    degree_required, exp_years   = _extract_degree_and_experience(item)

    tagline = (
        description[:300].rsplit(" ", 1)[0] + "…"
        if len(description) > 300
        else description
    )

    return {
        # ── core identity ──────────────────────────────────────────────────
        "source":             SOURCE,
        "url":                url,

        # ── job basics ─────────────────────────────────────────────────────
        "title":              title,
        "tagline":            tagline,
        "description":        description,
        "job_type":           job_type,
        "work_mode":          work_mode,

        # ── company ────────────────────────────────────────────────────────
        "company":            company_fields["company"],
        "company_logo":       company_fields["company_logo"],
        "company_rating":     company_fields["company_rating"],
        "company_industry":   company_fields["company_industry"],
        "company_size":       company_fields["company_size"],
        "company_website":    company_fields["company_website"],

        # ── location & remote ──────────────────────────────────────────────
        "location":           location,
        "is_remote":          work_mode in ("Remote", "Hybrid"),

        # ── compensation ───────────────────────────────────────────────────
        "salary":             salary,
        "benefits":           benefits,            # list[str]

        # ── requirements ───────────────────────────────────────────────────
        "degree_required":    degree_required,
        "experience_level":   exp_years,

        # ── apply info ─────────────────────────────────────────────────────
        "applicants":         _extract_applicants(item),

        # ── skills & dates ─────────────────────────────────────────────────
        "skills":             _extract_skills(item),
        "date_posted":        _extract_posted_at(item),
        "scraped_at":         datetime.now(timezone.utc).isoformat(),
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
