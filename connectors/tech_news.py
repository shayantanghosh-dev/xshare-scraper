"""
connectors/tech_news.py
========================
Fetches the latest tech articles from 4 RSS feeds daily,
picks the 25 most recent across all sources,
summarises each to ~50 words using Claude Haiku,
and saves them to the `tech_news` Supabase table.

Sources:
  TechCrunch    — https://techcrunch.com/feed/
  The Verge     — https://www.theverge.com/rss/index.xml
  Ars Technica  — https://feeds.arstechnica.com/arstechnica/index
  VentureBeat   — https://venturebeat.com/feed/

Requires:
  ANTHROPIC_API_KEY  in .env / Railway Variables

Install:
  pip install feedparser anthropic
"""

import os
import re
import time
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import feedparser
from bs4 import BeautifulSoup
import anthropic

logger = logging.getLogger(__name__)

# ── Anthropic client ────────────────────────────────────────────────────────────
_anthropic = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

# ── RSS sources: (display_name, source_key, feed_url, articles_to_pull) ────────
RSS_SOURCES = [
    ("TechCrunch",   "techcrunch",   "https://techcrunch.com/feed/",                          7),
    ("The Verge",    "theverge",     "https://www.theverge.com/rss/index.xml",                6),
    ("Ars Technica", "arstechnica",  "https://feeds.arstechnica.com/arstechnica/index",       6),
    ("VentureBeat",  "venturebeat",  "https://venturebeat.com/feed/",                         6),
]
# Total articles = sum of per-source limits = 25

# ── helpers ─────────────────────────────────────────────────────────────────────

def _clean_html(raw: str, max_chars: int = 2500) -> str:
    """Strip HTML tags and collapse whitespace from RSS content."""
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "html.parser")
    for tag in soup(["script", "style", "img", "a"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _parse_published(entry) -> str | None:
    """Best-effort ISO timestamp from a feedparser entry."""
    # feedparser provides published_parsed (struct_time, UTC)
    if entry.get("published_parsed"):
        try:
            return datetime(
                *entry.published_parsed[:6], tzinfo=timezone.utc
            ).isoformat()
        except Exception:
            pass
    # Fallback: raw published string
    if entry.get("published"):
        try:
            return parsedate_to_datetime(entry.published).isoformat()
        except Exception:
            pass
    return None


def _extract_body(entry) -> str:
    """Pull article body text from the RSS entry — prefers full content over summary."""
    # Some feeds (Ars Technica) include full article in content[0].value
    if entry.get("content"):
        return _clean_html(entry.content[0].value)
    # Most feeds include a summary/description
    if entry.get("summary"):
        return _clean_html(entry.summary)
    return ""


def _categorise(title: str, source_key: str) -> str:
    """Infer article category from title keywords."""
    t = title.lower()
    if any(k in t for k in ["ai ", " ai", "llm", "gpt", "claude", "gemini", "openai",
                              "machine learning", "neural", "deep learning", "chatbot"]):
        return "AI"
    if any(k in t for k in ["startup", "funding", "raised", "series a", "series b",
                              "venture", "vc ", "valuation", "acquisition"]):
        return "Startups"
    if any(k in t for k in ["security", "hack", "vulnerability", "breach", "malware",
                              "ransomware", "phishing", "cyber", "exploit"]):
        return "Security"
    if any(k in t for k in ["open source", "github", "linux", "rust", "python",
                              "golang", "developer", "programming", "api"]):
        return "Open Source"
    if any(k in t for k in ["apple", "google", "microsoft", "meta ", "amazon",
                              "nvidia", "tesla", "samsung", "intel", "qualcomm"]):
        return "Big Tech"
    if any(k in t for k in ["crypto", "bitcoin", "blockchain", "web3",
                              "ethereum", "nft", "defi"]):
        return "Crypto"
    if source_key == "venturebeat":
        return "Startups"
    return "Tech"


def _summarise(title: str, body: str, source: str) -> str:
    """Call Claude Haiku to produce a ~50-word factual summary."""
    context = f"Source: {source}\nTitle: {title}\n\n{body}" if body else f"Source: {source}\nTitle: {title}"
    try:
        msg = _anthropic.messages.create(
            model      = "claude-haiku-4-5-20251001",
            max_tokens = 130,
            messages   = [{
                "role":    "user",
                "content": (
                    "Summarise this tech news article in exactly 50 words. "
                    "Be factual, informative and neutral. "
                    "No bullet points. Plain prose only. "
                    "Do not start with 'This article' or 'The article'.\n\n"
                    f"{context}"
                ),
            }],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        logger.warning(f"  Summarisation failed for '{title[:50]}': {e}")
        return (title[:220] + "…") if len(title) > 220 else title


# ── fetch from a single RSS source ──────────────────────────────────────────────

def _fetch_source(display_name: str, source_key: str, feed_url: str, limit: int) -> list[dict]:
    logger.info(f"  Fetching {display_name} RSS…")
    try:
        feed = feedparser.parse(feed_url)
        if feed.bozo and not feed.entries:
            logger.warning(f"    ⚠  Feed parse error for {display_name}: {feed.bozo_exception}")
            return []
        entries = feed.entries[:limit]
        logger.info(f"    Got {len(entries)} entries from {display_name}")
        return [(display_name, source_key, e) for e in entries]
    except Exception as e:
        logger.error(f"    ❌  Failed to fetch {display_name}: {e}")
        return []


# ── main entry point ────────────────────────────────────────────────────────────

def fetch_tech_news() -> list[dict]:
    """
    Fetches up to 25 articles from 4 RSS sources,
    summarises each with Claude Haiku,
    and returns a list of dicts ready for Supabase.
    """
    logger.info("=" * 50)
    logger.info("Tech News — fetching from RSS feeds")
    logger.info("=" * 50)

    # ── 1. Collect all entries from all sources ────────────────────────────────
    all_entries = []
    for display_name, source_key, feed_url, limit in RSS_SOURCES:
        entries = _fetch_source(display_name, source_key, feed_url, limit)
        all_entries.extend(entries)

    if not all_entries:
        logger.error("No entries collected from any source.")
        return []

    logger.info(f"  Total entries collected: {len(all_entries)}")

    # ── 2. Sort by published date descending (newest first) ────────────────────
    def _sort_key(item):
        entry = item[2]
        if entry.get("published_parsed"):
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        return datetime.min.replace(tzinfo=timezone.utc)

    all_entries.sort(key=_sort_key, reverse=True)

    # ── 3. Deduplicate by URL ──────────────────────────────────────────────────
    seen_urls = set()
    unique_entries = []
    for item in all_entries:
        entry = item[2]
        url   = entry.get("link", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_entries.append(item)

    unique_entries = unique_entries[:25]   # cap at 25
    logger.info(f"  Unique entries after dedup: {len(unique_entries)}")

    # ── 4. Summarise each entry ────────────────────────────────────────────────
    logger.info("  Summarising with Claude Haiku…")
    results = []
    for i, (display_name, source_key, entry) in enumerate(unique_entries, 1):
        title = entry.get("title", "").strip()
        url   = entry.get("link",  "").strip()
        if not title or not url:
            continue

        logger.info(f"  [{i}/{len(unique_entries)}] [{display_name}] {title[:55]}")

        body        = _extract_body(entry)
        summary     = _summarise(title, body, display_name)
        published   = _parse_published(entry)
        category    = _categorise(title, source_key)

        results.append({
            "title":        title,
            "summary":      summary,
            "url":          url,
            "source":       source_key,
            "score":        0,       # RSS has no score; kept for schema compat
            "category":     category,
            "published_at": published,
            "scraped_at":   datetime.now(timezone.utc).isoformat(),
        })

        time.sleep(0.25)   # gentle Claude rate limiting

    logger.info(f"  Done — {len(results)} news items ready.")

    # Print source breakdown
    from collections import Counter
    counts = Counter(r["source"] for r in results)
    for src, count in counts.items():
        display = next((d for d, k, _, __ in RSS_SOURCES if k == src), src)
        logger.info(f"    {display:<15} {count} articles")

    return results
