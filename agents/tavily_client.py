"""
Tavily client — competitor discovery and review-based fallback.
"""

import logging
from typing import Dict, List

from config import (
    TAVILY_API_KEY,
    TAVILY_MAX_RESULTS,
    TAVILY_REVIEWS_MAX_RESULTS,
    TAVILY_SEARCH_DEPTH,
)

log = logging.getLogger(__name__)


def reset_tavily_query_count() -> None:
    """No-op; kept for API compatibility."""
    pass


def get_tavily_query_count() -> int:
    """Returns 0; kept for API compatibility."""
    return 0


# URLs where we should NOT use Firecrawl — use reviews instead
NON_WEBSITE_DOMAINS = (
    "facebook.com", "fb.com", "instagram.com", "linkedin.com",
    "yelp.com", "youtube.com", "tripadvisor.com",
    "google.com/maps", "maps.google.com", "goo.gl/maps",
    "bing.com/maps", "yellowpages.com", "angieslist.com",
    "homeadvisor.com", "nextdoor.com", "thumbtack.com",
)


def has_real_website(url: str) -> bool:
    """True if URL is a scrapable business site. False for listings/reviews."""
    if not url or len(url) < 10:
        return False
    return not any(d in url.lower() for d in NON_WEBSITE_DOMAINS)


def _do_tavily_search(query: str, max_results: int) -> List[Dict]:
    """Inner Tavily search. Raises on API errors (e.g. invalid key)."""
    from tavily import TavilyClient
    client = TavilyClient(api_key=TAVILY_API_KEY)
    response = client.search(query, max_results=max_results, search_depth=TAVILY_SEARCH_DEPTH)
    results = []
    if isinstance(response, dict):
        results = response.get("results", [])
        if not results and response.get("error"):
            raise ValueError(f"Tavily API error: {response.get('error')}")
    elif hasattr(response, "results"):
        results = getattr(response, "results", None) or []
    return results if isinstance(results, list) else []


def find_local_competitors(
    business_type: str,
    city: str,
    max_results: int = None,
) -> List[Dict]:
    """
    Tavily search for local competitors.
    Returns list of {name, url, content} dicts.
    Raises on API errors so the user sees the actual Tavily/API key message.
    """
    max_results = min(max_results or TAVILY_MAX_RESULTS, TAVILY_MAX_RESULTS)
    query = f"{business_type} {city}"
    if not TAVILY_API_KEY or not TAVILY_API_KEY.strip():
        raise ValueError(
            "TAVILY_API_KEY is not set. Add it in .env (local) or Streamlit Cloud: Manage app → Settings → Secrets."
        )
    results = _do_tavily_search(query, max_results)
    competitors = []
    seen = set()
    for r in results:
        if not isinstance(r, dict):
            continue
        name = (r.get("title") or r.get("name") or "").strip()
        url = (r.get("url") or r.get("link") or "").strip()
        content = (r.get("content") or r.get("snippet") or "").strip()
        if name and name not in seen:
            seen.add(name)
            competitors.append({"name": name, "url": url, "content": content})
    return competitors


def tavily_search(query: str, max_results: int = 10) -> List[Dict]:
    """
    Generic Tavily search. Returns list of {title, url, content} dicts.
    """
    max_results = min(max_results, 10)
    try:
        results = _do_tavily_search(query, max_results)
        out = []
        seen_urls = set()
        for r in results:
            if not isinstance(r, dict):
                continue
            url = (r.get("url") or r.get("link") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            out.append({
                "title": (r.get("title") or r.get("name") or "").strip(),
                "url": url,
                "content": (r.get("content") or r.get("snippet") or "").strip(),
            })
        return out
    except Exception as e:
        log.warning(f"Tavily search failed: {e}")
        return []


def get_services_from_reviews(competitor_name: str, city: str, niche: str = None) -> str:
    """
    Tavily search for reviews when competitor has no scrapable website.
    Returns raw text from review snippets.
    """
    niche = niche or "Junk Removal"
    query = f'"{competitor_name}" {niche} reviews'
    try:
        results = _do_tavily_search(query, TAVILY_REVIEWS_MAX_RESULTS)
        parts = []
        for r in results:
            c = r.get("content", r.get("snippet", ""))
            if c:
                parts.append(str(c))
        return " ".join(parts).strip() if parts else ""
    except Exception as e:
        log.warning(f"Tavily get_services_from_reviews failed: {e}")
        return ""
