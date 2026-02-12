"""
Firecrawl client — scrape only. Never used for Facebook, Google Maps, Yelp, etc.

Guards: onlyMainContent=True, timeout=30s, skip social/maps URLs.
"""

import requests

from config import FIRECRAWL_API_KEY, FIRECRAWL_TIMEOUT

FIRECRAWL_BASE_URL = "https://api.firecrawl.dev/v1/scrape"

HEADERS = {
    "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
    "Content-Type": "application/json",
}

# Block bad URLs early — Firecrawl should never touch listings/reviews
BLOCKED_DOMAINS = [
    "facebook.com", "fb.com", "instagram.com", "yelp.com",
    "google.com/maps", "maps.google.com", "goo.gl/maps",
    "linkedin.com", "youtube.com", "tripadvisor.com",
    "bing.com/maps", "yellowpages.com", "angieslist.com",
    "homeadvisor.com", "nextdoor.com", "thumbtack.com",
]


def is_supported_url(url: str) -> bool:
    """True if URL is a scrapable business site. False for listings/reviews."""
    if not url or len(url) < 10:
        return False
    return not any(domain in url.lower() for domain in BLOCKED_DOMAINS)


def firecrawl_scrape(url: str, timeout: int = None) -> dict:
    """
    Scrapes a single business website using Firecrawl.
    Returns dict with:
      - success (bool)
      - content (markdown text or error reason)
      - source (firecrawl)
    """
    if not url or not is_supported_url(url):
        return {
            "success": False,
            "content": "Unsupported or missing website URL",
            "source": "firecrawl",
        }

    payload = {
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": True,  # Guard: reduce payload, no full HTML
    }

    try:
        response = requests.post(
            FIRECRAWL_BASE_URL,
            headers=HEADERS,
            json=payload,
            timeout=timeout or FIRECRAWL_TIMEOUT,
        )

        if response.status_code != 200:
            return {
                "success": False,
                "content": f"Firecrawl error: {response.status_code}",
                "source": "firecrawl",
            }

        data = response.json()
        markdown = data.get("data", {}).get("markdown", "").strip()

        if not markdown:
            return {
                "success": False,
                "content": "Empty scrape result",
                "source": "firecrawl",
            }

        return {
            "success": True,
            "content": markdown,
            "source": "firecrawl",
        }

    except requests.exceptions.Timeout:
        return {
            "success": False,
            "content": "Firecrawl timeout",
            "source": "firecrawl",
        }
    except Exception as e:
        return {
            "success": False,
            "content": f"Firecrawl exception: {str(e)}",
            "source": "firecrawl",
        }
