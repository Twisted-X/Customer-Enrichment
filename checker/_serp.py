"""
Layer 3: SerpApi Google Search check for Twisted X products.

serp_check(url) -> dict

Searches Google for "Twisted X site:<domain>" using SerpApi.
If Google has indexed Twisted X products on the domain, returns definitive YES
without launching a browser — bypasses Cloudflare / PerimeterX bot protection.

Important: a no-results response is NOT a definitive NO. Google may simply not
have indexed the retailer's product pages. Falls through to Playwright in that
case.

Requires SERPAPI_KEY in environment / .env.
If the key is missing, returns definitive=False immediately (disabled).
"""
from __future__ import annotations

import logging
import re
import time
from threading import Lock
from urllib.parse import urlparse

import requests

from config import SERPAPI_KEY

log = logging.getLogger(__name__)

_SERP_URL     = "https://serpapi.com/search"
_SERP_TIMEOUT = 8  # seconds

# In-memory TTL cache keyed by domain.
# Avoids duplicate SerpApi calls for the same domain within a session or batch.
_CACHE_TTL_S  = 3600  # 1 hour
_cache: dict  = {}    # {domain: (result, expiry_ts)}
_cache_lock   = Lock()


def _extract_domain(url: str) -> str:
    """Return bare domain without www. prefix."""
    try:
        host = urlparse(url).hostname or ""
        return host.lower().removeprefix("www.")
    except Exception:
        return ""


def serp_check(url: str) -> dict:
    """
    Search Google via SerpApi for "Twisted X site:<domain>".

    Returns a result dict with:
        definitive      — True only when results found AND product-page signals present
        sells_twisted_x — True if definitive, False otherwise
        confidence      — "high" when definitive, "low" otherwise
        proof           — list of evidence strings (titles, links, snippets)
        serp_results    — number of Google results found
        sells_online    — True if ecom URL paths, snippet keywords, or price found

    Both conditions required for definitive=True:
        1. Product-page signals (ecom URL paths, snippet keywords, or $price)
        2. Footwear signals — boot/shoe/moc/western/cowboy/footwear in title,
           snippet, or link. Guards against a different brand also named
           "Twisted X" (e.g. wheel rims, workwear tools on homedepot.com).

    Falls through to Playwright when either condition is missing.

    Never raises. If SerpApi is disabled or errors, returns definitive=False
    so the caller falls through to Playwright.
    """
    _SKIP = {
        "definitive": False, "sells_twisted_x": False,
        "confidence": "low", "proof": [], "serp_results": 0,
    }

    if not SERPAPI_KEY:
        log.debug("SerpApi disabled — SERPAPI_KEY not set")
        return _SKIP

    domain = _extract_domain(url)
    if not domain:
        return _SKIP

    # Cache check — skip API call if we already queried this domain recently
    with _cache_lock:
        cached = _cache.get(domain)
        if cached:
            result, expiry = cached
            if time.time() < expiry:
                log.info("SerpApi cache hit for %s", domain)
                return result
            del _cache[domain]

    query = f'Twisted X site:{domain}'

    try:
        resp = requests.get(
            _SERP_URL,
            params={
                "api_key": SERPAPI_KEY,
                "engine":  "google",
                "q":       query,
                "num":     5,
            },
            timeout=_SERP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        # SerpApi returns errors in the JSON body with a 200 status
        if "error" in data:
            log.warning("SerpApi error for %s: %s — falling through to Playwright", domain, data["error"])
            return _SKIP

        items = data.get("organic_results", [])
        total = data.get("search_information", {}).get("total_results", 0)
        if isinstance(total, str):
            total = int(total.replace(",", "")) if total.replace(",", "").isdigit() else 0

        log.info("SerpApi: '%s' → %d results (total ~%d)", query, len(items), total)

        if not items:
            return {**_SKIP, "proof": [f"SerpApi: no Google results for '{query}'"]}

        # Filter out internal site-search pages — they prove users searched for
        # TX on the site, not that the site actually sells TX products.
        _SEARCH_PATH = re.compile(r'/s/|/search[/?]|[?&](q|query|search)=', re.I)
        product_items = [i for i in items if not _SEARCH_PATH.search(i.get("link", ""))]
        if not product_items:
            return {**_SKIP, "proof": [f"SerpApi: all {len(items)} result(s) are internal search pages — not definitive"]}

        proof = [f"SerpApi: {total} Google result(s) for '{query}'"]
        for item in product_items[:3]:
            title   = item.get("title", "")[:80]
            link    = item.get("link", "")
            snippet = item.get("snippet", "")[:120]
            proof.append(f"  Google result: {title} — {link}")
            if snippet:
                proof.append(f"    snippet: {snippet}")

        _ECOM_PATH_SIGNALS = (
            "/product", "/shop", "/cart", "/buy",
            "/collections", "/catalog", "/p/", "/item",
            "/checkout", "/store",
        )
        _SNIPPET_SIGNALS = (
            "add to cart", "buy now", "in stock", "free shipping",
            "free returns", "ships free", "order now", "checkout",
        )

        url_hit = any(
            any(seg in item.get("link", "").lower() for seg in _ECOM_PATH_SIGNALS)
            for item in product_items
        )
        # TX slug in the URL path is itself an ecom signal (Google indexed a
        # product/collection page for TX on that domain)
        tx_slug_hit = any(
            any(slug in item.get("link", "").lower()
                for slug in ("twisted-x", "twistedx", "twisted_x"))
            for item in product_items
        )
        snippet_hit = any(
            any(sig in (item.get("snippet", "") + item.get("title", "")).lower()
                for sig in _SNIPPET_SIGNALS)
            for item in product_items
        )
        # Also treat a bare price pattern as a snippet signal ($XX or $X,XXX)
        price_hit = any(
            bool(re.search(r'\$\d', item.get("snippet", "") + item.get("title", "")))
            for item in product_items
        )

        sells_online = url_hit or tx_slug_hit or snippet_hit or price_hit

        # Guard against a different brand also named "Twisted X" (e.g. wheels,
        # workwear tools on homedepot.com). Require at least one result to mention
        # footwear-related terms — if every result is clearly non-footwear, fall
        # through to Playwright rather than committing a false positive.
        _FOOTWEAR_SIGNALS = (
            "boot", "shoe", "footwear", "moc", "loafer", "sneaker",
            "western", "cowboy", "work boot", "driving moc",
        )
        # Only check title + snippet — not URL — because search query URLs like
        # /s/twisted+x+boots+hooey contain footwear words in the query string
        # without the site actually selling TX footwear.
        footwear_hit = any(
            any(sig in (item.get("snippet", "") + item.get("title", "")).lower()
                for sig in _FOOTWEAR_SIGNALS)
            for item in product_items
        )

        definitive = sells_online and footwear_hit

        if not sells_online:
            proof.append("  SerpApi: results found but no product-page signals — falling through to Playwright")
            log.info("SerpApi: results found for %s but no ecom signals — not definitive", domain)
        elif not footwear_hit:
            proof.append("  SerpApi: results found but no footwear signals — likely different brand, falling through to Playwright")
            log.info("SerpApi: results found for %s but no footwear signals — not definitive", domain)

        result = {
            "definitive":      definitive,
            "sells_twisted_x": True if definitive else False,
            "confidence":      "high" if definitive else "low",
            "proof":           proof,
            "serp_results":    total,
            "sells_online":    sells_online,
        }
        with _cache_lock:
            _cache[domain] = (result, time.time() + _CACHE_TTL_S)
        log.info("SerpApi cache stored for %s (TTL %ds)", domain, _CACHE_TTL_S)
        return result

    except requests.HTTPError as exc:
        status = getattr(exc.response, "status_code", 0)
        if status == 429:
            log.warning("SerpApi quota exceeded for %s — falling through to Playwright", domain)
        else:
            log.warning("SerpApi HTTP error %d for %s — falling through", status, domain)
        return _SKIP

    except Exception as exc:
        log.warning("SerpApi error for %s: %s — falling through to Playwright", domain, exc)
        return _SKIP
