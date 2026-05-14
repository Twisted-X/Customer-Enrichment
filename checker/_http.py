"""
Layer-1: HTTP-first Twisted X detection (no browser).

http_first_check(url) → dict

Probes a set of known TX brand URL paths (e.g. /collections/twisted-x,
/brands/twisted-x) via plain HTTP GET and scans each for TX SKUs.
These pages, when present, are dense with TX product HTML including SKUs.

Only returns definitive=True on an SKU match — brand name alone is not
sufficient due to false-positive risk (press mentions, 'brands we carry' pages).
"""
from __future__ import annotations

import logging
from urllib.parse import urlparse as _urlparse

from ._http_client import http_get
from ._scanners import scan_html_for_skus

log = logging.getLogger(__name__)

_BLOCK_SIGNALS = [
    "checking your browser", "cloudflare", "verify you are human",
    "ddos-guard", "access denied",
]

# Known TX brand/collection URL paths, ordered by signal strength.
# Explicit brand slugs come first (highest precision); generic search last.
_TX_BRAND_PATHS = [
    "/brands/twisted-x/",
    "/brands/twisted-x",
    "/collections/twisted-x",
    "/product-category/twisted-x/",
    "/twisted-x",
    "/c/twisted-x",
    "/t/twisted-x",
    "/shop/twisted-x",
    "/catalog/category/view/id/twisted-x",
    "/brands/twisted-x-boots",
    "/collections/twisted-x-boots",
    "/search?type=product&q=Twisted+X",
    "/search?type=product&q=twistedx",
    "/search?q=Twisted+X",
    "/search?q=twistedx",
    "/search-page?query=twistedx",
    "/search-page?query=Twisted+X",
    "/?s=Twisted+X&post_type=product",
    "/?s=twistedx&post_type=product",
    "/catalog/productsearch?keywords=Twisted+X",
]

_FAIL: dict = {
    "success": False, "definitive": False, "proof": [], "blocked": False,
    "sells_twisted_x": False, "sells_footwear": None, "confidence": "low",
    "sample_products": [], "page_url": None, "error": None, "blocked_reasons": None,
}


def http_first_check(url: str) -> dict:
    """
    Probe known TX brand/collection pages via plain HTTP.
    Returns definitive=True only on an SKU fingerprint match.
    Never raises.
    """
    try:
        parsed = _urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        for path in _TX_BRAND_PATHS:
            brand_url = origin + path
            result = _scan_url(brand_url, timeout=5)
            if result is not None:
                log.info("Layer 1 brand-page hit: %s", brand_url)
                return result

        return {
            "success": True, "definitive": False, "proof": [], "blocked": False,
            "sells_twisted_x": False, "sells_footwear": None, "confidence": "low",
            "sample_products": [], "page_url": None, "error": None, "blocked_reasons": None,
        }

    except Exception as exc:
        log.warning("http_first_check error for %s: %s", url, exc)
        return dict(_FAIL)


def _scan_url(url: str, timeout: int = 3) -> dict | None:
    """
    Fetch `url` and scan for TX SKUs.
    Returns a definitive result dict on a hit, None when no hit (or unreachable).
    """
    try:
        resp = http_get(url, timeout=timeout)

        if resp is None or resp.status_code >= 400:
            return None
        if "text/html" not in resp.headers.get("content-type", ""):
            return None

        html = resp.text
        if len(html) < 500:
            return None  # Empty / SPA shell

        if any(s in html.lower() for s in _BLOCK_SIGNALS):
            return None  # Blocked — don't report definitive, let Playwright handle it

        scan = scan_html_for_skus(html)
        if scan["matched_codes"]:
            proof = [f"{c} found in page HTML" for c in sorted(scan["matched_codes"])[:3]]
            return {
                "success": True, "definitive": True, "sells_twisted_x": True,
                "confidence": "high", "proof": proof,
                "sample_products": scan["sample_products"], "page_url": resp.url,
                "blocked": False, "error": None, "sells_footwear": None, "blocked_reasons": None,
            }
    except Exception as exc:
        log.debug("_scan_url error for %s: %s", url, exc)

    return None
