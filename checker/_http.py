"""
Layer-1: HTTP-first Twisted X detection (no browser).

http_first_check(url) → dict

Attempts a plain HTTP GET (Chrome UA) before launching Playwright.
Only returns definitive=True on an SKU match — brand name alone is not
sufficient due to false-positive risk (press mentions, 'brands we carry' pages).
"""
from __future__ import annotations

import logging
import os

import requests

from ._scanners import scan_html_for_skus

log = logging.getLogger(__name__)

# Shared HTTP headers used by Layer-1 and Layer-2
HTTP_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
HTTP_HEADERS = {"User-Agent": HTTP_UA}

GOOGLE_CSE_API_KEY = os.environ.get("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_CX      = os.environ.get("GOOGLE_CSE_CX", "")

_BLOCK_SIGNALS = [
    "checking your browser", "cloudflare", "verify you are human",
    "ddos-guard", "access denied",
]

_FAIL: dict = {
    "success": False, "definitive": False, "proof": [], "blocked": False,
    "sells_twisted_x": False, "sells_footwear": None, "confidence": "low",
    "sample_products": [], "page_url": None, "error": None, "blocked_reasons": None,
}


def http_first_check(url: str) -> dict:
    """
    Attempt a plain HTTP GET before launching Playwright.
    Returns definitive=True only on an SKU fingerprint match.
    Never raises.
    """
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS, allow_redirects=True)

        if resp.status_code >= 400:
            return dict(_FAIL)
        if "text/html" not in resp.headers.get("content-type", ""):
            return dict(_FAIL)

        html = resp.text
        if len(html) < 2000:
            return dict(_FAIL)  # SPA shell — no real content yet

        if any(s in html.lower() for s in _BLOCK_SIGNALS):
            return {**_FAIL, "blocked": True}

        scan = scan_html_for_skus(html)
        if scan["matched_codes"]:
            proof = [f"{c} found in page HTML" for c in sorted(scan["matched_codes"])[:3]]
            return {
                "success": True, "definitive": True, "sells_twisted_x": True,
                "confidence": "high", "proof": proof,
                "sample_products": scan["sample_products"], "page_url": resp.url,
                "blocked": False, "error": None, "sells_footwear": None, "blocked_reasons": None,
            }

        return {
            "success": True, "definitive": False, "proof": [], "blocked": False,
            "sells_twisted_x": False, "sells_footwear": None, "confidence": "low",
            "sample_products": [], "page_url": None, "error": None, "blocked_reasons": None,
        }

    except Exception as exc:
        log.warning("http_first_check error for %s: %s", url, exc)
        return dict(_FAIL)
