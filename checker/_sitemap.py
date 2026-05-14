"""
Layer-2: Sitemap-based Twisted X detection.

sitemap_check(url) → dict

Fetches and parses sitemaps looking for TX product URL slugs.
Checks robots.txt for Sitemap: directives, then falls back to
/sitemap.xml and /sitemap_index.xml.

Only returns definitive=True on a positive slug match — absence is NOT a
definitive negative (products may simply not be listed in the sitemap).
"""
from __future__ import annotations

import gzip
import logging
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlparse as _urlparse

from ._http_client import http_get

log = logging.getLogger(__name__)

# Matches twisted-x, twisted_x, twistedx, tx-footwear, tx-boots, tx-work
_TX_RE = re.compile(
    r'twisted[_\-]?x|twistedx|\btx[_\-](footwear|boots?|work|western|casual|kids?)',
    re.IGNORECASE,
)

# Child sitemaps whose names suggest product/brand content — fetched first
# Note: 'item' omitted — it matches inside 'sitemap' on every URL
_PRIORITY_RE = re.compile(
    r'product|brand|categor|collection|catalog',
    re.IGNORECASE,
)

# Child sitemaps that are definitely irrelevant — skipped entirely
_SKIP_RE = re.compile(
    r'blog|news|post|article|press|recipe|video|image|media|sitemap-misc',
    re.IGNORECASE,
)

# Max child sitemaps to fetch (HTTP requests are the bottleneck, not URL scanning)
_MAX_CHILD_FETCHES = 10

_FAIL: dict = {
    "success": False, "definitive": False, "proof": [], "blocked": False,
    "sells_twisted_x": False, "sells_footwear": None, "confidence": "low",
    "sample_products": [], "page_url": None, "error": None, "blocked_reasons": None,
}


def sitemap_check(url: str) -> dict:
    """
    Fetch and parse sitemaps looking for TX product URL slugs.
    Never raises.
    """
    try:
        parsed   = _urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        candidates  = _collect_candidates(base_url)
        n_checked   = 0
        any_fetched = False

        for candidate in candidates:
            hit, n_checked, any_fetched = _process_candidate(candidate, n_checked, any_fetched)
            if hit is not None:
                return hit

        if any_fetched:
            return {
                "success": True, "definitive": False,
                "proof": [f"Sitemap: {n_checked} URLs checked, no TX slugs found"],
                "blocked": False, "sells_twisted_x": False, "sells_footwear": None,
                "confidence": "low", "sample_products": [], "page_url": None,
                "error": None, "blocked_reasons": None,
            }
        return dict(_FAIL)

    except Exception as exc:
        log.warning("sitemap_check error for %s: %s", url, exc)
        return dict(_FAIL)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _collect_candidates(base_url: str) -> list:
    """Read robots.txt for Sitemap: directives; append standard fallbacks."""
    candidates: list = []
    try:
        r = http_get(f"{base_url}/robots.txt", timeout=5)
        if r is not None and r.status_code == 200:
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sm_url = line.split(":", 1)[1].strip()
                    if sm_url:
                        candidates.append(sm_url)
    except Exception:
        pass

    for default in [f"{base_url}/sitemap.xml", f"{base_url}/sitemap_index.xml"]:
        if default not in candidates:
            candidates.append(default)
    return candidates


def _process_candidate(candidate: str, n_checked: int, any_fetched: bool):
    """Fetch one sitemap, scan all page URLs, recurse into child sitemaps."""
    try:
        r = http_get(candidate, timeout=8)
        if r is None or r.status_code != 200:
            return None, n_checked, any_fetched

        any_fetched = True
        # Detect gzip from magic bytes — curl_cffi auto-decompresses but
        # may still report Content-Encoding: gzip in headers, so the header
        # is unreliable. Magic bytes are always accurate.
        is_gz = r.content[:2] == b'\x1f\x8b'
        page_locs, child_locs = _parse_sitemap(r.content, is_gz)

        for loc in page_locs:
            n_checked += 1
            if _TX_RE.search(loc):
                return _tx_found(loc), n_checked, any_fetched

        hit, n_checked = _scan_children(child_locs, n_checked)
        if hit:
            return hit, n_checked, any_fetched

    except Exception:
        pass

    return None, n_checked, any_fetched


def _prioritise_children(child_locs: list) -> list:
    """
    Sort child sitemaps so product/brand/category ones come first.
    Skip blog/news/media sitemaps entirely — they never contain TX products.
    """
    keep = [u for u in child_locs if not _SKIP_RE.search(u)]
    priority = [u for u in keep if _PRIORITY_RE.search(u)]
    rest     = [u for u in keep if not _PRIORITY_RE.search(u)]
    return priority + rest


def _scan_children(child_locs: list, n_checked: int):
    """
    Fetch up to _MAX_CHILD_FETCHES child sitemaps in priority order.
    No URL count cap — string matching is free; HTTP fetches are the bottleneck.
    """
    ordered = _prioritise_children(child_locs)
    fetches = 0

    for child_url in ordered:
        if fetches >= _MAX_CHILD_FETCHES:
            break
        try:
            rc = http_get(child_url, timeout=8)
            fetches += 1
            if rc is None or rc.status_code != 200:
                continue
            is_gz = rc.content[:2] == b'\x1f\x8b'
            child_page_locs, _ = _parse_sitemap(rc.content, is_gz)
            for loc in child_page_locs:
                n_checked += 1
                if _TX_RE.search(loc):
                    return _tx_found(loc), n_checked
        except Exception:
            continue

    return None, n_checked


def _tx_found(loc: str) -> dict:
    return {
        "success": True, "definitive": True, "sells_twisted_x": True,
        "confidence": "high",
        "proof": [f"Sitemap: TX product URL found: {loc[:100]}"],
        "page_url": loc, "blocked": False, "error": None,
        "sells_footwear": None, "blocked_reasons": None, "sample_products": [],
    }


def _parse_sitemap(content: bytes, is_gz: bool) -> tuple:
    """Decompress if needed, parse sitemap XML. Returns (page_locs, child_locs)."""
    try:
        raw = gzip.decompress(content) if is_gz else content
        root = ET.fromstring(raw)
    except Exception:
        return ([], [])

    NS = "http://www.sitemaps.org/schemas/sitemap/0.9"

    def _locs(tag_ns, tag_plain):
        els = root.findall(tag_ns) or root.findall(tag_plain)
        return [e.text.strip() for e in els if e.text and e.text.strip()]

    page_locs  = _locs(f".//{{{NS}}}url/{{{NS}}}loc",     ".//url/loc")
    child_locs = _locs(f".//{{{NS}}}sitemap/{{{NS}}}loc", ".//sitemap/loc")
    return (page_locs, child_locs)
