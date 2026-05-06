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

import requests

from ._http import HTTP_HEADERS

log = logging.getLogger(__name__)

_TX_RE = re.compile(r'twisted[_\-]?x|twistedx', re.IGNORECASE)

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
        r = requests.get(f"{base_url}/robots.txt", timeout=5, headers=HTTP_HEADERS)
        if r.status_code == 200:
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
    """Fetch one sitemap, scan page URLs, recurse into child sitemaps (≤3)."""
    try:
        r = requests.get(candidate, timeout=8, headers=HTTP_HEADERS)
        if r.status_code != 200:
            return None, n_checked, any_fetched

        any_fetched = True
        page_locs, child_locs = _parse_sitemap(r.content, candidate.endswith(".gz"))

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


def _scan_children(child_locs: list, n_checked: int):
    """Fetch up to 3 child sitemaps and scan for TX slugs. Cap at 2000 total URLs."""
    for i, child_url in enumerate(child_locs):
        if n_checked >= 2000 or i >= 3:
            break
        try:
            rc = requests.get(child_url, timeout=8, headers=HTTP_HEADERS)
            if rc.status_code != 200:
                continue
            child_page_locs, _ = _parse_sitemap(rc.content, child_url.endswith(".gz"))
            for loc in child_page_locs:
                n_checked += 1
                if _TX_RE.search(loc):
                    return _tx_found(loc), n_checked
                if n_checked >= 2000:
                    break
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
        raw  = gzip.decompress(content) if is_gz else content
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
