"""
checker — Twisted X retailer detection package.

Public API:

    from checker import run_check

    result = run_check("https://www.atwoods.com/")
    # result["sells_twisted_x"] → True/False/None
    # result["confidence"]      → "high" | "medium" | "low"
    # result["sample_products"] → list of {name, sku, price, image, product_url}

Three detection layers run in order, short-circuiting on a definitive YES:

    Layer 1 — HTTP-first  (_http.py)    cheap GET, no browser
    Layer 2 — Sitemap     (_sitemap.py) parse robots.txt / sitemap.xml
    Layer 3 — Playwright  (_playwright.py) full browser + platform-aware search

Module map (one responsibility each):

    _types.py      TypedDict definitions + result factories
    _scanners.py   SKU fingerprint + brand-context DOM scanning
    _platform.py   Platform detection (Shopify / WooCommerce / NetSuite / normal)
    _search.py     Platform-aware search strategies
    _http.py       Layer-1: plain HTTP check (no browser)
    _sitemap.py    Layer-2: sitemap URL slug check
    _playwright.py Layer-3: browser orchestration + result assembly
"""
from __future__ import annotations
import logging
from urllib.parse import urlparse as _urlparse

from config import get_retailer_name
from url_validator import normalize_url
from ._types import new_check_result
from ._http import http_first_check
from ._sitemap import sitemap_check
from ._playwright import playwright_check

log = logging.getLogger(__name__)


def run_check(url: str) -> dict:
    """
    Entry point: determine whether a retailer URL sells Twisted X products.

    Runs three layers in order. Each layer can short-circuit with a definitive
    answer so we never launch a browser when a cheap HTTP check is sufficient.

    Args:
        url: Raw retailer URL (e.g. "https://www.atwoods.com/")

    Returns:
        dict matching the CheckResponse Pydantic schema:
          sells_twisted_x, confidence, proof, sample_products,
          sells_online, sells_footwear, store_type, blocked, error, ...
    """
    normalized = normalize_url(url)
    if not normalized:
        return new_check_result(url, "unknown", error="Invalid URL format")

    retailer_name = get_retailer_name(normalized)

    # ── Layer 1: HTTP-first — no browser, cheap GET ──
    log.info("Layer 1 (HTTP-first): %s", normalized)
    http_result = http_first_check(normalized)
    if http_result.get("definitive") and http_result.get("sells_twisted_x") is True:
        log.info("Layer 1 definitive YES — skipping Playwright")
        r = new_check_result(normalized, retailer_name)
        r.update({
            "sells_twisted_x": True,
            "confidence":      http_result["confidence"],
            "proof":           http_result["proof"],
            "sample_products": http_result.get("sample_products", []),
            "page_url":        http_result.get("page_url") or normalized,
            "sells_online":    True,
            "store_type":      "ecommerce",
            "blocked":         False,
            "error":           None,
        })
        return r

    # ── Layer 2: Sitemap — parse robots.txt / sitemap.xml ──
    log.info("Layer 2 (sitemap): %s", normalized)
    sitemap_result = sitemap_check(normalized)
    if sitemap_result.get("definitive") and sitemap_result.get("sells_twisted_x") is True:
        log.info("Layer 2 definitive YES — skipping Playwright")
        r = new_check_result(normalized, retailer_name)
        r.update({
            "sells_twisted_x": True,
            "confidence":      sitemap_result["confidence"],
            "proof":           sitemap_result["proof"],
            "page_url":        sitemap_result.get("page_url") or normalized,
            "sells_online":    True,
            "store_type":      "ecommerce",
            "blocked":         False,
            "error":           None,
        })
        return r

    # ── Layer 3: Playwright — full browser ──
    log.info("Layer 3 (Playwright): %s", normalized)
    result = playwright_check(url, normalized, retailer_name)

    # Merge sitemap context note if Layer 3 found nothing
    if sitemap_result.get("proof") and not result.get("sells_twisted_x"):
        result.setdefault("proof", []).append(sitemap_result["proof"][0])

    return result
