"""
Platform-aware search strategies used inside Layer-3 (Playwright check).

Each strategy navigates to the best search page for its platform and
runs SKU + brand scanning. Returns a SearchOutcome.

  search_netsuite(page, base_url)                      → SearchOutcome
  search_shopify_or_woo(page, platform, base_url, url) → SearchOutcome
  search_generic(page, base_url, url)                  → SearchOutcome

All three are called exclusively from _playwright.py. They never open a
new browser — they receive the already-open Playwright page and navigate it.
"""
from __future__ import annotations

import logging
from typing import List
from urllib.parse import urlparse as _urlparse

from ._types import SearchOutcome, empty_search
from ._scanners import scan_page_for_skus, find_brand_in_product_context

log = logging.getLogger(__name__)

# Generic search URL patterns tried when platform-specific strategies fail
_GENERIC_SEARCH_URLS = [
    "/search?q=Twisted+X",
    "/search?type=product&q=Twisted+X",
    "/searchPage.action?keyWord=Twisted+X",
    "/catalog/productsearch",
    "/?s=Twisted+X&post_type=product",
    "/?s=Twisted+X",
]

# "No results" signals — if page text contains any of these, search was empty
_NO_RESULTS_PHRASES = [
    'no results', 'no products found', 'nothing found',
    '0 results', 'no items found',
]


def search_netsuite(page, base_url: str) -> SearchOutcome:
    """
    NetSuite / SuiteCommerce: navigate directly to /catalog/productsearch.

    NetSuite renders its product catalog via this endpoint. We scroll the page
    three times to trigger lazy-loaded product records before scanning.
    """
    search_url = f"{base_url}/catalog/productsearch"
    log.info("NetSuite catalog URL: %s", search_url)

    try:
        page.goto(search_url, timeout=20_000, wait_until='domcontentloaded')
        page.wait_for_timeout(3_500)
        _scroll_to_load(page)
    except Exception as exc:
        log.warning("NetSuite catalog navigation failed: %s", exc)
        return empty_search()

    return _scan_current_page(page)


def search_shopify_or_woo(page, platform: str, base_url: str, normalized_url: str) -> SearchOutcome:
    """
    Shopify / WooCommerce: try platform-specific URL patterns first, then
    fall back to Playwright search-bar interaction if URL patterns find nothing.
    """
    search_urls = _search_urls_for_platform(page, platform, base_url)
    log.info("%s: trying %d search URL pattern(s)", platform, len(search_urls))

    for url in search_urls:
        outcome = _try_url(page, url, platform)
        if outcome["found_match"]:
            return outcome

    # URL patterns found nothing — try Playwright search bar as fallback
    log.info("%s URL patterns exhausted, falling back to search-bar interaction", platform)
    return _search_via_ui(page, normalized_url)


def search_generic(page, base_url: str, normalized_url: str) -> SearchOutcome:
    """
    Normal / unknown platform: try Playwright search-bar interaction first, then
    fall back to generic URL patterns if the search bar isn't found or returns nothing.
    """
    log.info("Generic site: trying Playwright search-bar interaction")
    outcome = _search_via_ui(page, normalized_url)
    if outcome["found_match"]:
        return outcome

    log.info("Search-bar interaction found nothing, trying generic URL patterns")
    for path in _GENERIC_SEARCH_URLS:
        outcome = _try_url(page, f"{base_url}{path}", "generic")
        if outcome["found_match"]:
            return outcome

    return empty_search()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _scroll_to_load(page, rounds: int = 3) -> None:
    """Scroll to the bottom of the page `rounds` times to trigger lazy loading."""
    for _ in range(rounds):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(800)
        except Exception:
            break


def _page_has_content(page) -> bool:
    """Return True if the page has meaningful text and no explicit 'no results' message."""
    try:
        text = page.inner_text('body').lower()
        if len(text) < 200:
            return False
        return not any(phrase in text for phrase in _NO_RESULTS_PHRASES)
    except Exception:
        return False


def _scan_current_page(page) -> SearchOutcome:
    """Run SKU scan + brand scan on the current page and package as SearchOutcome."""
    sku_scan = scan_page_for_skus(page)
    if sku_scan["matched_codes"]:
        return {
            "found_match": True,
            "sku_scan": sku_scan,
            "brand_found": False,
            "brand_samples": [],
            "page_url": page.url,
        }

    brand_found, brand_samples = find_brand_in_product_context(page)
    return {
        "found_match": brand_found,
        "sku_scan": sku_scan,
        "brand_found": brand_found,
        "brand_samples": brand_samples,
        "page_url": page.url if brand_found else None,
    }


def _try_url(page, url: str, label: str) -> SearchOutcome:
    """Navigate to `url`, wait for content, scan. Returns empty_search() on failure."""
    log.info("%s: trying %s", label, url)
    try:
        page.goto(url, timeout=20_000, wait_until='domcontentloaded')
        # Smart wait: poll until content renders (max ~8s)
        for _ in range(4):
            page.wait_for_timeout(2_000)
            if _page_has_content(page):
                break

        if not _page_has_content(page):
            return empty_search()

        return _scan_current_page(page)
    except Exception as exc:
        log.debug("URL attempt failed (%s): %s", url, exc)
        return empty_search()


def _search_via_ui(page, normalized_url: str) -> SearchOutcome:
    """
    Use Playwright to type into the site's own search box and press Enter.
    Tries "Twisted X" then "TwistedX" if the first finds nothing.
    """
    import url_validator  # avoid circular import at module level

    for term in ['Twisted X', 'TwistedX']:
        try:
            page.goto(normalized_url, timeout=15_000, wait_until='domcontentloaded')
            page.wait_for_timeout(1_000)

            if not url_validator._search_on_site(page, term):
                continue

            page.wait_for_timeout(3_000)
            log.info("Search-bar interaction succeeded with '%s'", term)

            if _page_has_content(page):
                outcome = _scan_current_page(page)
                if outcome["found_match"]:
                    return outcome
        except Exception as exc:
            log.debug("UI search failed for term '%s': %s", term, exc)

    return empty_search()


def _search_urls_for_platform(page, platform: str, base_url: str) -> List[str]:
    """Return the ordered list of search URLs to try for a given platform."""
    if platform == 'shopify':
        # Try to detect the store's actual search URL from its navigation links
        try:
            detected = page.evaluate("""() => {
                const links = [...document.querySelectorAll('a[href*="search"]')];
                for (const l of links) {
                    if (l.href.includes('/search')) return l.href.split('?')[0];
                }
                return null;
            }""")
            if detected:
                return [
                    f"{detected}?type=product&q=Twisted+X",
                    f"{detected}?q=Twisted+X",
                ]
        except Exception:
            pass
        return [
            f"{base_url}/search?type=product&q=Twisted+X",
            f"{base_url}/search?q=Twisted+X",
        ]

    if platform == 'woocommerce':
        return [
            f"{base_url}/?s=Twisted+X&post_type=product",
            f"{base_url}/?s=Twisted+X",
        ]

    return []
