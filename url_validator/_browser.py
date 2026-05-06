"""
Playwright browser-interaction helpers.

Functions
---------
_close_popups(page)                      -> None
_try_fill_search_input(page, term)       -> bool
_search_on_site(page, term)              -> bool
_try_category_pages(page, base_url)      -> bool
"""
import logging
from urllib.parse import urlparse

from playwright.sync_api import Page

from ._constants import _POPUP_CLOSE_SELECTORS, _SEARCH_INPUT_SELECTORS, _SEARCH_ICON_SELECTORS
from ._brand import _check_brand_in_content, _is_netsuite_site

log = logging.getLogger(__name__)


def _close_popups(page: Page) -> None:
    """Dismiss any popup overlays that might block interactions."""
    for selector in _POPUP_CLOSE_SELECTORS:
        try:
            btn = page.query_selector(selector)
            if btn and btn.is_visible():
                btn.click(timeout=2000)
                page.wait_for_timeout(500)
        except Exception as e:
            log.debug("Popup close failed for selector %r: %s", selector, e)

    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(500)
    except Exception as e:
        log.debug("Escape key press failed: %s", e)


def _try_fill_search_input(page: Page, search_term: str) -> bool:
    """
    Attempt to find a visible search input and submit search_term via Enter.
    Tries each selector in _SEARCH_INPUT_SELECTORS in order.
    Returns True on the first successful submission.
    """
    for selector in _SEARCH_INPUT_SELECTORS:
        try:
            inp = page.query_selector(selector)
            if inp and inp.is_visible():
                try:
                    inp.scroll_into_view_if_needed()
                    page.wait_for_timeout(300)
                    try:
                        inp.click(timeout=3000)
                    except Exception:
                        inp.evaluate("el => el.focus()")
                    page.wait_for_timeout(300)
                    inp.fill(search_term)
                    page.wait_for_timeout(300)
                    inp.press("Enter")
                    page.wait_for_timeout(2000)
                    return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def _search_on_site(page: Page, search_term: str) -> bool:
    """
    Locate and use the site's search box to search for search_term.

    Strategy (tried in order):
    1. Fill a visible search input directly.
    2. Click a search icon/toggle to reveal a hidden input, then fill it.
    3. Navigate directly to common search URL patterns.

    Returns True if a search was submitted (not whether results were found).
    """
    _close_popups(page)

    if _try_fill_search_input(page, search_term):
        return True

    # Strategy 2: click a search icon/toggle to reveal the input
    for icon_sel in _SEARCH_ICON_SELECTORS:
        try:
            icon = page.query_selector(icon_sel)
            if icon and icon.is_visible():
                icon.click()
                page.wait_for_timeout(1000)
                if _try_fill_search_input(page, search_term):
                    return True
        except Exception:
            continue

    # Strategy 3: direct URL navigation.
    # Runs BEFORE JS form submit to prefer WooCommerce product search over
    # generic WordPress blog search, which can return non-product results.
    try:
        base = urlparse(page.url)
        base_url = f"{base.scheme}://{base.netloc}"
        encoded = search_term.replace(' ', '+')

        search_urls = [
            f"{base_url}/?s={encoded}&post_type=product",      # WooCommerce product search
            f"{base_url}/search?q={encoded}",                   # Shopify/generic
            f"{base_url}/search?type=product&q={encoded}",      # Shopify product-only
            f"{base_url}/searchPage.action?keyWord={encoded}",  # Java/Struts (e.g. stockdales.com)
            f"{base_url}/catalog/productsearch",                # NetSuite catalog
            f"{base_url}/?s={encoded}",                         # WordPress generic (last)
        ]

        # NetSuite/SuiteCommerce: try /catalog/productsearch first
        if _is_netsuite_site(page):
            catalog_url = f"{base_url}/catalog/productsearch"
            search_urls = [catalog_url] + [u for u in search_urls if u != catalog_url]

        for search_url in search_urls:
            try:
                page.goto(search_url, timeout=15000, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)
                landed = page.url.lower()
                looks_like_search = (
                    '?s=' in landed or 'search' in landed
                    or 'q=' in landed or 'productsearch' in landed
                )
                if looks_like_search:
                    try:
                        body_len = len(page.inner_text('body'))
                    except Exception:
                        body_len = 0
                    if body_len > 300:
                        return True
            except Exception:
                continue
    except Exception:
        pass

    return False


def _try_category_pages(page: Page, base_url: str) -> bool:
    """
    Navigate to /boots, /footwear, /shoes and check each for a TX brand mention.
    Returns True at the first page that matches.
    """
    for path in ['/boots', '/footwear', '/shoes']:
        try:
            page.goto(base_url.rstrip('/') + path, timeout=6000, wait_until='domcontentloaded')
            page.wait_for_timeout(2000)
            if _check_brand_in_content(page.inner_text('body').lower(), page.content().lower()):
                return True
        except Exception:
            continue
    return False
