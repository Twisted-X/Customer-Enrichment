"""
URL normalisation, brand indicator detection, and site classification.

Functions
---------
normalize_url(url)                         -> str | None
_check_brand_in_content(text, html)        -> bool
_check_product_links(page)                 -> bool
_is_netsuite_site(page)                    -> bool
_classify_brand_site(final_url, text, url) -> (bool, bool)
"""
from typing import Optional
from urllib.parse import urlparse

from patchright.sync_api import Page
from brand_config import ALL_INDICATORS as _BRAND_INDICATORS

from ._constants import _GENERIC_BRAND_WORDS, _PRODUCT_TITLE_SELECTORS


def normalize_url(url: str) -> Optional[str]:
    """
    Normalize and validate a raw URL string.

    Fixes common malformed patterns (doubled protocols, missing www, etc.)
    and adds https:// when the protocol is absent.

    Returns the normalized URL, or None if it cannot be made valid.
    """
    if not url or url.strip() in ['- None -', 'None', '', 'N/A', 'n/a']:
        return None

    url = url.strip()

    # Fix common malformed URLs before parsing
    if url.startswith('http://http:/'):
        url = url.replace('http://http:/', 'http://')
    if url.startswith('http://ww.'):
        url = url.replace('http://ww.', 'http://www.')
    if url.startswith('https://https://'):
        url = url.replace('https://https://', 'https://')

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    try:
        parsed = urlparse(url)
        if not parsed.netloc or '.' not in parsed.netloc:
            return None
        return url
    except Exception:
        return None


def _check_brand_in_content(text: str, html: str = '') -> bool:
    """
    Return True if any TX brand indicator is found in text or HTML.

    Brand indicators are loaded from config/brand_indicators.json via
    brand_config.py, so adding a new brand propagates here automatically.
    Generic words (e.g. "hooey") only count when at least one non-generic
    indicator is also present.
    """
    content = (text + ' ' + html) if html else text

    specific_hit = any(
        ind in content
        for ind in _BRAND_INDICATORS
        if ind not in _GENERIC_BRAND_WORDS
    )
    if specific_hit:
        return True

    return False


def _check_product_links(page: Page) -> bool:
    """Return True if any product link or heading on the page mentions a TX brand."""
    try:
        links = page.query_selector_all(
            'a[href*="product"], a[href*="item"], a[href*="boot"], '
            'a[href*="shoe"], a[href*="p-"], a[href*="/p/"]'
        )
        for link in links[:100]:
            try:
                href = link.get_attribute('href') or ''
                text = (link.inner_text() or '').lower()
                try:
                    parent_text = link.evaluate(
                        'el => (el.closest("div, article, section") || el).innerText || ""'
                    ).lower()
                    combined = text + ' ' + parent_text + ' ' + href.lower()
                except Exception:
                    combined = text + ' ' + href.lower()
                if _check_brand_in_content(combined):
                    return True
            except Exception:
                continue

        for selector in _PRODUCT_TITLE_SELECTORS:
            for el in page.query_selector_all(selector)[:50]:
                try:
                    if _check_brand_in_content((el.inner_text() or '').lower()):
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def _is_netsuite_site(page: Page) -> bool:
    """
    Return True if the page appears to be a NetSuite/SuiteCommerce storefront.
    Used by _search_on_site to prioritise /catalog/productsearch over generic
    WordPress search URLs, which can false-positive on blog content.
    """
    try:
        html = page.content().lower()
        url = page.url.lower()
        indicators = ['netsuite', 'suitecommerce', 'suite-commerce', '/catalog/', 'nlapi', 'sc.analytics']
        if any(ind in html for ind in indicators):
            return True
        if '/catalog/' in url:
            return True
    except Exception:
        pass
    return False


def _classify_brand_site(final_url: str, page_text: str, original_url: str) -> tuple:
    """
    Return (is_official_brand, is_brand_site) for the loaded page.

    is_official_brand — final URL is on twistedx.com or twisted-x.com.
    is_brand_site     — page is an informational/locator page that carries
                        TX branding but likely does not sell directly.

    The twistedx.com redirect check uses original_url so that navigating
    directly to twistedx.com is NOT treated as a redirect/brand-site.
    """
    final_lower = final_url.lower()
    original_lower = original_url.lower()

    is_official_brand = 'twistedx.com' in final_lower or 'twisted-x.com' in final_lower

    is_brand_site = any([
        # Only flag as brand site if we were redirected TO twistedx.com
        'twistedx.com' in final_lower and 'twistedx.com' not in original_lower,
        'twisted-x.com' in final_lower,
        'find a retailer' in page_text,
        'where to buy' in page_text,
        'retailer locator' in page_text,
        'dealer locator' in page_text,
        'authorized dealer' in page_text,
        'find a store' in page_text,
    ])

    return is_official_brand, is_brand_site
