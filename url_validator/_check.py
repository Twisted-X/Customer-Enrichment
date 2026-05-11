"""
check_url: orchestrates per-URL detection and returns a unified result dict.

Functions
---------
_has_physical_store_indicators(page) -> bool
_apply_brand_site_overrides(...)     -> None   (mutates result in place)
check_url(url, page, retries)        -> dict
"""
import logging
from typing import Dict
from urllib.parse import urlparse

from patchright.sync_api import Page, TimeoutError as PlaywrightTimeout

from browser_utils import goto_safe as _goto_safe

from ._constants import VALIDATION_TIMEOUT, _PHYSICAL_STORE_PHRASES
from ._brand import _classify_brand_site
from ._browser import _close_popups
from ._detect import detect_twisted_x, detect_online_sales_capability, detect_footwear

log = logging.getLogger(__name__)


def _has_physical_store_indicators(page: Page) -> bool:
    """Return True if the page contains typical physical-store language."""
    try:
        pt = page.inner_text('body', timeout=8_000).lower()
        return any(p in pt for p in _PHYSICAL_STORE_PHRASES)
    except Exception:
        return False


def _apply_brand_site_overrides(
    result: Dict,
    is_official_brand: bool,
    is_brand_site: bool,
    online_check: Dict,
    page: Page,
) -> None:
    """
    Adjust result['sells_online'] for Twisted X brand / informational pages.

    Official brand sites (twistedx.com) only count as selling online when they
    show real product listings with working purchase buttons. Other brand/info
    sites need high-confidence online indicators. Mutates result and online_check
    in place (online_check is already stored in result['online_sales']).
    """
    if is_official_brand:
        has_product_listings = any([
            page.query_selector('div[class*="product"]') is not None,
            page.query_selector('[class*="product-grid"]') is not None,
            page.query_selector('[class*="product-list"]') is not None,
        ])
        has_working_buttons = False
        try:
            buttons = page.query_selector_all(
                'button:has-text("Add to Cart"), a:has-text("Add to Cart"), button:has-text("Buy Now")'
            )
            has_working_buttons = any(btn.is_visible() for btn in buttons[:3])
        except Exception:
            pass

        if has_product_listings and has_working_buttons:
            result['sells_online'] = online_check['sells_online']
        else:
            result['sells_online'] = False
            online_check['confidence'] = 'low'
            online_check['blockers'].append('official brand site (no direct sales)')

    elif is_brand_site:
        if online_check['confidence'] == 'high' and online_check.get('indicators'):
            result['sells_online'] = online_check['sells_online']
        else:
            result['sells_online'] = False
            online_check['confidence'] = 'low'
            online_check['blockers'].append('brand/informational site')

    else:
        result['sells_online'] = online_check['sells_online']


def check_url(url: str, page: Page, retries: int = 2, skip_tx_check: bool = False) -> Dict:
    """
    Full validation: Twisted X detection + online sales + footwear detection.

    Tracks redirects and applies brand-site overrides so informational pages
    that redirect to twistedx.com are not credited as online sellers.

    Args:
        url:           Retailer URL to validate.
        page:          Live Playwright page (caller owns the browser context).
        retries:       How many extra navigation attempts on transient errors.
        skip_tx_check: When True, skip detect_twisted_x and the early-exit
                       e-commerce gate — only runs detect_online_sales_capability
                       and detect_footwear. Use this from checker._playwright
                       which runs its own authoritative SKU scan so the duplicate
                       detect_twisted_x pass is wasted work.

    Returns:
        {
            'has_twisted_x':               bool,
            'sells_online':                bool,
            'sells_footwear':              bool | None,
            'combined_status':             str,
            'twisted_x_method':            str,
            'online_sales':                dict,
            'final_url':                   str,
            'redirected':                  bool,
            'has_physical_store_indicators': bool,
            'error':                       str | None,
        }

    combined_status values:
        'has_products_sells_online'   — TX detected + e-commerce → scrape
        'has_products_in_store_only'  — TX detected, no online sales
        'ecommerce_no_twisted_x'      — e-commerce confirmed, no TX
        'no_products_no_online'       — skip
        'error'                       — navigation failed
    """
    result = {
        'has_twisted_x': False,
        'sells_online': False,
        'sells_footwear': None,
        'combined_status': 'none',
        'twisted_x_method': None,
        'online_sales': {},
        'final_url': url,
        'found_on_url': None,   # URL where TX products were actually found
        'redirected': False,
        'error': None,
    }

    # ── Navigation with retries ────────────────────────────────────────────
    navigation_success = False
    last_error = None

    for attempt in range(retries + 1):
        timeout = VALIDATION_TIMEOUT + (attempt * 5000)
        try:
            _goto_safe(page, url, timeout=timeout)
            page.wait_for_timeout(1000)
            if page.url and page.url != 'about:blank':
                navigation_success = True
                break
            else:
                last_error = 'Navigation resulted in blank page'
        except PlaywrightTimeout:
            last_error = f'Timeout (attempt {attempt + 1}/{retries + 1})'
            if attempt < retries:
                page.wait_for_timeout(2000)
        except Exception as e:
            error_msg = str(e)
            is_transient = any(
                t in error_msg for t in ['ERR_NAME_NOT_RESOLVED', 'net::', 'Navigation timeout', 'Timeout']
            )
            if is_transient and attempt < retries:
                last_error = f'Network error (attempt {attempt + 1}/{retries + 1}): {error_msg[:60]}'
                page.wait_for_timeout(3000)
            else:
                last_error = f'Error: {error_msg[:80]}'
                break

    if not navigation_success:
        result['error'] = last_error
        result['combined_status'] = 'error'
        return result

    # ── Post-navigation checks ─────────────────────────────────────────────
    try:
        final_url = page.url
        result['final_url'] = final_url
        result['redirected'] = (final_url != url)

        try:
            # Use a generous timeout — JS-heavy sites (Magento, SPA) render
            # body content asynchronously and may need >10 s after load event.
            page_text = page.inner_text('body', timeout=30_000).lower()
        except Exception:
            # Challenge pages (Imperva, CF) can make inner_text hang.
            # Fall back to raw HTML text stripping.
            try:
                import re as _re
                page_text = _re.sub(r'<[^>]+>', ' ', page.content()).lower()
            except Exception:
                page_text = ''
        is_official_brand, is_brand_site = _classify_brand_site(final_url, page_text, url)

        _close_popups(page)
        page.wait_for_timeout(500)

        # _cached_online_check holds a result from the early-exit gate check so
        # we never re-run detect_online_sales_capability on a page that may have
        # been WAF-blocked by TX-detection navigations (Imperva / Cloudflare can
        # block subsequent requests even when the first page load succeeded).
        _cached_online_check = None

        if not skip_tx_check:
            # Early exit: if no e-commerce signals, skip the expensive TX and
            # footwear checks (saves 30-60 s per URL on non-e-commerce sites).
            # Skipped when the checker runs its own authoritative SKU scan.
            if not is_official_brand:
                quick_online = detect_online_sales_capability(page)
                _cached_online_check = quick_online   # cache — page is clean here
                if not quick_online['sells_online']:
                    result.update(
                        sells_online=False,
                        sells_footwear=None,   # detection was skipped — unknown, not False
                        has_twisted_x=False,
                        online_sales=quick_online,
                        combined_status='no_products_no_online',
                        has_physical_store_indicators=_has_physical_store_indicators(page),
                    )
                    return result

            # Check 1: Twisted X detection (may navigate to search/category pages)
            tx_check = detect_twisted_x(page, final_url, return_page_info=True)
            result['has_twisted_x'] = tx_check['has_products']
            result['twisted_x_method'] = tx_check['method']
            found_on_url = tx_check.get('found_on_url', final_url)
            result['found_on_url'] = found_on_url   # expose for scraper to navigate back
            if tx_check['error']:
                result['error'] = tx_check['error']

            # Navigate back to where TX was found before running online sales check.
            try:
                target_url = found_on_url if (found_on_url and found_on_url != final_url) else final_url
                if page.url != target_url:
                    _goto_safe(page, target_url, timeout=12_000)
                    page.wait_for_timeout(1000)
                    _close_popups(page)
            except Exception:
                pass  # Continue on current page if navigation fails
        else:
            # TX check skipped — caller (checker._playwright) owns TX detection.
            # Navigate back to the homepage so online/footwear checks run there.
            if page.url != final_url:
                try:
                    _goto_safe(page, final_url, timeout=12_000)
                    page.wait_for_timeout(1000)
                    _close_popups(page)
                except Exception:
                    pass

        # Check 2: Online sales capability.
        # Reuse the cached result from the early-exit gate when available — TX
        # detection navigations can leave the browser on a WAF-blocked page and
        # a fresh detect_online_sales_capability call would see empty content.
        if _cached_online_check is not None:
            online_check = _cached_online_check
            log.debug("check_url: reusing cached online_check (%s)", online_check.get('confidence'))
        else:
            online_check = detect_online_sales_capability(page)
        result['online_sales'] = online_check
        _apply_brand_site_overrides(result, is_official_brand, is_brand_site, online_check, page)

        # Check 3: Footwear detection
        try:
            parsed = urlparse(final_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            footwear_check = detect_footwear(page, base_url)
            result['sells_footwear'] = footwear_check.get('sells_footwear')
        except Exception:
            result['sells_footwear'] = None

        # Coerce footwear: TX implies footwear; no online sales → can't be online footwear.
        if result['has_twisted_x']:
            result['sells_footwear'] = True
        elif not result['sells_online'] and result['sells_footwear'] is None:
            result['sells_footwear'] = False

        # ── Combined status ────────────────────────────────────────────────
        if result['has_twisted_x'] and result['sells_online']:
            result['combined_status'] = 'has_products_sells_online'
        elif result['has_twisted_x']:
            result['combined_status'] = 'has_products_in_store_only'
        elif result['sells_online']:
            result['combined_status'] = 'ecommerce_no_twisted_x'
        else:
            result['combined_status'] = 'no_products_no_online'

        result['has_physical_store_indicators'] = (
            _has_physical_store_indicators(page)
            if result['combined_status'] == 'no_products_no_online'
            else False
        )

        return result

    except Exception as e:
        result['error'] = f'Error after navigation: {str(e)[:80]}'
        result['combined_status'] = 'error'
        return result
