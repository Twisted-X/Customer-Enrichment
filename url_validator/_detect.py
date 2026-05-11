"""
Per-URL detection: Twisted X products, online sales capability, footwear.

Each function takes a live Playwright page and returns a findings dict.
They are independent of each other and are orchestrated by check_url (_check.py).

Functions
---------
detect_footwear(page, base_url)             -> dict
detect_twisted_x(page, url, ...)            -> dict
detect_online_sales_capability(page)        -> dict
"""
import logging
import re as _re
from typing import Dict

from patchright.sync_api import Page, TimeoutError as PlaywrightTimeout

from browser_utils import goto_safe as _goto_safe


def _body_text(page: Page, timeout: int = 10_000) -> str:
    """
    Return visible body text as a lowercase string.

    Caps inner_text at `timeout` ms and falls back to tag-stripped HTML so
    WAF challenge pages (Imperva, Cloudflare) that keep the DOM in a loading
    state never block a caller indefinitely.
    """
    try:
        return page.inner_text('body', timeout=timeout).lower()
    except Exception:
        try:
            return _re.sub(r'<[^>]+>', ' ', page.content()).lower()
        except Exception:
            return ''

from ._constants import (
    _NO_RESULTS_PHRASES,
    _PURCHASE_BUTTON_SELECTORS,
    _CART_SELECTORS,
    _ONLINE_BLOCKER_PHRASES,
    SEARCH_GROWTH_RATIO,
)
from ._brand import _check_brand_in_content, _check_product_links
from ._browser import _search_on_site, _try_category_pages

log = logging.getLogger(__name__)


def detect_footwear(page: Page, base_url: str) -> Dict:
    """
    Detect if the site sells footwear (boots, shoes, sandals, etc.).

    Tries three methods in order: homepage scan, category page navigation,
    site search for "boots".

    Returns:
        {'sells_footwear': bool | None, 'confidence': str, 'method': str}
        sells_footwear=None means unknown (blocked or timed out).
    """
    result = {'sells_footwear': False, 'confidence': 'low', 'method': None}
    footwear_terms = [
        'boots', 'shoes', 'footwear', 'sandals', 'slippers',
        'cowboy boots', 'work boots', 'western boots',
        'mens footwear', 'womens footwear', 'boot', 'shoe',
    ]
    footwear_paths = ['/boots', '/footwear', '/shoes', '/sandals', '/slippers', '/boot', '/shoe']

    try:
        page_text = _body_text(page)
        page_html = page.content().lower()
        current_url = page.url.lower()

        # Step 1: Homepage scan
        for term in footwear_terms:
            if term in page_text or term in page_html:
                if 'href' in page_html and (term in page_html or f'>{term}' in page_html or term in current_url):
                    result.update(sells_footwear=True, confidence='high', method='homepage_nav')
                    return result
                result.update(sells_footwear=True, confidence='medium', method='homepage_content')
                return result

        for path in footwear_paths:
            if path in current_url or path.rstrip('/') in current_url:
                result.update(sells_footwear=True, confidence='high', method='url_path')
                return result

        # Step 2: Category page check
        for path in ['/boots', '/footwear', '/shoes']:
            try:
                _goto_safe(page, base_url.rstrip('/') + path, timeout=8_000)
                page.wait_for_timeout(1000)
                cat_text = _body_text(page)
                cat_html = page.content().lower()
                if len(cat_text) < 300:
                    continue
                if any(p in cat_text for p in ['no results', 'no products', '0 items', 'page not found', '404']):
                    continue
                has_add_to_cart = 'add to cart' in cat_text or 'add to bag' in cat_text or 'add-to-cart' in cat_html
                has_product_grid = 'product' in cat_html and ('price' in cat_text or '$' in cat_text)
                has_product_links = 'href' in cat_html and ('/product' in cat_html or '/p/' in cat_html or '/item' in cat_html)
                if has_add_to_cart or has_product_grid or has_product_links:
                    result.update(sells_footwear=True, confidence='high', method=f'category_{path}')
                    return result
            except Exception:
                continue

        # Step 3: Search fallback
        if _search_on_site(page, 'boots'):
            page.wait_for_timeout(2000)
            search_text = _body_text(page)
            no_results = any(p in search_text for p in ['no results', 'no products found', '0 results', 'nothing found'])
            if not no_results and len(search_text) > 400:
                result.update(sells_footwear=True, confidence='medium', method='search_boots')
                return result

    except Exception as e:
        result['sells_footwear'] = None
        result['method'] = f'error:{str(e)[:30]}'

    return result


def detect_twisted_x(page: Page, url: str, return_page_info: bool = False) -> Dict:
    """
    Detect if the site carries Twisted X products using four methods in order.

    Methods tried:
    1. Homepage text + HTML scan.
    2. Product link and heading scan on the homepage.
    3. Site search for "Twisted X" and "twistedx" (with scroll for lazy results).
    4. Category pages (/boots, /footwear, /shoes) if search fails.

    Returns:
        {
            'has_products': bool,
            'method':       str  ('homepage_mention' | 'product_links' |
                                  'search_results' | 'search_results_links' |
                                  'image_alt_text' | 'category_page' | 'not_found'),
            'found_on_url': str | None  (set only when return_page_info=True),
            'error':        str | None,
        }
    """
    result = {'has_products': False, 'method': None, 'found_on_url': None, 'error': None}

    def _found(method: str) -> Dict:
        result['has_products'] = True
        result['method'] = method
        if return_page_info:
            result['found_on_url'] = page.url
        return result

    try:
        page_text = _body_text(page)
        page_html = page.content().lower()

        if _check_brand_in_content(page_text, page_html):
            return _found('homepage_mention')

        if _check_product_links(page):
            return _found('product_links')

        # Case variants are redundant — retailer search engines are universally
        # case-insensitive. Two variants: canonical name + slug form.
        for search_term in ['Twisted X', 'twistedx']:
            original_url = page.url

            if _search_on_site(page, search_term):
                page.wait_for_timeout(3000)
                new_url = page.url
                page_after_search = _body_text(page)

                url_changed = (
                    new_url != original_url
                    or 'search' in new_url.lower()
                    or 'q=' in new_url.lower()
                )
                content_changed = len(page_after_search) > len(page_text) * SEARCH_GROWTH_RATIO

                if url_changed or content_changed:
                    # Scroll to surface lazy-loaded results
                    try:
                        for _ in range(3):
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(1500)
                    except Exception:
                        pass

                    results_text = _body_text(page)
                    results_html = page.content().lower()
                    has_no_results = any(p in results_text for p in _NO_RESULTS_PHRASES)

                    if _check_brand_in_content(results_text, results_html) and not has_no_results:
                        return _found('search_results')

                    if not has_no_results and _check_product_links(page):
                        return _found('search_results_links')

                    if not has_no_results:
                        try:
                            for img in page.query_selector_all('img[alt*="twisted"], img[alt*="Twisted"]')[:10]:
                                alt = (img.get_attribute('alt') or '').lower()
                                if _check_brand_in_content(alt):
                                    return _found('image_alt_text')
                        except Exception:
                            pass

            # Only reload the homepage if we actually navigated away, so we
            # start the next search term from a clean state without an extra
            # page load when the search never left the homepage.
            if page.url != original_url:
                try:
                    _goto_safe(page, url, timeout=10_000)
                    page.wait_for_timeout(500)
                except Exception:
                    pass

        if _try_category_pages(page, url):
            return _found('category_page')

        result['method'] = 'not_found'
        return result

    except PlaywrightTimeout:
        result['error'] = 'Timeout loading page'
        return result
    except Exception as e:
        result['error'] = f'Error: {str(e)[:50]}'
        return result


def detect_online_sales_capability(page: Page) -> Dict:
    """
    Detect whether the site allows online purchasing.

    STRICT: Requires actual functional purchase buttons or a shopping cart,
    not just text mentions. Blocker phrases like "in-store only" can override
    weak online signals, but are ignored when a cart is confirmed present.

    Returns:
        {
            'sells_online':  bool,
            'confidence':    'high' | 'medium' | 'low',
            'indicators':    List[str],
            'blockers':      List[str],
        }
    """
    result = {'sells_online': False, 'confidence': 'low', 'indicators': [], 'blockers': []}

    try:
        page_text = _body_text(page)
        page_html = page.content().lower()

        # Check for visible, non-disabled purchase buttons
        has_functional_buttons = False
        for selector in _PURCHASE_BUTTON_SELECTORS:
            try:
                for btn in page.query_selector_all(selector)[:5]:
                    try:
                        if btn.is_visible() and btn.get_attribute('disabled') is None:
                            has_functional_buttons = True
                            result['indicators'].append(f'functional_button:{selector[:40]}')
                            break
                    except Exception:
                        continue
                if has_functional_buttons:
                    break
            except Exception:
                continue

        # Check for a shopping cart link/widget
        has_cart = False
        for selector in _CART_SELECTORS:
            try:
                for el in page.query_selector_all(selector)[:3]:
                    if el.is_visible():
                        has_cart = True
                        result['indicators'].append('shopping_cart_found')
                        break
                if has_cart:
                    break
            except Exception:
                continue

        # Cart present → confirmed e-commerce; purchase buttons alone → also confirmed.
        if has_cart and ('checkout' in page_text or 'checkout' in page_html):
            result.update(sells_online=True, confidence='high')
            result['indicators'].append('cart_and_checkout')
        elif has_cart:
            result.update(sells_online=True, confidence='high')
        elif has_functional_buttons:
            result.update(sells_online=True, confidence='high')

        # Blocker phrases override weak online signals, but NOT a confirmed cart.
        blocker_found = False
        for blocker in _ONLINE_BLOCKER_PHRASES:
            if blocker in page_text:
                result['blockers'].append(blocker)
                blocker_found = True
                strong_blocker = blocker in ('in-store only', 'no online ordering', 'call for availability')
                if strong_blocker and result['sells_online'] and not has_cart:
                    result.update(sells_online=False, confidence='low')
                    result['indicators'].append(f'blocked_by:{blocker}')

        if blocker_found and not has_functional_buttons and not has_cart:
            result['sells_online'] = False
            if result['confidence'] == 'high':
                result['confidence'] = 'medium'
            elif result['confidence'] == 'medium':
                result['confidence'] = 'low'

        # Without buttons or cart, don't mark as selling online regardless of text.
        if not has_functional_buttons and not has_cart:
            result['sells_online'] = False
            if result['confidence'] == 'high':
                result['confidence'] = 'medium'

        return result

    except Exception as e:
        result['indicators'].append(f'error:{str(e)[:30]}')
        return result
