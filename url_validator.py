"""
URL Validator - Pre-filter URLs to find ones that actually sell Twisted X online

This script validates URLs from NetSuite CSV to determine:
1. If URL is valid and accessible
2. If site has Twisted X products
3. If site sells products online (vs in-store only)

This pre-filtering saves time and API costs by only scraping relevant URLs.
"""
import csv
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse
from datetime import datetime
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

from config import HEADLESS


# Configuration
TIMEOUT_MS = 20000  # 20 seconds per URL for thorough check
VALIDATION_TIMEOUT = 18000  # 18 seconds for page load


def normalize_url(url: str) -> Optional[str]:
    """
    Normalize and validate URL.
    
    Fixes common issues:
    - http://http:/ -> http://
    - http://ww. -> http://www.
    - Missing protocol -> adds https://
    - Invalid entries like "- None -"
    
    Args:
        url: Raw URL string from CSV
        
    Returns:
        Normalized URL string or None if invalid
    """
    if not url or url.strip() in ['- None -', 'None', '', 'N/A', 'n/a']:
        return None
    
    url = url.strip()
    
    # Fix common malformed URLs
    if url.startswith('http://http:/'):
        url = url.replace('http://http:/', 'http://')
    if url.startswith('http://ww.'):
        url = url.replace('http://ww.', 'http://www.')
    if url.startswith('https://https://'):
        url = url.replace('https://https://', 'https://')
    
    # Add protocol if missing
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Validate URL format
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return None
        # Basic validation - must have domain
        if '.' not in parsed.netloc:
            return None
        return url
    except Exception:
        return None


def _close_popups(page: Page):
    """Close any popup overlays that might block interactions."""
    popup_close_selectors = [
        '[class*="lightbox"] [class*="close"]',
        '[class*="modal"] [class*="close"]',
        '[class*="popup"] [class*="close"]',
        '[class*="overlay"] button',
        'button[aria-label="Close"]',
        'button[aria-label="close"]',
        '[class*="dismiss"]',
        '.close-button',
        '[data-dismiss]',
        'button:has-text("Close")',
        'button:has-text("No thanks")',
        'button:has-text("×")',
        '[class*="fb_lightbox"] button',
    ]
    
    for selector in popup_close_selectors:
        try:
            close_btn = page.query_selector(selector)
            if close_btn and close_btn.is_visible():
                close_btn.click(timeout=2000)
                page.wait_for_timeout(500)
        except:
            continue
    
    # Try pressing Escape key
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(500)
    except:
        pass


def _is_netsuite_site(page: Page) -> bool:
    """
    Detect NetSuite/SuiteCommerce site for search URL ordering.
    When true, /catalog/productsearch should be tried first.
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


def _search_on_site(page: Page, search_term: str) -> bool:
    """
    Find search box and search for a term.
    Reused from fetcher.py logic.
    
    Args:
        page: Playwright page object
        search_term: Term to search for
        
    Returns:
        True if search was submitted, False otherwise
    """
    # Close popups first
    _close_popups(page)
    
    # Common search input selectors (includes WooCommerce, Shopify, BigCommerce, etc.)
    search_selectors = [
        'input[type="search"]',
        'input[placeholder*="Search" i]',
        'input[placeholder*="search"]',
        '.chakra-input[type="search"]',
        'input[name="q"]',
        'input[name="s"]',                              # WooCommerce default
        'input[name="search"]',
        'input[name="query"]',
        'input[aria-label*="Search" i]',
        'input[aria-label*="search" i]',
        '#search',
        '#search-input',
        '#woocommerce-product-search-field',             # WooCommerce widget
        '.search-input',
        '.woocommerce-product-search input',             # WooCommerce search form
        'form.search-form input[type="search"]',         # WordPress/WooCommerce
        'form.search-form input[name="s"]',              # WordPress/WooCommerce
        'form[role="search"] input',                     # Accessibility-friendly forms
        '.dgwt-wcas-search-input',                       # AJAX Search for WooCommerce plugin
        '#dgwt-wcas-search-input',                       # AJAX Search for WooCommerce plugin
        '[class*="search-field"] input',
        '[class*="search"] input',
        '[class*="Search"] input',
    ]
    
    for selector in search_selectors:
        try:
            search_input = page.query_selector(selector)
            if search_input:
                is_visible = search_input.is_visible()
                if is_visible:
                    try:
                        search_input.scroll_into_view_if_needed()
                        page.wait_for_timeout(300)
                        try:
                            search_input.click(timeout=3000)
                        except:
                            search_input.evaluate("el => el.focus()")
                        page.wait_for_timeout(300)  # Reduced wait
                        search_input.fill(search_term)
                        page.wait_for_timeout(300)  # Reduced wait
                        search_input.press("Enter")
                        page.wait_for_timeout(2000)  # Reduced from 3000
                        return True
                    except Exception:
                        continue
        except Exception:
            continue
    
    # Try clicking search icon/toggle first (WooCommerce, Shopify, etc.)
    search_icon_selectors = [
        'button[aria-label*="Search" i]',
        'button[aria-label*="search" i]',
        '[class*="search-icon"]',
        '[class*="search-button"]',
        '[class*="search-toggle"]',                      # WooCommerce theme toggles
        '[class*="header-search"] a',                    # Theme header search triggers
        '[class*="header-search"] button',
        'a[href*="search"]',
        '.search-submit',                                # WordPress search submit
        'button[type="submit"][class*="search"]',
        'form.search-form button',                       # WordPress/WooCommerce
    ]
    
    for icon_sel in search_icon_selectors:
        try:
            icon = page.query_selector(icon_sel)
            if icon and icon.is_visible():
                icon.click()
                page.wait_for_timeout(1000)
                for selector in search_selectors:
                    try:
                        search_input = page.query_selector(selector)
                        if search_input and search_input.is_visible():
                            search_input.fill(search_term)
                            search_input.press("Enter")
                            page.wait_for_timeout(3000)
                            return True
                    except:
                        continue
        except:
            continue
    
    # Fallback: direct URL-based search (runs BEFORE JS form submit to prefer
    # WooCommerce product search over generic WordPress blog search)
    try:
        from urllib.parse import urlparse
        base = urlparse(page.url)
        base_url = f"{base.scheme}://{base.netloc}"
        encoded_term = search_term.replace(' ', '+')

        search_urls = [
            f"{base_url}/?s={encoded_term}&post_type=product",   # WooCommerce product search
            f"{base_url}/search?q={encoded_term}",               # Shopify/generic
            f"{base_url}/search?type=product&q={encoded_term}",  # Shopify product-only search
            f"{base_url}/searchPage.action?keyWord={encoded_term}",  # Java/Struts (e.g. stockdales.com)
            f"{base_url}/catalog/productsearch",  # NetSuite/catalog (full catalog; scan for Twisted X)
            f"{base_url}/?s={encoded_term}",                     # WordPress generic (last)
        ]

        # NetSuite/SuiteCommerce: try /catalog/productsearch first (WooCommerce URL can false-positive)
        if _is_netsuite_site(page):
            catalog_url = f"{base_url}/catalog/productsearch"
            search_urls = [catalog_url] + [u for u in search_urls if u != catalog_url]

        for search_url in search_urls:
            try:
                page.goto(search_url, timeout=15000, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)
                if '?s=' in page.url or 'search' in page.url.lower() or 'q=' in page.url or 'productsearch' in page.url.lower():
                    return True
            except:
                continue
    except:
        pass
    
    return False


def _check_brand_in_content(text: str, html: str = '') -> bool:
    """
    Check if any Twisted X Global Brands are mentioned in text or HTML.
    
    Args:
        text: Visible text content (lowercase)
        html: HTML content (lowercase, optional)
        
    Returns:
        True if any brand indicator found
    """
    brand_indicators = [
        'twisted x', 'twistedx', 'twisted-x', 'twistedx.com',
        'twisted x work', 'twistedx work', 'twisted-x work',
        'black star', 'black star boots', 'blackstar',
        'cellsole', 'cell sole', 'cell-sole',
        'hooey',  # When clearly Twisted X related
    ]
    
    content = text
    if html:
        content = text + ' ' + html
    
    return any(indicator in content for indicator in brand_indicators)


def _check_product_links(page: Page) -> bool:
    """
    Check product links for brand mentions.
    More thorough checking including product titles, descriptions, and all links.
    
    Args:
        page: Playwright page object
        
    Returns:
        True if brand found in product links
    """
    try:
        # Get all links that might be products (expanded selectors)
        links = page.query_selector_all('a[href*="product"], a[href*="item"], a[href*="boot"], a[href*="shoe"], a[href*="p-"], a[href*="/p/"]')
        
        # Check first 100 links (increased from 50)
        for link in links[:100]:
            try:
                href = link.get_attribute('href') or ''
                text = (link.inner_text() or '').lower()
                
                # Also check parent elements for product names
                try:
                    parent = link.evaluate_handle('el => el.closest("div, article, section")')
                    if parent:
                        parent_text = (parent.inner_text() or '').lower() if hasattr(parent, 'inner_text') else ''
                        combined_text = text + ' ' + parent_text + ' ' + href.lower()
                    else:
                        combined_text = text + ' ' + href.lower()
                except:
                    combined_text = text + ' ' + href.lower()
                
                if _check_brand_in_content(combined_text):
                    return True
            except:
                continue
        
        # Also check product titles/names in common product containers
        try:
            product_selectors = [
                '[class*="product"] [class*="title"]',
                '[class*="product"] [class*="name"]',
                '[class*="item"] [class*="title"]',
                'h2, h3, h4'  # Product titles often in headings
            ]
            
            for selector in product_selectors:
                elements = page.query_selector_all(selector)
                for el in elements[:50]:
                    try:
                        text = (el.inner_text() or '').lower()
                        if _check_brand_in_content(text):
                            return True
                    except:
                        continue
        except:
            pass
    except:
        pass
    
    return False


def detect_footwear(page: Page, base_url: str) -> Dict:
    """
    Detect if the site sells footwear (boots, shoes, sandals, etc.).
    
    Returns:
        {'sells_footwear': bool|None, 'confidence': str, 'method': str}
        None = unknown (blocked/timeout)
    """
    result = {'sells_footwear': False, 'confidence': 'low', 'method': None}
    footwear_terms = [
        'boots', 'shoes', 'footwear', 'sandals', 'slippers',
        'cowboy boots', 'work boots', 'mens footwear', 'womens footwear',
        'boot', 'shoe', 'western boots', 'work boots'
    ]
    # URL path patterns that indicate footwear
    footwear_paths = ['/boots', '/footwear', '/shoes', '/sandals', '/slippers', '/boot', '/shoe']
    
    try:
        # Step 1: Homepage scan
        page_text = page.inner_text('body').lower()
        page_html = page.content().lower()
        current_url = page.url.lower()
        
        for term in footwear_terms:
            if term in page_text or term in page_html:
                # Check if in nav/link context (more reliable)
                if f'href' in page_html and (term in page_html or f'>{term}' in page_html or term in current_url):
                    result['sells_footwear'] = True
                    result['confidence'] = 'high'
                    result['method'] = 'homepage_nav'
                    return result
                # Loose match in page content
                result['sells_footwear'] = True
                result['confidence'] = 'medium'
                result['method'] = 'homepage_content'
                return result
        
        for path in footwear_paths:
            if path in current_url or path.rstrip('/') in current_url:
                result['sells_footwear'] = True
                result['confidence'] = 'high'
                result['method'] = 'url_path'
                return result
        
        # Step 2: Category page check
        category_paths = ['/boots', '/footwear', '/shoes']
        for path in category_paths:
            try:
                category_url = base_url.rstrip('/') + path
                page.goto(category_url, timeout=6000, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)
                cat_text = page.inner_text('body').lower()
                cat_html = page.content().lower()
                if len(cat_text) < 300:
                    continue
                # No-results indicators
                no_results = any(p in cat_text for p in ['no results', 'no products', '0 items', 'page not found', '404'])
                if no_results:
                    continue
                # Product indicators
                has_add_to_cart = 'add to cart' in cat_text or 'add to bag' in cat_text or 'add-to-cart' in cat_html
                has_product_grid = 'product' in cat_html and ('price' in cat_text or '$' in cat_text)
                has_product_links = 'href' in cat_html and ('/product' in cat_html or '/p/' in cat_html or '/item' in cat_html)
                if has_add_to_cart or has_product_grid or has_product_links:
                    result['sells_footwear'] = True
                    result['confidence'] = 'high'
                    result['method'] = f'category_{path}'
                    return result
            except Exception:
                continue
        
        # Step 3: Search fallback
        if _search_on_site(page, 'boots'):
            page.wait_for_timeout(2000)
            search_text = page.inner_text('body').lower()
            no_results = any(p in search_text for p in ['no results', 'no products found', '0 results', 'nothing found'])
            if not no_results and len(search_text) > 400:
                result['sells_footwear'] = True
                result['confidence'] = 'medium'
                result['method'] = 'search_boots'
                return result
    except Exception as e:
        result['sells_footwear'] = None
        result['method'] = f'error:{str(e)[:30]}'
        return result
    return result


def _try_category_pages(page: Page, base_url: str) -> bool:
    """
    Try navigating to common category pages that might have Twisted X products.
    Only tries a few most common categories to save time.
    
    Args:
        page: Playwright page object
        base_url: Base URL of the site
        
    Returns:
        True if brand found on category page
    """
    # Only try most common categories (reduced from 8 to 3)
    category_paths = [
        '/boots', '/footwear', '/shoes'
    ]
    
    for path in category_paths:
        try:
            category_url = base_url.rstrip('/') + path
            page.goto(category_url, timeout=6000, wait_until='domcontentloaded')  # Reduced timeout
            page.wait_for_timeout(2000)  # Reduced wait
            
            page_text = page.inner_text('body').lower()
            page_html = page.content().lower()
            
            if _check_brand_in_content(page_text, page_html):
                return True
        except:
            continue
    
    return False


def detect_twisted_x(page: Page, url: str, return_page_info: bool = False) -> Dict:
    """
    Detect if site has Twisted X products using multiple robust methods.
    
    Methods (in order):
    1. Check homepage text AND HTML for brand mentions
    2. Check product links for brand names
    3. Try searching with multiple search terms and variations
    4. Try category pages (boots, footwear, etc.) - only if search fails
    5. Check search results thoroughly (text + HTML + links)
    
    Args:
        page: Playwright page object
        url: URL being checked
        return_page_info: If True, also return the URL where products were found
        
    Returns:
        {
            'has_products': bool,
            'method': str ('homepage_mention' | 'product_links' | 'search_results' | 'category_page' | None),
            'found_on_url': str (URL where products were found, if return_page_info=True),
            'error': str or None
        }
    """
    result = {
        'has_products': False,
        'method': None,
        'found_on_url': None,
        'error': None
    }
    
    try:
        # Get both text and HTML for thorough checking
        page_text = page.inner_text('body').lower()
        page_html = page.content().lower()
        
        # Method 1: Check homepage text AND HTML for Twisted X Global Brands
        if _check_brand_in_content(page_text, page_html):
            result['has_products'] = True
            result['method'] = 'homepage_mention'
            if return_page_info:
                result['found_on_url'] = page.url
            return result
        
        # Method 2: Check product links on homepage
        if _check_product_links(page):
            result['has_products'] = True
            result['method'] = 'product_links'
            if return_page_info:
                result['found_on_url'] = page.url
            return result
        
        # Method 3: Try searching with multiple search terms (FAST PATH)
        # Try more variations to catch different naming conventions
        search_terms = ['Twisted X', 'TwistedX', 'twisted x', 'twistedx', 'TwistedX Boots', 'Twisted X Boots']
        
        for search_term in search_terms:
            original_url = page.url
            
            if _search_on_site(page, search_term):
                # Wait for search results to load
                page.wait_for_timeout(3000)
                
                # Check if URL changed OR if page content changed significantly (search might work without URL change)
                new_url = page.url
                page_after_search = page.inner_text('body').lower()
                
                # Check if search worked (URL changed OR content changed significantly)
                url_changed = new_url != original_url or 'search' in new_url.lower() or 'q=' in new_url.lower()
                content_changed = len(page_after_search) > len(page_text) * 1.2  # Content increased by 20%+
                
                if url_changed or content_changed:
                    # Search likely worked, check results
                    
                    # Scroll to load more content (3 times for better coverage)
                    try:
                        for _ in range(3):
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(1500)
                    except:
                        pass
                    
                    # Get updated content after scrolling
                    results_text = page.inner_text('body').lower()
                    results_html = page.content().lower()
                    
                    # Check for "no results" indicators BEFORE counting as positive
                    no_results_phrases = [
                        'no results', 'no results found', 'no results could be found',
                        'did not match', 'no products found', 'no items found',
                        '0 results', '0 items', 'nothing found',
                        'no matches', 'could not find',
                        'your search returned no', 'no search results',
                        'we couldn\'t find', 'we could not find',
                        'sorry, no results', 'sorry, we couldn\'t find',
                    ]
                    has_no_results = any(phrase in results_text for phrase in no_results_phrases)
                    
                    # Check text, HTML, and links (but skip if "no results" page)
                    if _check_brand_in_content(results_text, results_html) and not has_no_results:
                        result['has_products'] = True
                        result['method'] = 'search_results'
                        if return_page_info:
                            result['found_on_url'] = page.url
                        return result
                    
                    # Also check product links in search results (skip on "no results" pages)
                    if not has_no_results and _check_product_links(page):
                        result['has_products'] = True
                        result['method'] = 'search_results_links'
                        if return_page_info:
                            result['found_on_url'] = page.url
                        return result
                    
                    # Check for product images with alt text (skip on "no results" pages)
                    if not has_no_results:
                        try:
                            images = page.query_selector_all('img[alt*="twisted"], img[alt*="Twisted"]')
                            if images:
                                for img in images[:10]:
                                    alt_text = (img.get_attribute('alt') or '').lower()
                                    if _check_brand_in_content(alt_text):
                                        result['has_products'] = True
                                        result['method'] = 'image_alt_text'
                                        if return_page_info:
                                            result['found_on_url'] = page.url
                                        return result
                        except:
                            pass
                
                # If search didn't work or no results, go back to homepage for next search
                try:
                    page.goto(url, timeout=8000, wait_until='domcontentloaded')
                    page.wait_for_timeout(1500)
                except:
                    pass
        
        # Method 4: Try category pages (only if search failed - slower)
        if _try_category_pages(page, url):
            result['has_products'] = True
            result['method'] = 'category_page'
            if return_page_info:
                result['found_on_url'] = page.url
            return result
        
        # No Twisted X found after all methods
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
    Detect if site allows online purchasing.
    
    STRICT CHECKING: Requires actual functional purchase buttons, not just text mentions.
    
    Checks for:
    - ACTUAL clickable purchase buttons (Add to Cart, Buy Now, etc.)
    - Shopping cart functionality
    - Checkout process
    - Blockers: "In-store only", "Visit our store", etc.
    
    Args:
        page: Playwright page object
        
    Returns:
        {
            'sells_online': bool,
            'confidence': 'high' | 'medium' | 'low',
            'indicators': List[str],
            'blockers': List[str]
        }
    """
    result = {
        'sells_online': False,
        'confidence': 'low',
        'indicators': [],
        'blockers': []
    }
    
    try:
        page_text = page.inner_text('body').lower()
        page_html = page.content().lower()
        
        # ===== CRITICAL: Check for ACTUAL FUNCTIONAL purchase buttons =====
        # Don't just check for text - check for actual clickable buttons
        purchase_button_selectors = [
            'button:has-text("Add to Cart")',
            'button:has-text("Add to Bag")',
            'button:has-text("Buy Now")',
            'button:has-text("Purchase")',
            'a:has-text("Add to Cart")',
            'a:has-text("Buy Now")',
            '[class*="add-to-cart"] button',
            '[class*="add-to-cart"] a',
            '[class*="buy-now"] button',
            '[id*="add-to-cart"]',
            '[id*="buy-now"]',
            'button[data-action="add-to-cart"]',
            'button[data-action="buy-now"]',
            # WooCommerce-specific
            'button.single_add_to_cart_button',           # WooCommerce product page
            '.add_to_cart_button',                        # WooCommerce shop/archive
            'a.add_to_cart_button',                       # WooCommerce AJAX add to cart
            'button[name="add-to-cart"]',                 # WooCommerce form submit
            '.woocommerce-cart-form button',              # WooCommerce cart
            'a:has-text("Add to wishlist")',              # WooCommerce Wishlist plugin
            'button:has-text("Add to wishlist")',
            # Shopify-specific
            'button[name="add"]',                         # Shopify add to cart
            'form[action*="/cart/add"] button',
        ]
        
        has_functional_buttons = False
        for selector in purchase_button_selectors:
            try:
                elements = page.query_selector_all(selector)
                if elements:
                    # Check if at least one button is actually visible and clickable
                    for btn in elements[:5]:  # Check first 5 buttons
                        try:
                            if btn.is_visible():
                                # Check if it's not disabled
                                is_disabled = btn.get_attribute('disabled') is not None
                                if not is_disabled:
                                    has_functional_buttons = True
                                    result['indicators'].append(f'functional_button:{selector[:40]}')
                                    break
                        except:
                            continue
                    if has_functional_buttons:
                        break
            except:
                continue
        
        # Also check for shopping cart icon/link (indicates e-commerce functionality)
        cart_selectors = [
            'a[href*="cart"]',
            'a[href*="checkout"]',
            '[class*="cart"] a',
            '[class*="shopping-cart"]',
            '[id*="cart"]',
            'button:has-text("View Cart")',
            'a:has-text("Cart")',
            # WooCommerce-specific
            '.woocommerce-cart-form',                     # WooCommerce cart page
            'a[href*="wc-ajax"]',                         # WooCommerce AJAX cart
            '.cart-contents',                             # WooCommerce mini-cart link
            '.woocommerce-mini-cart',                     # WooCommerce mini-cart widget
            '[class*="woo"] [class*="cart"]',             # Generic WooCommerce cart
            'a[href*="/cart/"]',                          # Common cart URL pattern
            '.cart-count',                                # Cart count badge
            '.cart_totals',                               # WooCommerce cart totals
        ]
        
        has_cart = False
        for selector in cart_selectors:
            try:
                elements = page.query_selector_all(selector)
                if elements:
                    for el in elements[:3]:
                        if el.is_visible():
                            has_cart = True
                            result['indicators'].append('shopping_cart_found')
                            break
                    if has_cart:
                        break
            except:
                continue
        
        # ===== STRONG INDICATORS (require actual buttons, not just text) =====
        # IMPORTANT: shopping_cart_found and cart_and_checkout = 100% sells online
        # Check these FIRST before any other logic
        if has_cart and ('checkout' in page_text or 'checkout' in page_html):
            # Has cart and checkout mentioned = 100% e-commerce
            result['sells_online'] = True
            result['confidence'] = 'high'
            result['indicators'].append('cart_and_checkout')
        elif has_cart:
            # Has shopping cart = 100% sells online (even without checkout text)
            result['sells_online'] = True
            result['confidence'] = 'high'
            # shopping_cart_found already added to indicators above
        elif has_functional_buttons:
            result['sells_online'] = True
            result['confidence'] = 'high'
        
        # ===== BLOCKERS (In-store only indicators) =====
        # BUT: shopping_cart_found and cart_and_checkout override blockers (100% online)
        blockers = [
            'visit our store',
            'in-store only',
            'available in store',
            'call for availability',
            'contact us for pricing',
            'visit us',
            'come see us',
            'store locations',
            'find a store',
            'no online ordering',
            'physical store',
            'brick and mortar',
            'store hours',
            # Note: 'location' is too generic - removed as blocker
        ]
        
        blocker_found = False
        for blocker in blockers:
            if blocker in page_text:
                result['blockers'].append(blocker)
                blocker_found = True
                # Strong blockers override online indicators ONLY if we don't have cart
                # If shopping_cart_found or cart_and_checkout, ignore blockers
                if blocker in ['in-store only', 'no online ordering', 'call for availability']:
                    if result['sells_online'] and not has_cart:
                        result['sells_online'] = False
                        result['confidence'] = 'low'
                        result['indicators'].append(f'blocked_by:{blocker}')
        
        # If we have strong blockers but no functional buttons AND no cart, definitely not online
        if blocker_found and not has_functional_buttons and not has_cart:
            result['sells_online'] = False
            if result['confidence'] == 'high':
                result['confidence'] = 'medium'
            elif result['confidence'] == 'medium':
                result['confidence'] = 'low'
        
        # Final check: If no functional buttons AND no cart found, don't mark as online
        # (Even if text mentions exist, without buttons/cart it's not functional)
        if not has_functional_buttons and not has_cart:
            result['sells_online'] = False
            if result['confidence'] == 'high':
                result['confidence'] = 'medium'
        
        return result
        
    except Exception as e:
        result['indicators'].append(f'error:{str(e)[:30]}')
        return result


def check_url(url: str, page: Page, retries: int = 2) -> Dict:
    """
    Complete validation: Twisted X products + Online sales capability.
    
    Tracks redirects and checks final URL to avoid false positives from
    brand/informational sites that redirect to retailers.
    
    Args:
        url: URL to check
        page: Playwright page object
        retries: Number of retry attempts for failed navigations
        
    Returns:
        {
            'has_twisted_x': bool,
            'sells_online': bool,
            'combined_status': str,
            'twisted_x_method': str,
            'online_sales': Dict,
            'final_url': str,
            'redirected': bool,
            'error': str or None
        }
    """
    result = {
        'has_twisted_x': False,
        'sells_online': False,
        'sells_footwear': None,
        'combined_status': 'none',
        'twisted_x_method': None,
        'online_sales': {},
        'final_url': url,
        'redirected': False,
        'error': None
    }
    
    # Try navigation with retries and different strategies
    navigation_success = False
    last_error = None
    
    # Try different wait strategies (avoid networkidle - it often hangs on busy sites)
    wait_strategies = ['domcontentloaded', 'load', 'load']
    
    for attempt in range(retries + 1):
        strategy_idx = min(attempt, len(wait_strategies) - 1)
        wait_strategy = wait_strategies[strategy_idx]
        
        try:
            # Navigate to URL and track redirects
            # Increase timeout on retries
            timeout = VALIDATION_TIMEOUT + (attempt * 5000)
            
            page.goto(
                url, 
                timeout=timeout,
                wait_until=wait_strategy
            )
            
            # Additional wait for JS and redirects
            page.wait_for_timeout(3000)
            
            # Verify we actually loaded a page (not an error page)
            current_url = page.url
            if current_url and current_url != 'about:blank':
                navigation_success = True
                break
            else:
                last_error = 'Navigation resulted in blank page'
                if attempt < retries:
                    continue
        except PlaywrightTimeout as e:
            last_error = f'Timeout ({wait_strategy}, attempt {attempt + 1}/{retries + 1})'
            if attempt < retries:
                page.wait_for_timeout(2000)
                continue
        except Exception as e:
            error_msg = str(e)
            # Check if it's a DNS/network error that might be transient
            if any(err in error_msg for err in ['ERR_NAME_NOT_RESOLVED', 'net::', 'Navigation timeout', 'Timeout']):
                last_error = f'Network/Navigation error (attempt {attempt + 1}/{retries + 1}): {error_msg[:60]}'
                if attempt < retries:
                    page.wait_for_timeout(3000)  # Wait longer before retry for network errors
                    continue
            else:
                last_error = f'Error: {error_msg[:80]}'
                # Don't retry for non-network errors
                break
    
    if not navigation_success:
        result['error'] = last_error
        result['combined_status'] = 'error'
        return result
    
    try:
        
        # Get final URL after any redirects
        final_url = page.url
        result['final_url'] = final_url
        result['redirected'] = (final_url != url)
        
        # Check if this is a brand/informational site (not a retailer)
        # Brand sites often redirect or just show info without selling
        page_text = page.inner_text('body').lower()
        page_html = page.content().lower()
        
        is_brand_site = any([
            'twistedx.com' in final_url.lower() and 'twistedx.com' not in url.lower(),  # Only if redirected TO twistedx.com
            'twisted-x.com' in final_url.lower(),
            'find a retailer' in page_text,
            'where to buy' in page_text,
            'retailer locator' in page_text,
            'dealer locator' in page_text,
            'authorized dealer' in page_text,
            'find a store' in page_text,
        ])
        
        # Also check if it's the official Twisted X brand site
        is_official_brand = 'twistedx.com' in final_url.lower() or 'twisted-x.com' in final_url.lower()
        
        # Close any popups
        _close_popups(page)
        page.wait_for_timeout(500)  # Reduced wait

        # Early short-circuit: Check online sales on homepage first.
        # If site doesn't sell online, it can't sell footwear or Twisted X online — skip expensive checks.
        if not is_official_brand:
            quick_online_check = detect_online_sales_capability(page)
            if not quick_online_check['sells_online']:
                result['sells_online'] = False
                result['sells_footwear'] = False
                result['has_twisted_x'] = False
                result['online_sales'] = quick_online_check
                result['combined_status'] = 'no_products_no_online'
                try:
                    pt = page.inner_text('body').lower()
                    physical_phrases = [
                        'find a store', 'store locator', 'our locations', 'store locations',
                        'visit us', 'visit our store', 'find a location', 'store hours',
                        'locations', 'our stores'
                    ]
                    result['has_physical_store_indicators'] = any(p in pt for p in physical_phrases)
                except Exception:
                    result['has_physical_store_indicators'] = False
                return result
        
        # Check 1: Does it have Twisted X?
        # (This may navigate to search results or category pages)
        # Return the URL where products were found so we can check online sales there
        twisted_x_check = detect_twisted_x(page, final_url, return_page_info=True)
        result['has_twisted_x'] = twisted_x_check['has_products']
        result['twisted_x_method'] = twisted_x_check['method']
        found_on_url = twisted_x_check.get('found_on_url', final_url)
        if twisted_x_check['error']:
            result['error'] = twisted_x_check['error']
        
        # Check 2: Can you buy online?
        # Check on the page where we found Twisted X products (or homepage if found on homepage)
        # This is more accurate - if products are on search/category pages, check if those pages have purchase buttons
        try:
            # If we found products on a different page, check that page for online sales
            if found_on_url and found_on_url != final_url:
                page.goto(found_on_url, timeout=8000, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)  # Reduced wait
                _close_popups(page)
            else:
                # Found on homepage, make sure we're on homepage
                if page.url != final_url:
                    page.goto(final_url, timeout=8000, wait_until='domcontentloaded')
                    page.wait_for_timeout(2000)
                    _close_popups(page)
        except:
            pass  # If navigation fails, continue with current page
        
        # Check online sales capability on the page where products were found
        online_check = detect_online_sales_capability(page)
        result['online_sales'] = online_check
        
        # Special handling for official brand sites - they typically don't sell directly
        if is_official_brand:
            # Check if it actually has a functional online store (not just redirects/info)
            # Look for actual product listings with prices and purchase buttons
            has_product_listings = any([
                page.query_selector('div[class*="product"]') is not None,
                page.query_selector('[class*="product-grid"]') is not None,
                page.query_selector('[class*="product-list"]') is not None,
            ])
            
            # Check for actual working purchase buttons (not just text)
            has_working_buttons = False
            try:
                buttons = page.query_selector_all('button:has-text("Add to Cart"), a:has-text("Add to Cart"), button:has-text("Buy Now")')
                if buttons:
                    # Check if at least one is visible and clickable
                    for btn in buttons[:3]:  # Check first 3
                        if btn.is_visible():
                            has_working_buttons = True
                            break
            except:
                pass
            
            if not (has_product_listings and has_working_buttons):
                # Official brand site without functional store = likely just info/redirects
                result['sells_online'] = False
                result['online_sales']['confidence'] = 'low'
                result['online_sales']['blockers'].append('official brand site (no direct sales)')
            else:
                result['sells_online'] = online_check['sells_online']
        elif is_brand_site:
            # Other brand/info sites need STRONG evidence
            if online_check['confidence'] != 'high' or not online_check.get('indicators'):
                result['sells_online'] = False
                result['online_sales']['confidence'] = 'low'
                result['online_sales']['blockers'].append('brand/informational site')
            else:
                result['sells_online'] = online_check['sells_online']
        else:
            result['sells_online'] = online_check['sells_online']
        
        # Check 3: Does it sell footwear?
        try:
            from urllib.parse import urlparse
            parsed = urlparse(final_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            footwear_check = detect_footwear(page, base_url)
            result['sells_footwear'] = footwear_check.get('sells_footwear')
        except Exception:
            result['sells_footwear'] = None
        # If they don't sell online, they can't sell footwear online — avoid ? when detection failed
        if result['sells_online'] is False and result['sells_footwear'] is None:
            result['sells_footwear'] = False
        # Blind rule: Twisted X is a footwear brand — if they sell TX, they sell footwear
        if result['has_twisted_x'] is True:
            result['sells_footwear'] = True

        # Determine combined status
        if result['has_twisted_x'] and result['sells_online']:
            result['combined_status'] = 'has_products_sells_online'  # ✅ SCRAPE THIS
        elif result['has_twisted_x'] and not result['sells_online']:
            result['combined_status'] = 'has_products_in_store_only'  # ⚠️ Physical store or brand site
        elif not result['has_twisted_x'] and result['sells_online']:
            result['combined_status'] = 'ecommerce_no_twisted_x'  # ❌ No products
        else:
            result['combined_status'] = 'no_products_no_online'  # ❌ Skip

        # When no online sales, check for physical store indicators (for store_type inference)
        if result['combined_status'] == 'no_products_no_online':
            try:
                pt = page.inner_text('body').lower()
                physical_phrases = [
                    'find a store', 'store locator', 'our locations', 'store locations',
                    'visit us', 'visit our store', 'find a location', 'store hours',
                    'locations', 'our stores'
                ]
                result['has_physical_store_indicators'] = any(p in pt for p in physical_phrases)
            except Exception:
                result['has_physical_store_indicators'] = False
        else:
            result['has_physical_store_indicators'] = False

        return result
        
    except Exception as e:
        # If we got here, navigation succeeded but something else failed
        result['error'] = f'Error after navigation: {str(e)[:80]}'
        result['combined_status'] = 'error'
        return result


def validate_urls(input_csv: str, output_csv: str) -> Dict:
    """
    Validate all URLs from CSV and create filtered output.
    
    Args:
        input_csv: Path to input CSV with URLs
        output_csv: Path to output CSV for results
        
    Returns:
        Dictionary with statistics and results
    """
    # Read URLs from CSV
    urls = []
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get('Web Address', '').strip()
            if url:
                urls.append(url)
    
    print(f"\n{'='*60}")
    print(f"Enhanced URL Validator")
    print(f"Checking {len(urls)} URLs for Twisted X + Online Sales")
    print(f"{'='*60}\n")
    
    results = []
    stats = {
        'total': len(urls),
        'invalid': 0,
        'has_products_sells_online': 0,      # ✅ Scrape these
        'has_products_in_store_only': 0,     # ⚠️ Physical stores
        'ecommerce_no_twisted_x': 0,         # ❌ No products
        'no_products_no_online': 0,          # ❌ Skip
        'errors': 0,
    }
    
    # Process URLs with Playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        )
        Stealth().apply_stealth_sync(context)
        page = context.new_page()
        
        for i, url in enumerate(urls, 1):
            normalized = normalize_url(url)
            
            if not normalized:
                stats['invalid'] += 1
                results.append({
                    'original_url': url,
                    'normalized_url': None,
                    'status': 'invalid',
                    'has_twisted_x': False,
                    'sells_online': False,
                    'twisted_x_method': None,
                    'online_confidence': None,
                    'online_indicators': '',
                    'blockers': '',
                    'error': 'Invalid URL format'
                })
                print(f"[{i}/{stats['total']}] ❌ Invalid: {url[:50]}")
                continue
            
            print(f"[{i}/{stats['total']}] {normalized[:60]}...", end=' ', flush=True)
            
            check = check_url(normalized, page)
            
            status = check['combined_status']
            if status in stats:
                stats[status] += 1
            else:
                stats['errors'] += 1
            
            if check['error']:
                stats['errors'] += 1
            
            results.append({
                'original_url': url,
                'normalized_url': normalized,
                'final_url': check.get('final_url', normalized),
                'redirected': check.get('redirected', False),
                'status': status,
                'has_twisted_x': check['has_twisted_x'],
                'sells_online': check['sells_online'],
                'twisted_x_method': check['twisted_x_method'],
                'online_confidence': check['online_sales'].get('confidence'),
                'online_indicators': ', '.join(check['online_sales'].get('indicators', [])[:3]),
                'blockers': ', '.join(check['online_sales'].get('blockers', [])[:2]),
                'error': check['error'],
            })
            
            # Print status with redirect info
            redirect_info = f" (→ {check.get('final_url', '')[:40]})" if check.get('redirected') else ""
            if status == 'has_products_sells_online':
                print(f"✅ Twisted X + Online Sales{redirect_info}")
            elif status == 'has_products_in_store_only':
                print(f"⚠️  Twisted X (In-Store Only/Brand Site){redirect_info}")
            elif status == 'ecommerce_no_twisted_x':
                print(f"❌ E-commerce (No Twisted X){redirect_info}")
            elif check['error']:
                # Show full error message (not truncated)
                error_msg = check['error'][:100] if len(check['error']) > 100 else check['error']
                print(f"⚠️  Error: {error_msg}")
            else:
                print(f"❌ No products{redirect_info}")
        
        browser.close()
    
    # Write full results CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'original_url', 'normalized_url', 'final_url', 'redirected', 'status', 
            'has_twisted_x', 'sells_online', 'twisted_x_method',
            'online_confidence', 'online_indicators', 'blockers', 'error'
        ])
        writer.writeheader()
        writer.writerows(results)
    
    # Write filtered list (only online sellers with products)
    filtered_csv = output_csv.replace('.csv', '_filtered_online_only.csv')
    with open(filtered_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Web Address'])
        for r in results:
            if r['status'] == 'has_products_sells_online' and r['normalized_url']:
                writer.writerow([r['normalized_url']])
    
    # Print summary
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total URLs:                    {stats['total']}")
    print(f"Invalid URLs:                  {stats['invalid']}")
    print(f"\n✅ HAS PRODUCTS + SELLS ONLINE: {stats['has_products_sells_online']} (SCRAPE THESE)")
    print(f"⚠️  HAS PRODUCTS (In-Store Only): {stats['has_products_in_store_only']} (Physical stores)")
    print(f"❌ E-commerce (No Twisted X):    {stats['ecommerce_no_twisted_x']}")
    print(f"❌ No Products/No Online:        {stats['no_products_no_online']}")
    print(f"⚠️  Errors:                      {stats['errors']}")
    print(f"\n📄 Full results:  {output_csv}")
    print(f"📄 Filtered list: {filtered_csv} (only online sellers)")
    print(f"{'='*60}\n")
    
    return {**stats, 'results': results}


if __name__ == "__main__":
    import sys
    
    input_file = "data/CustomCustomerSearchResults990.csv"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"data/url_validation_{timestamp}.csv"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    validate_urls(input_file, output_file)

