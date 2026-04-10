"""
FastAPI server for Twisted X Scraper API

Rearchitected for Celigo integration:
- POST /api/scrape: Fetch product blocks from a URL (no LLM)
- POST /api/verify: Verify LLM-extracted products against source blocks
"""
import os
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from models import (
    ScrapeRequestNew, ScrapeResponse, ProductBlock,
    VerifyRequest, VerifyResponse,
    CheckRequest, CheckResponse
)
import cleaning


app = FastAPI(
    title="Twisted X Scraper API",
    description="Dumb fetcher + verifier API for Celigo integration (no LLM calls)",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Twisted X Scraper API (Celigo Rearchitecture)",
        "version": "2.0.0",
        "endpoints": {
            "health": "/health",
            "check": "/api/check (POST - quick yes/no: does this URL sell Twisted X?)",
            "scrape": "/api/scrape (POST - fetch product blocks, no LLM)",
            "verify": "/api/verify (POST - verify LLM-extracted products)",
            "retailers": "/api/retailers/urls (GET - list retailer URLs from CSV)"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/api/test")
async def test_endpoint():
    """Simple test endpoint to verify API is working"""
    return {"message": "API is working", "timestamp": datetime.now().isoformat()}


# =============================================================================
# POST /api/check - Quick Twisted X detection (yes/no + proof)
# =============================================================================

import re


def _check_brand_in_product_context(page) -> tuple:
    """
    Check if "Twisted X" appears in actual product context (product links, product cards, etc.).
    Used as fallback when SKU fingerprint finds no matches but retailer may use different SKU naming.

    Returns:
        (found: bool, samples: list of {name, product_url})
    """
    try:
        print(f"[check] _check_brand_in_product_context on: {page.url}")
        result = page.evaluate("""() => {
            const brandTerms = ['twisted x', 'twistedx', 'twisted-x'];
            const isBrand = (t) => brandTerms.some(term => t.includes(term));

            // Product link selectors: links that typically point to products
            const productLinkSelectors = [
                'a[href*="product"]', 'a[href*="item"]', 'a[href*="catalog"]',
                'a[href*="/p/"]', 'a[href*="/p-"]', 'a[href*="boot"]', 'a[href*="shoe"]'
            ];
            const productContainerSelectors = [
                '[class*="product"]', '[class*="product-card"]', '[class*="item-cell"]',
                '[class*="product-tile"]', '[class*="product-item"]', '[class*="plp-product"]'
            ];

            const results = [];
            const seen = new Set();

            // Check product links
            for (const sel of productLinkSelectors) {
                const links = document.querySelectorAll(sel);
                for (const link of links) {
                    const text = (link.textContent || '').trim();
                    if (!text || text.length < 10 || text.length > 300) continue;
                    const lower = text.toLowerCase();
                    if (!isBrand(lower)) continue;
                    const key = text.slice(0, 50).toLowerCase();
                    if (seen.has(key)) continue;
                    seen.add(key);
                    results.push({ name: text, product_url: link.href || '' });
                    if (results.length >= 5) return results;
                }
            }

            // Check product containers (text inside product cards/tiles)
            for (const sel of productContainerSelectors) {
                const els = document.querySelectorAll(sel);
                for (const el of els) {
                    const text = (el.textContent || '').trim();
                    if (!text || text.length < 10 || text.length > 400) continue;
                    const lower = text.toLowerCase();
                    if (!isBrand(lower)) continue;
                    const key = text.slice(0, 50).toLowerCase();
                    if (seen.has(key)) continue;
                    seen.add(key);
                    const link = el.querySelector('a[href]');
                    results.push({ name: text.split('\\n')[0].trim(), product_url: link ? link.href : '' });
                    if (results.length >= 5) return results;
                }
            }

            return results;
        }""")
        print(f"[check] _check_brand_in_product_context: {len(result)} product(s) with brand in context")
        return (len(result) > 0, result[:5])
    except Exception as e:
        print(f"[check] _check_brand_in_product_context error: {e}")
        return (False, [])


def _scan_for_style_codes(page) -> dict:
    """
    Scan page text, HTML, and product URLs for known Twisted X style codes.
    Returns dict with matched_codes (set), matched_in (list of where found),
    and sample_products (list of products with matched SKUs).
    """
    from config import TX_STYLE_CODES

    if not TX_STYLE_CODES:
        return {"matched_codes": set(), "matched_in": [], "sample_products": []}

    matched_codes = set()
    matched_in = []

    # Single-pass token scan: extract all tokens from page, match against code set
    try:
        page_text = page.inner_text('body')
        page_html = page.content()
    except Exception:
        return {"matched_codes": set(), "matched_in": [], "sample_products": []}

    # Extract alphanumeric tokens (style codes are 4-15 chars like WDM0093, BACKPKECO001)
    token_re = re.compile(r'[A-Za-z0-9]{4,15}')
    text_tokens = set(t.upper() for t in token_re.findall(page_text))
    html_tokens = set(t.upper() for t in token_re.findall(page_html))
    all_tokens = text_tokens | html_tokens

    matched_codes = TX_STYLE_CODES & all_tokens

    if not matched_codes:
        return {"matched_codes": set(), "matched_in": [], "sample_products": []}

    # Determine WHERE we found the codes for proof
    for code in sorted(matched_codes)[:10]:
        if code in text_tokens:
            matched_in.append(f"{code} in page text")
        elif code in html_tokens:
            matched_in.append(f"{code} in page HTML/URLs")

    # Extract sample products that contain matched style codes
    sample_products = []
    try:
        # Get all links and find ones containing matched codes
        links = page.query_selector_all('a[href]')
        seen = set()
        code_pattern = re.compile('|'.join(re.escape(c) for c in matched_codes), re.IGNORECASE)

        for link in links:
            try:
                href = link.get_attribute('href') or ""
                text = link.inner_text().strip()
                full = text + " " + href

                if not code_pattern.search(full):
                    continue
                if len(text) < 5 or len(text) > 300:
                    continue

                # Find which code matched
                code_match = code_pattern.search(full)
                sku = code_match.group().upper() if code_match else ""

                # Deduplicate by SKU
                if sku in seen:
                    continue
                seen.add(sku)

                # Parse product name (skip junk lines)
                lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 3]
                name = ""
                price = "N/A"
                for l in lines:
                    ll = l.lower()
                    if any(w in ll for w in ['filter', 'sort', 'search', 'results for', 'shop all']):
                        continue
                    if '$' in l or 'see price' in ll:
                        price = l.strip()
                        continue
                    if ll in ['twisted x', 'twistedx']:
                        continue
                    if not name and len(l) > 5:
                        name = l
                if not name:
                    name = sku

                # Get image from parent container
                img = ""
                try:
                    parent = link.evaluate_handle("el => el.closest('li, div, article, section')")
                    if parent:
                        img = parent.evaluate("""el => {
                            const i = el.querySelector('img');
                            return i ? (i.src || '') : '';
                        }""") or ""
                except Exception:
                    pass

                sample_products.append({
                    "name": name,
                    "price": price,
                    "sku": sku,
                    "image": img,
                    "product_url": href,
                })
                if len(sample_products) >= 5:
                    break
            except Exception:
                continue
    except Exception:
        pass

    # If no link-based samples, try text-based extraction near matched codes
    if not sample_products:
        try:
            lines = [l.strip() for l in page_text.split('\n') if l.strip()]
            for i, line in enumerate(lines):
                for code in list(matched_codes)[:20]:
                    if code.lower() in line.lower():
                        name = ""
                        price = "N/A"
                        # Look at surrounding lines for product info
                        window = lines[max(0, i-2):min(len(lines), i+4)]
                        for wl in window:
                            wll = wl.lower()
                            if '$' in wl:
                                price = wl.strip()
                            elif len(wl) > 10 and not any(w in wll for w in ['filter', 'sort', 'search']):
                                if not name:
                                    name = wl.strip()
                        if not name:
                            name = code
                        sample_products.append({
                            "name": name, "price": price, "sku": code,
                            "image": "", "product_url": "",
                        })
                        break
                if len(sample_products) >= 5:
                    break
        except Exception:
            pass

    return {
        "matched_codes": matched_codes,
        "matched_in": matched_in[:5],
        "sample_products": sample_products[:5],
    }


def _detect_blocked(page) -> tuple:
    """
    Detect if the site is blocking automated access (CAPTCHA, bot detection, etc.).
    Uses stronger patterns to avoid false positives (e.g. "powered by cloudflare", "unblock").
    Returns (is_blocked: bool, reasons: List[str]).
    """
    reasons = []
    try:
        text = page.inner_text("body").lower()
        html = page.content().lower()
        url = page.url.lower()
        combined = text + " " + html + " " + url

        # Exclude benign contexts that would cause false positives
        if "powered by cloudflare" in combined or "cloudflare cdn" in combined:
            pass  # Don't add Cloudflare reason for CDN badges
        elif "cloudflare" in combined and any(
            phrase in combined for phrase in [
                "checking your browser", "please wait", "enable javascript",
                "please enable javascript", "verify you are", "security check",
                "checking if the site connection"
            ]
        ):
            reasons.append("Cloudflare challenge")

        # Strong indicators - use specific phrases to avoid false positives
        strong_indicators = [
            ("access denied", "Access denied"),
            ("forbidden", "Forbidden"),
            ("bot detected", "Bot detected"),
            ("unusual traffic", "Unusual traffic"),
            ("please enable javascript", "JavaScript required / blocking"),
            ("verify you are human", "Human verification"),
            ("you have been blocked", "Blocked"),
            ("access blocked", "Blocked"),
            ("request blocked", "Blocked"),
            ("we have blocked", "Blocked"),
            ("your access has been blocked", "Blocked"),
            ("complete the security check", "Security check"),
            ("px-show", "Bot verification page (e.g. Demandware)"),
        ]
        for indicator, label in strong_indicators:
            if indicator in combined and label not in reasons:
                reasons.append(label)

        # Exclude: "blocked" alone can be in "unblock", "blocked popup", cookie banners
        # Only add generic "blocked" if we haven't already added it from stronger phrases
        if "Blocked" not in reasons and any(
            phrase in combined for phrase in [
                "you have been blocked", "automated access", "blocked automated",
                "blocked the request", "blocked for security"
            ]
        ):
            reasons.append("Blocked")

        # Reinforce: minimal visible content often indicates block/challenge page
        # Only use when we already have other blocking signals (don't block minimal but valid sites)
        MIN_VISIBLE_TEXT = 500  # Block pages usually have < 300 chars; real sites have 1000+
        if len(text) < MIN_VISIBLE_TEXT and len(reasons) > 0:
            reasons.append("Minimal content")
    except Exception:
        pass
    return (len(reasons) > 0, reasons)


def _detect_search_pattern(page, platform: str, base_url: str) -> List[str]:
    """
    Detect the actual search URL pattern(s) used by the site.
    Returns a list of search URLs to try, ordered by likelihood.
    """
    from urllib.parse import quote_plus
    search_term = quote_plus("Twisted X")
    search_term_encoded = quote_plus("Twisted X", safe='')  # Double-encoded for some sites
    
    patterns = []
    
    if platform == 'shopify':
        # Try to detect from search form or input field
        try:
            # Check search form action and input name
            search_info = page.evaluate("""() => {
                const form = document.querySelector('form[action*="search"], form[action*="/search"]');
                const input = document.querySelector('input[name*="search"], input[name*="q"], input[name*="term"], input[type="search"]');
    
    return {
                    formAction: form ? form.action : null,
                    inputName: input ? input.name : null,
                    inputValue: input ? input.value : null
                };
            }""")
            
            # If we found a custom pattern, try it first
            if search_info and search_info.get('inputName'):
                input_name = search_info['inputName']
                if 'searchTerm' in input_name.lower() or 'searchterm' in input_name.lower():
                    patterns.append(f"{base_url}/search?searchTerm={search_term}")
                    patterns.append(f"{base_url}/search?searchTerm={search_term_encoded}")
        except:
            pass
        
        # Standard Shopify patterns (try both common variations)
        patterns.extend([
            f"{base_url}/search?searchTerm={search_term}",      # Custom pattern (Rural King style)
            f"{base_url}/search?searchTerm={search_term_encoded}",  # Double-encoded
            f"{base_url}/search?q={search_term}",               # Standard Shopify
            f"{base_url}/search?type=product&q={search_term}",  # Shopify product-only search (e.g. masonbrothersshoes.com)
        ])
    
    elif platform == 'woocommerce':
        patterns.extend([
            f"{base_url}/?s={search_term}&post_type=product",           # Standard WooCommerce
            f"{base_url}/searchPage.action?keyWord={search_term}",      # Java/Struts (e.g. stockdales.com)
        ])
    
    # Remove duplicates while preserving order
    seen = set()
    unique_patterns = []
    for p in patterns:
        if p not in seen:
            seen.add(p)
            unique_patterns.append(p)
    
    return unique_patterns


def _detect_platform(page) -> str:
    """
    Detect e-commerce platform by analyzing page HTML and JavaScript context.
    
    Returns:
        'shopify': Shopify store detected
        'woocommerce': WordPress/WooCommerce detected
        'normal': Standard website (unknown platform)
    """
    try:
        html = page.content().lower()
        url = page.url.lower()
        
        # Shopify indicators (check HTML, URL, and JS context)
        shopify_indicators = [
            'shopify.theme',
            'cdn.shopify.com',
            'shopify-section',
            'shopify-product',
            'shopify.shop',
            'myshopify.com',
            'shopifycdn',
            'shopify-api',
            'shopify-checkout',
        ]
        if any(indicator in html for indicator in shopify_indicators):
            return 'shopify'
        
        # More lenient check: look for "shopify" in class names, IDs, or script sources
        # (but avoid false positives from words containing "shopify")
        if 'shopify' in html:
            # Check if it's in a meaningful context (class, id, src, etc.)
            shopify_contexts = [
                'class="shopify',
                'id="shopify',
                'src="shopify',
                'data-shopify',
                '-shopify-',
                '.shopify',
            ]
            if any(ctx in html for ctx in shopify_contexts):
                return 'shopify'
        
        # Check JavaScript context for Shopify
        try:
            shopify_check = page.evaluate("""() => {
                return typeof Shopify !== 'undefined' || 
                       (window.Shopify && window.Shopify.theme) ||
                       document.querySelector('[data-shopify]') !== null;
            }""")
            if shopify_check:
                return 'shopify'
        except:
            pass
        
        # NetSuite/SuiteCommerce indicators (check before WooCommerce - catalog-style sites)
        netsuite_indicators = [
            'netsuite',
            'suitecommerce',
            'suite-commerce',
            '/catalog/productsearch',
            'nlapi',
            'sc.analytics',
        ]
        if any(indicator in html for indicator in netsuite_indicators):
            return 'netsuite'
        if '/catalog/' in url:
            return 'netsuite'

        # WooCommerce/WordPress indicators
        woocommerce_indicators = [
            'woocommerce',
            'wp-content',
            'wp-includes',
            'wp-json',
            'wordpress',
            'wc-product',
            'woocommerce-product-search',
            '/wp-content/themes/',
        ]
        if any(indicator in html for indicator in woocommerce_indicators):
            return 'woocommerce'
        
        # Check URL patterns
        if 'myshopify.com' in url:
            return 'shopify'
        
        return 'normal'
    except Exception:
        return 'normal'


def _check_url_sync(url: str) -> dict:
    """
    Quick check: does this URL sell Twisted X?

    Dual verification:
    1. URL validator (search + detection)
    2. SKU fingerprint scan (3025 known style codes)
    Both must agree for high confidence. SKU match alone = definitive proof.
    
    Platform-aware search strategy:
    - Shopify/WooCommerce: Try standard URL patterns first, then fall back to Playwright search bar interaction
    - Normal sites: Use Playwright search bar interaction first, then fall back to URL patterns
    """
    from url_validator import check_url as validate_url, normalize_url
    from config import HEADLESS, get_retailer_name, TX_STYLE_CODES
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    normalized = normalize_url(url)
    if not normalized:
        return {
            "url": url,
            "retailer": "unknown",
            "sells_twisted_x": False,
            "sells_footwear": None,
            "confidence": "low",
            "store_type": "unknown",
            "sells_online": False,
            "proof": [],
            "sample_products": [],
            "page_url": None,
            "checked_at": datetime.now().isoformat(),
            "error": "Invalid URL format",
            "blocked": False,
        }

    retailer_name = get_retailer_name(normalized)

    result = {
        "url": normalized,
        "retailer": retailer_name,
        "sells_twisted_x": False,
        "sells_footwear": None,
        "confidence": "low",
        "store_type": "unknown",
        "sells_online": False,
        "proof": [],
        "sample_products": [],
        "page_url": None,
        "checked_at": datetime.now().isoformat(),
        "error": None,
        "blocked": False,
    }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )
            Stealth().apply_stealth_sync(context)
            page = context.new_page()

            try:
                print(f"\n[check] Checking: {retailer_name} ({normalized})")

                # ══════════════════════════════════════════════════════
                # STEP 1: Detect platform and perform platform-aware search
                # ══════════════════════════════════════════════════════
                page.goto(normalized, timeout=15000, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)
                
                # Check if site shows blocking indicators (but continue scanning - if we find SKUs, we clearly weren't blocked)
                is_blocked, blocked_reasons = _detect_blocked(page)
                
                platform = _detect_platform(page)
                print(f"[check] Detected platform: {platform}")

                early_sku_scan = None
                early_sku_matched = False
                early_brand_found = False
                early_brand_samples = []
                early_brand_page_url = None
                from urllib.parse import urlparse
                base = urlparse(normalized)
                base_url = f"{base.scheme}://{base.netloc}"

                def _sku_and_brand_on_page():
                    """Run both SKU scan and brand check; return (sku_scan, sku_matched, brand_found, brand_samples)."""
                    s = _scan_for_style_codes(page)
                    sm = len(s["matched_codes"]) > 0
                    bf, bs = (False, [])
                    if not sm:
                        bf, bs = _check_brand_in_product_context(page)
                    return s, sm, bf, bs

                if platform == 'netsuite':
                    # NetSuite/SuiteCommerce: Use ONLY /catalog/productsearch (no Playwright, no fallbacks)
                    search_url = f"{base_url}/catalog/productsearch"
                    print(f"[check] Using NetSuite catalog URL (only): {search_url}")
                    try:
                        page.goto(search_url, timeout=20000, wait_until='domcontentloaded')
                        page.wait_for_timeout(3500)
                        # Scroll to trigger lazy-loaded catalog content
                        try:
                            for _ in range(3):
                                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                page.wait_for_timeout(800)
                        except Exception:
                            pass
                        ftxt = page.inner_text('body').lower()
                        # Always run SKU + brand check when we have content (don't skip on error URL)
                        if len(ftxt) > 200:
                            early_sku_scan, early_sku_matched, early_brand_found, early_brand_samples = _sku_and_brand_on_page()
                            if early_brand_found:
                                early_brand_page_url = page.url
                            print(f"[check] NetSuite catalog: SKU={len(early_sku_scan['matched_codes'])} codes, brand={early_brand_found}")
                        else:
                            print(f"[check] NetSuite catalog: insufficient content (len={len(ftxt)})")
                    except Exception as e:
                        print(f"[check] NetSuite catalog failed: {e}")

                elif platform in ['shopify', 'woocommerce'] and not early_sku_matched and not early_brand_found:
                    # Shopify/WooCommerce: Detect and use actual search URL patterns
                    print(f"[check] Using standard URL patterns for {platform}")
                    search_urls = _detect_search_pattern(page, platform, base_url)
                    print(f"[check] Detected {len(search_urls)} search pattern(s) to try")
                    
                    for search_url in search_urls:
                        try:
                            print(f"[check] Trying {platform} search URL: {search_url}")
                            page.goto(search_url, timeout=20000, wait_until='domcontentloaded')
                            try:
                                page.wait_for_load_state('load', timeout=8000)
                            except Exception:
                                pass
                            
                            # Smart wait: poll until content appears (max 8s)
                            ftxt = ""
                            for wait_round in range(4):
                                page.wait_for_timeout(2000)
                                ftxt = page.inner_text('body').lower()
                                if len(ftxt) > 500:
                                    break
                            print(f"[check] Search page text_len={len(ftxt)}")
                            
                            no_results = any(p in ftxt for p in [
                                'no results', 'no products found', 'nothing found',
                                '0 results', 'no items found',
                            ])
                            has_content = len(ftxt) > 200 and not no_results
                            
                            if has_content:
                                print(f"[check] {platform} search returned content, running SKU + brand check...")
                                early_sku_scan, early_sku_matched, early_brand_found, early_brand_samples = _sku_and_brand_on_page()
                                if early_brand_found:
                                    early_brand_page_url = page.url
                                print(f"[check] SKU scan: {len(early_sku_scan['matched_codes'])} codes, brand: {early_brand_found}")
                                if early_sku_matched or early_brand_found:
                                    break
                        except Exception as e:
                            print(f"[check] {platform} search failed: {e}")
                    
                    # Fallback: If URL patterns didn't find SKUs/brand, try Playwright search bar interaction
                    if not early_sku_matched and not early_brand_found:
                        print(f"[check] {platform} URL patterns didn't find SKUs, trying Playwright search as fallback")
                        import url_validator
                        
                        for search_term in ['Twisted X', 'TwistedX']:
                            page.goto(normalized, timeout=15000, wait_until='domcontentloaded')
                            page.wait_for_timeout(1000)
                            
                            if url_validator._search_on_site(page, search_term):
                                page.wait_for_timeout(3000)
                                print(f"[check] Playwright fallback search succeeded with '{search_term}'")
                                
                                # Check if we got results
                                ftxt = page.inner_text('body').lower()
                                if len(ftxt) > 200:
                                    early_sku_scan, early_sku_matched, early_brand_found, early_brand_samples = _sku_and_brand_on_page()
                                    if early_brand_found:
                                        early_brand_page_url = page.url
                                    print(f"[check] SKU scan after Playwright fallback: {len(early_sku_scan['matched_codes'])} codes, brand: {early_brand_found}")
                                    if early_sku_matched or early_brand_found:
                                        break

                else:  # normal site (not NetSuite, not Shopify/WooCommerce)
                    # Normal sites: Try Playwright search bar interaction first
                    print("[check] Using Playwright search bar interaction for normal site")
                    import url_validator
                    
                    search_worked = False
                    for search_term in ['Twisted X', 'TwistedX']:
                        page.goto(normalized, timeout=15000, wait_until='domcontentloaded')
                        page.wait_for_timeout(1000)
                        
                        if url_validator._search_on_site(page, search_term):
                            page.wait_for_timeout(3000)
                            print(f"[check] Playwright search succeeded with '{search_term}'")
                            search_worked = True
                            
                            # Check if we got results
                            ftxt = page.inner_text('body').lower()
                            if len(ftxt) > 200:
                                early_sku_scan, early_sku_matched, early_brand_found, early_brand_samples = _sku_and_brand_on_page()
                                if early_brand_found:
                                    early_brand_page_url = page.url
                                print(f"[check] SKU scan after Playwright search: {len(early_sku_scan['matched_codes'])} codes, brand: {early_brand_found}")
                                if early_sku_matched or early_brand_found:
                                    break
                    
                    # Fallback: If Playwright search failed, try standard URL patterns
                    if not search_worked or (not early_sku_matched and not early_brand_found):
                        print("[check] Playwright search failed/empty, trying standard URL patterns as fallback")
                        # Normal sites: generic search URLs (NetSuite uses only catalog in its block)
                        search_urls = [
                            f"{base_url}/search?q=Twisted+X",               # Generic search
                            f"{base_url}/search?type=product&q=Twisted+X",  # Shopify product-only search
                            f"{base_url}/searchPage.action?keyWord=Twisted+X",  # Java/Struts (e.g. stockdales.com)
                            f"{base_url}/catalog/productsearch",            # Catalog-style
                            f"{base_url}/?s=Twisted+X&post_type=product",   # WordPress/WooCommerce
                            f"{base_url}/?s=Twisted+X",                     # WordPress generic
                        ]
                        for search_url in search_urls:
                            try:
                                print(f"[check] Fallback URL: {search_url}")
                                page.goto(search_url, timeout=15000, wait_until='domcontentloaded')
                                page.wait_for_timeout(3500)
                                # Scroll to trigger lazy-loaded content (NetSuite, etc.)
                                try:
                                    for _ in range(3):
                                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                        page.wait_for_timeout(800)
                                except Exception:
                                    pass
                                ftxt = page.inner_text('body').lower()
                                if len(ftxt) > 200 and ('error' not in page.url and 'notfound' not in page.url.lower()):
                                    early_sku_scan, early_sku_matched, early_brand_found, early_brand_samples = _sku_and_brand_on_page()
                                    if early_brand_found:
                                        early_brand_page_url = page.url
                                    if early_sku_matched:
                                        print(f"[check] Fallback URL found SKUs: {len(early_sku_scan['matched_codes'])} codes")
                                        break
                                    if early_brand_found:
                                        print(f"[check] Fallback URL found brand in product context: {len(early_brand_samples)} samples")
                                        break
                            except Exception:
                                continue

                # ══════════════════════════════════════════════════════
                # STEP 2: Run full validator (navigation, search, brand detection)
                # ══════════════════════════════════════════════════════
                page.goto(normalized, timeout=15000, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)
                validation = validate_url(normalized, page)

                # Core answer
                result["sells_twisted_x"] = validation["has_twisted_x"]
                result["sells_online"] = validation["sells_online"]
                result["sells_footwear"] = validation.get("sells_footwear")

                # Store type
                combined = validation.get("combined_status", "")
                final_url = validation.get("final_url", normalized)

                is_brand_site = (
                    "twistedx.com" in final_url.lower() or
                    "twisted-x.com" in final_url.lower()
                )
                if is_brand_site:
                    result["store_type"] = "brand_site"
                elif combined == "has_products_sells_online":
                    result["store_type"] = "ecommerce"
                elif combined == "has_products_in_store_only":
                    result["store_type"] = "company_store"
                elif combined == "ecommerce_no_twisted_x":
                    result["store_type"] = "ecommerce"
                elif combined == "no_products_no_online" and validation.get("has_physical_store_indicators"):
                    result["store_type"] = "company_store"
                else:
                    result["store_type"] = "unknown"

                # Build proof
                proof = []

                # Note: Blocked detection already happened early (before scanning), so we skip here

                # ══════════════════════════════════════════════════════
                # STEP 3: SKU Fingerprint — use early scan or do fresh scan
                # When early_sku_matched is False: check BOTH product page AND homepage
                # ══════════════════════════════════════════════════════
                brand_found_dual_check = False
                brand_samples_dual_check = []

                if early_sku_matched and early_sku_scan:
                    sku_scan = early_sku_scan
                    sku_matched = True
                    print(f"[check] Using early SKU scan: {len(sku_scan['matched_codes'])} codes")
                elif early_brand_found and early_brand_samples:
                    # Found via brand in product context during Step 1 (fallback URL, etc.)
                    result["sells_twisted_x"] = True
                    result["confidence"] = "high"
                    result["page_url"] = early_brand_page_url or page.url
                    result["sample_products"] = early_brand_samples[:5]
                    proof.append("VERIFIED: Twisted X found in product context (brand name in product links/cards)")
                    proof.append(f"Brand-in-product match: {len(early_brand_samples)} product(s) contain 'Twisted X'")
                    for i, sp in enumerate(early_brand_samples[:5], 1):
                        name = sp.get("name", "?")[:80]
                        proof.append(f"  {i}. {name}")
                    proof.append(f"Found on: {early_brand_page_url or page.url}")
                    sku_scan = {"matched_codes": set(), "matched_in": [], "sample_products": []}
                    sku_matched = False
                    # Add ecommerce signals if available (validation runs before this)
                    online_info = validation.get("online_sales", {})
                    indicators = online_info.get("indicators", [])
                    if indicators:
                        proof.append(f"E-commerce signals: {', '.join(indicators[:5])}")
                else:
                    # Navigate to product page, then run SKU + brand check on both product page and homepage
                    sku_scan = {"matched_codes": set(), "matched_in": [], "sample_products": []}
                    sku_matched = False

                    def _run_sku_and_brand_check():
                        s = _scan_for_style_codes(page)
                        sm = len(s["matched_codes"]) > 0
                        bf, bs = _check_brand_in_product_context(page) if not sm else (False, [])
                        return s, sm, bf, bs

                    # ── Check 1: Product/search page ──
                    print("[check] Navigating to product/search page for SKU + brand check...")
                    if platform == 'netsuite':
                        # NetSuite: use ONLY catalog/productsearch (no Playwright fallback)
                        product_url = f"{base_url}/catalog/productsearch"
                        try:
                            page.goto(product_url, timeout=20000, wait_until='domcontentloaded')
                            page.wait_for_timeout(3500)
                            for _ in range(3):
                                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                page.wait_for_timeout(800)
                        except Exception as e:
                            print(f"[check] NetSuite catalog navigation failed: {e}")
                    elif platform == 'shopify':
                        product_url = f"{base_url}/search?q=Twisted+X"
                        try:
                            page.goto(product_url, timeout=15000, wait_until='domcontentloaded')
                            page.wait_for_timeout(3500)
                        except Exception as e:
                            print(f"[check] Shopify search navigation failed: {e}")
                    elif platform == 'woocommerce':
                        product_url = f"{base_url}/?s=Twisted+X&post_type=product"
                        try:
                            page.goto(product_url, timeout=15000, wait_until='domcontentloaded')
                            page.wait_for_timeout(3500)
                        except Exception as e:
                            print(f"[check] WooCommerce search navigation failed: {e}")
                    else:
                        # Normal: use Playwright search from homepage
                        try:
                            page.goto(normalized, timeout=15000, wait_until='domcontentloaded')
                            page.wait_for_timeout(1500)
                            import url_validator
                            if url_validator._search_on_site(page, 'Twisted X'):
                                page.wait_for_timeout(4000)
                                for _ in range(2):
                                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                    page.wait_for_timeout(1000)
                        except Exception as e:
                            print(f"[check] Playwright search failed: {e}")

                    print(f"[check] Product page URL: {page.url}")
                    sku_scan, sku_matched, brand_found_dual_check, brand_samples_dual_check = _run_sku_and_brand_check()
                    if sku_matched:
                        print(f"[check] SKU match on product page: {len(sku_scan['matched_codes'])} codes")
                    elif brand_found_dual_check:
                        print(f"[check] Brand in product context on product page: {len(brand_samples_dual_check)} samples")

                    # ── Check 2: Homepage (if not found on product page) ──
                    if not sku_matched and not brand_found_dual_check:
                        print("[check] No match on product page, checking homepage...")
                        try:
                            page.goto(normalized, timeout=15000, wait_until='domcontentloaded')
                            page.wait_for_timeout(3000)
                            sku_scan, sku_matched, bf, bs = _run_sku_and_brand_check()
                            if bf and bs:
                                brand_found_dual_check = True
                                brand_samples_dual_check = bs
                            if sku_matched:
                                print(f"[check] SKU match on homepage: {len(sku_scan['matched_codes'])} codes")
                            elif brand_found_dual_check:
                                print(f"[check] Brand in product context on homepage: {len(brand_samples_dual_check)} samples")
                        except Exception as e:
                            print(f"[check] Homepage check failed: {e}")

                if validation["has_twisted_x"] or sku_matched:
                    result["page_url"] = page.url

                    # ── Determine final verdict using dual verification ──
                    # SKU match = definitive proof (fingerprint)
                    # Name match verified below as secondary signal
                    sku_codes_found = sorted(sku_scan["matched_codes"])[:10]
                    sku_samples = sku_scan["sample_products"]

                    if sku_matched:
                        # SKU MATCH: This is definitive — style codes are fingerprints
                        result["sells_twisted_x"] = True
                        result["confidence"] = "high"

                        proof.append(f"VERIFIED: {len(sku_codes_found)} Twisted X style code(s) found on page")
                        proof.append(f"Matched SKUs: {', '.join(sku_codes_found[:8])}")
                        for loc in sku_scan["matched_in"][:3]:
                            proof.append(f"  Found: {loc}")
                        proof.append(f"Search page: {page.url}")

                        # Combine: SKU-based samples + text-based product extraction
                        all_samples = sku_samples[:]
                        if len(all_samples) < 5:
                            try:
                                text_products = page.evaluate("""() => {
                                    const body = document.body.innerText;
                                    const lines = body.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                                    const products = [];
                                    const seen = new Set();
                                    const skipRe = /filter|sort|search result|showing|category|results for|page \\d|\\d+ items/i;
                                    
                                    for (let i = 0; i < lines.length; i++) {
                                        const lower = lines[i].toLowerCase();
                                        const isBrand = (lower === 'twisted x' || lower === 'twistedx');
                                        
                                        if (!isBrand) continue;
                                        if (i + 1 >= lines.length) continue;
                                        
                                        // Brand line → next line should be product name
                                        const nameLine = lines[i + 1].trim();
                                        if (nameLine.length < 5 || skipRe.test(nameLine)) continue;
                                        // Skip count/filter lines like "(426)" or "Twisted X (426)"
                                        if (/^\\(\\d+\\)$/.test(nameLine)) continue;
                                        if (/^\\d+$/.test(nameLine)) continue;
                                        
                                        // Skip if name is just a count or filter remnant
                                        if (/^[\\(\\d\\)\\s]+$/.test(nameLine)) continue;
                                        if (nameLine.length < 8) continue;
                                        
                                        const fullName = 'Twisted X ' + nameLine;
                                        const key = fullName.slice(0, 40).toLowerCase();
                                        if (seen.has(key)) continue;
                                        seen.add(key);
                                        
                                        // Look for price in next few lines
                                        let price = 'N/A';
                                        for (let j = i + 1; j < Math.min(i + 5, lines.length); j++) {
                                            if (lines[j].includes('$') && !lines[j].match(/^\\$\\d+-\\$\\d+/)) {
                                                price = lines[j].trim(); break;
                                            }
                                            if (lines[j].toLowerCase().includes('see price')) {
                                                price = lines[j].trim(); break;
                                            }
                                        }
                                        
                                        products.push({name: fullName, price, sku: '', image: '', product_url: ''});
                                        if (products.length >= 5) break;
                                    }
                                    return products;
                                }""")
                                existing = {s.get("name", "").lower()[:30] for s in all_samples}
                                for tp in text_products:
                                    if tp["name"].lower()[:30] not in existing:
                                        all_samples.append(tp)
                                    if len(all_samples) >= 5:
                                        break
                            except Exception:
                                pass

                        result["sample_products"] = all_samples[:5]

                        if all_samples:
                            proof.append("Sample Twisted X products on page:")
                            for i, sp in enumerate(all_samples[:5], 1):
                                name = sp.get("name", "Unknown")[:70]
                                price = sp.get("price", "N/A")
                                sku = sp.get("sku", "")
                                prefix = f"[{sku}] " if sku else ""
                                proof.append(f"  {i}. {prefix}{name} — {price}")
                    else:
                        # Validator said yes, but NO SKU match — needs name verification
                        method = validation.get("twisted_x_method", "unknown")
                        proof.append(f"Initial detection via: {method}")
                        proof.append(f"SKU scan: No Twisted X style codes found in page")
                        proof.append(f"Search page: {page.url}")

                        # Try to extract products and check names as fallback
                        try:
                            name_samples = page.evaluate("""() => {
                                const results = [];
                                const links = document.querySelectorAll('a[href]');
                                const seen = new Set();
                                for (const link of links) {
                                    const text = link.textContent.trim().toLowerCase();
                                    if (!text.includes('twisted x') && !text.includes('twistedx')) continue;
                                    if (text.length < 10 || text.length > 200) continue;
                                    const key = text.slice(0, 40);
                                    if (seen.has(key)) continue;
                                    seen.add(key);
                                    const href = link.href || '';
                                    results.push({ name: link.textContent.trim(), product_url: href });
                                    if (results.length >= 5) break;
                                }
                                return results;
                            }""")
                        except Exception:
                            name_samples = []

                        if name_samples:
                            # Found products with "Twisted X" in the name
                            result["sells_twisted_x"] = True
                            result["confidence"] = "medium"
                            result["sample_products"] = name_samples[:5]

                            proof.append(f"Name match: {len(name_samples)} product(s) contain 'Twisted X' in name")
                            for i, sp in enumerate(name_samples[:5], 1):
                                name = sp.get("name", "?")[:80]
                                proof.append(f"  {i}. {name}")
                            proof.append("Note: No SKU fingerprint match — confidence medium")
                        elif early_brand_found and early_brand_samples:
                            # Already verified via brand-in-product-context in Step 1 (e.g. searchPage.action)
                            # Don't overwrite with false positive — proof already added in Step 3 early_brand block
                            pass
                        else:
                            # No SKU match AND no name match → false positive
                            result["sells_twisted_x"] = False
                            result["confidence"] = "high"
                            proof.insert(0, "No Twisted X products found on this site")
                            proof.append("VERIFICATION FAILED: Validator flagged site but no SKU codes or product names match Twisted X")

                    # Online sales evidence
                    online_info = validation.get("online_sales", {})
                    indicators = online_info.get("indicators", [])
                    if indicators:
                        proof.append(f"E-commerce signals: {', '.join(indicators[:5])}")

                    blockers = online_info.get("blockers", [])
                    if blockers:
                        proof.append(f"Offline signals: {', '.join(blockers[:3])}")

                    online_conf = online_info.get("confidence", "low")
                    proof.append(f"Online sales confidence: {online_conf}")
                else:
                    # Both validator AND SKU scan say no — use brand-in-product-context from dual-page check
                    # OR from Step 1 (WooCommerce/Shopify URL loop, fallback URLs, etc.)
                    brand_in_product_found = brand_found_dual_check or (early_brand_found and bool(early_brand_samples))
                    brand_samples = brand_samples_dual_check if brand_found_dual_check else (early_brand_samples or [])
                    if brand_in_product_found and brand_samples:
                        result["sells_twisted_x"] = True
                        result["confidence"] = "high"
                        result["page_url"] = early_brand_page_url if (early_brand_found and early_brand_page_url) else page.url
                        result["sample_products"] = brand_samples[:5]

                        proof.append("VERIFIED: Twisted X found in product context (brand name in product links/cards)")
                        proof.append(f"Brand-in-product match: {len(brand_samples)} product(s) contain 'Twisted X'")
                        for i, sp in enumerate(brand_samples[:5], 1):
                            name = sp.get("name", "?")[:80]
                            proof.append(f"  {i}. {name}")
                        proof.append(f"Search/catalog page: {page.url}")
                        proof.append("Note: No SKU fingerprint match (retailer may use different style codes)")

                        # Add ecommerce signals if available
                        online_info = validation.get("online_sales", {})
                        indicators = online_info.get("indicators", [])
                        if indicators:
                            proof.append(f"E-commerce signals: {', '.join(indicators[:5])}")
                    else:
                        proof.append("No Twisted X products found on this site")
                        proof.append(f"SKU scan: 0 of {len(TX_STYLE_CODES)} known style codes found on page")

                        method = validation.get("twisted_x_method")
                        if method and method != 'not_found':
                            proof.append(f"Detection method tried: {method}")

                        # Include ecommerce detection details for context
                        online_info = validation.get("online_sales", {})
                        indicators = online_info.get("indicators", [])
                        blockers = online_info.get("blockers", [])

                        if indicators:
                            proof.append(f"E-commerce signals found: {', '.join(indicators[:5])}")
                            proof.append("Site appears to be an online store, but does not carry Twisted X")
                        elif blockers:
                            proof.append(f"Offline signals: {', '.join(blockers[:3])}")
                            proof.append("Site may be in-store only or not a standard ecommerce site")
                        else:
                            proof.append("No e-commerce signals detected (no cart, no add-to-cart buttons)")
                            proof.append("Site may be down, blocked the scraper, or is not an online store")

                        if final_url != normalized:
                            proof.append(f"Final URL after redirects: {final_url}")

                        if validation.get("error"):
                            proof.append(f"Note: {validation['error']}")

                    result["confidence"] = "high"

                # blocked = true only when we couldn't get useful content
                # If we found products/SKUs or real e-commerce (cart, checkout), we got through — don't mark blocked
                got_usable_content = (
                    sku_matched
                    or validation.get("has_twisted_x") is True
                    or result.get("sells_twisted_x") is True
                    or result.get("sells_online") is True  # Real cart/checkout = we saw the real site
                )
                # Add footwear proof
                sf = result.get("sells_footwear")
                if sf is True:
                    proof.append("Footwear: Y (boots/shoes/footwear categories or content found)")
                elif sf is False:
                    proof.append("Footwear: N (no footwear categories found)")
                elif sf is None:
                    proof.append("Footwear: unknown")

                if got_usable_content:
                    result["blocked"] = False
                elif is_blocked:
                    result["blocked"] = True
                    result["blocked_reasons"] = ", ".join(blocked_reasons[:5]) if blocked_reasons else "Unknown"
                    # If they don't sell online, they can't sell footwear or Twisted X online — keep N, not ?
                    if result.get("sells_online") is False:
                        result["sells_twisted_x"] = False
                        result["sells_footwear"] = False
                    else:
                        result["sells_twisted_x"] = None
                        result["sells_footwear"] = None
                    result["confidence"] = "low"
                    proof.insert(0, "Twisted X: unknown (site blocked automated access). Manual check required.")
                    proof.append("Site may be blocking automated access. Please verify manually.")
                    if blocked_reasons:
                        proof.append(f"Indicators: {', '.join(blocked_reasons[:5])}")

                result["proof"] = proof

                if validation.get("error"):
                    result["error"] = validation["error"]

                print(f"[check] Result: sells_twisted_x={result.get('sells_twisted_x')}, "
                      f"store_type={result['store_type']}, confidence={result['confidence']}")

            finally:
                browser.close()

    except Exception as e:
        err_str = str(e)
        if "closed" in err_str.lower() or "target" in err_str.lower():
            result["error"] = "Connection to page was lost during check. Site may be slow or unstable. Please verify manually or retry."
            result["proof"] = [
                "Connection to page was lost during check. Site may be slow or unstable. Please verify manually or retry.",
            ]
        else:
            result["error"] = f"Check failed: {err_str[:200]}"
            result["proof"] = [f"Error during check: {err_str[:100]}"]
        print(f"[check] Error for {url}: {e}")

    return result


# Max time for a single /api/check request (seconds). Prevents indefinite loading.
CHECK_TIMEOUT_SECONDS = 180


@app.post("/api/check", response_model=CheckResponse)
async def check_endpoint(request: CheckRequest):
    """
    Quick check: does this URL sell Twisted X products?

    Returns a simple yes/no answer with proof explaining the determination.
    No product extraction, no pagination -- just detection.

    Typical response time: 15-60 seconds. Request times out after 180 seconds.
    """
    import asyncio
    loop = asyncio.get_event_loop()
    result = None
    for attempt in range(2):  # Retry once on "connection lost" (transient tab/browser crash)
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(None, _check_url_sync, request.url),
                timeout=CHECK_TIMEOUT_SECONDS,
            )
            # If we got "connection lost", retry once with a fresh browser
            if attempt == 0 and result.get("error") and "Connection to page was lost" in (result.get("error") or ""):
                print(f"[check] Connection lost for {request.url}, retrying once after 2s delay...")
                await asyncio.sleep(2)  # Brief delay to let environment settle
                continue
            break
        except asyncio.TimeoutError:
            from config import get_retailer_name
            normalized = request.url.rstrip("/") or request.url
            result = {
                "url": request.url,
                "retailer": get_retailer_name(normalized) or "unknown",
                "sells_twisted_x": None,
                "sells_footwear": None,
                "confidence": "low",
                "store_type": "unknown",
                "sells_online": False,
                "proof": [
                    "Twisted X: unknown (check timed out). Manual check required.",
                    "Check timed out after {} seconds. Site may be slow or unresponsive. Please verify manually.".format(
                        CHECK_TIMEOUT_SECONDS
                    ),
                ],
                "sample_products": [],
                "page_url": None,
                "checked_at": datetime.now().isoformat(),
                "error": "Check timed out; please verify manually.",
                "blocked": False,
            }
            break
    if result is None:
        result = {"url": request.url, "retailer": "unknown", "sells_twisted_x": False, "sells_footwear": None,
                  "confidence": "low", "store_type": "unknown", "sells_online": False, "proof": [],
                  "sample_products": [], "page_url": None, "checked_at": datetime.now().isoformat(),
                  "error": "Check failed.", "blocked": False}
    # #region agent log
    try:
        import json as _json
        import time as _t
        with open("/Users/yasasvi/Documents/twisted-x-scraper/.cursor/debug.log", "a") as _f:
            _f.write(_json.dumps({"hypothesisId":"H2","location":"api_server.py:check_endpoint","message":"api_returning","data":{"req_url":request.url,"url":result.get("url"),"sells_twisted_x":result.get("sells_twisted_x"),"sells_online":result.get("sells_online")},"timestamp":int(_t.time()*1000)}) + "\n")
    except Exception:
        pass
    # #endregion
    return CheckResponse(**result)


# =============================================================================
# POST /api/scrape - Fetch product blocks (no LLM)
# =============================================================================

def _click_next_page(page) -> bool:
    """
    Try to find and click a 'Next page' button/link.

    Attempts multiple common pagination selectors. Waits for navigation
    after clicking to ensure the next page loads.

    Returns:
        True if successfully navigated to the next page, False otherwise.
    """
    next_selectors = [
        'a[rel="next"]',
        'a[aria-label="Next"]',
        'a[aria-label="Next Page"]',
        'button[aria-label="Next"]',
        'a:has-text("Next")',
        'button:has-text("Next")',
        'a:has-text("›")',
        'a:has-text("»")',
        '.pagination a.next',
        '.pagination .next a',
        '.pagination-next a',
        '[class*="pagination"] a:has-text("Next")',
        '[class*="pagination"] [class*="next"]',
        '[class*="pager"] a:has-text("Next")',
        'nav[aria-label="Pagination"] a:last-child',
    ]

    for selector in next_selectors:
        try:
            el = page.query_selector(selector)
            if el and el.is_visible():
                # Check it's not disabled
                classes = (el.get_attribute("class") or "").lower()
                aria_disabled = (el.get_attribute("aria-disabled") or "").lower()
                if "disabled" in classes or aria_disabled == "true":
                    continue

                current_url = page.url
                el.click()
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                page.wait_for_timeout(2000)

                # Verify page actually changed (URL or content shift)
                new_url = page.url
                if new_url != current_url:
                    print(f"[pagination] Navigated: {new_url}")
                    return True

                # URL didn't change but page might have updated via JS
                # (infinite scroll / AJAX pagination). Accept it.
                print(f"[pagination] Page content updated (same URL)")
                return True

        except Exception:
            continue

    return False


def _scrape_url_sync(url: str, search_term: str = "Twisted X", max_pages: int = 5, timeout: int = 30000) -> dict:
    """
    Synchronously scrape a URL and return product blocks (no LLM).
    
    Steps:
    1. Playwright navigation + search
    2. Store type detection (ecommerce/company_store/brand_site)
    3. DOM cleaning + product block extraction
    
    Returns:
        ScrapeResponse-compatible dictionary
    """
    from url_validator import check_url as validate_url, normalize_url
    from config import HEADLESS, get_retailer_name
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    normalized = normalize_url(url)
    if not normalized:
        return {
            "url": url,
            "retailer": "unknown",
            "scraped_at": datetime.now().isoformat(),
            "method": "error",
            "store_type": "unknown",
            "sells_online": False,
            "online_confidence": "low",
            "online_indicators": [],
            "blockers": [],
            "product_count": 0,
            "products": [],
            "errors": ["Invalid URL format"],
        }

    retailer_name = get_retailer_name(normalized)

    result = {
        "url": normalized,
        "retailer": retailer_name,
        "scraped_at": datetime.now().isoformat(),
        "method": "error",
        "store_type": "unknown",
        "sells_online": False,
        "online_confidence": "low",
        "online_indicators": [],
        "blockers": [],
        "product_count": 0,
        "products": [],
        "errors": []
    }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )
            Stealth().apply_stealth_sync(context)
            page = context.new_page()

            try:
                print(f"\n[scrape] Processing: {retailer_name}")
                print(f"[scrape] URL: {normalized}")

                # Step 1: URL validation (store type + Twisted X detection)
                validation = validate_url(normalized, page)

                result["sells_online"] = validation["sells_online"]
                result["online_confidence"] = validation.get("online_sales", {}).get("confidence", "low")
                result["online_indicators"] = validation.get("online_sales", {}).get("indicators", [])
                result["blockers"] = validation.get("online_sales", {}).get("blockers", [])

                combined = validation.get("combined_status", "")
                final_url = validation.get("final_url", normalized)

                is_brand_site = (
                    "twistedx.com" in final_url.lower() or
                    "twisted-x.com" in final_url.lower()
                )

                if is_brand_site:
                    result["store_type"] = "brand_site"
                elif combined == "has_products_sells_online":
                    result["store_type"] = "ecommerce"
                elif combined == "has_products_in_store_only":
                    result["store_type"] = "company_store"
                elif combined == "ecommerce_no_twisted_x":
                    result["store_type"] = "ecommerce"
                elif combined == "no_products_no_online" and validation.get("has_physical_store_indicators"):
                    result["store_type"] = "company_store"
                else:
                    result["store_type"] = "unknown"

                if validation.get("error"):
                    result["errors"].append(validation["error"])

                print(f"[scrape] Store type: {result['store_type']}, sells_online: {result['sells_online']}")

                # Step 2: DOM cleaning + product block extraction (with pagination)
                all_products = []
                method_used = None

                for page_num in range(1, max_pages + 1):
                    print(f"[scrape] Extracting page {page_num}/{max_pages}...")
                    cleaning_result = cleaning.clean_and_extract(page)

                    if not method_used:
                        method_used = cleaning_result["method"]

                    page_products = cleaning_result.get("products", [])
                    if page_products:
                        all_products.extend(page_products)
                        print(f"[scrape] Page {page_num}: {len(page_products)} blocks ({cleaning_result['method']})")
                    else:
                        print(f"[scrape] Page {page_num}: 0 blocks")

                    if cleaning_result.get("error"):
                        result["errors"].append(f"Page {page_num}: {cleaning_result['error']}")

                    # Stop if this is the last page we need
                    if page_num >= max_pages:
                        break

                    # Try to navigate to the next page
                    next_clicked = _click_next_page(page)
                    if not next_clicked:
                        print(f"[scrape] No more pages after page {page_num}")
                        break

                result["method"] = method_used or "error"
                result["products"] = all_products
                result["product_count"] = len(all_products)

                print(f"[scrape] Total: {len(all_products)} blocks across {page_num} page(s)")

            finally:
                browser.close()

        print(f"[scrape] Done: {retailer_name} - {result['product_count']} blocks")

    except Exception as e:
        result["errors"].append(f"Scraper error: {str(e)[:200]}")
        print(f"[scrape] Error for {url}: {str(e)}")

    return result


@app.post("/api/scrape", response_model=ScrapeResponse)
async def scrape_endpoint(request: ScrapeRequestNew):
    """
    Scrape a URL and return product blocks (no LLM extraction).
    
    Designed for Celigo integration:
    1. Navigates to the URL using Playwright
    2. Detects store type (ecommerce/company_store/brand_site)
    3. Cleans DOM and extracts product blocks
    4. Returns structured blocks for Celigo to send to LLM
    
    Typical response time: 15-60 seconds per URL.
    """
    import asyncio
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _scrape_url_sync,
        request.url,
        request.search_term,
        request.max_pages,
        request.timeout
    )
    return ScrapeResponse(**result)


# =============================================================================
# POST /api/verify - Verify LLM-extracted products
# =============================================================================

@app.post("/api/verify", response_model=VerifyResponse)
async def verify_endpoint(request: VerifyRequest):
    """
    Verify LLM-extracted products against original product blocks.
    
    Designed for Celigo integration:
    1. Receives LLM-extracted products from Celigo
    2. Receives original ProductBlocks from /api/scrape
    3. Cross-checks each product against its source block
    4. Returns verified and flagged products
    
    Pure deterministic logic - no LLM calls.
    """
    from verifier import verify_products_against_blocks

    try:
        result = verify_products_against_blocks(
            request.extracted_products,
            request.original_products
        )

        return VerifyResponse(
            verified_products=result["verified_products"],
            flagged_products=result["flagged_products"],
            verification_stats=result["verification_stats"]
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Verification error: {str(e)[:200]}"
        )


# =============================================================================
# GET /api/retailers/urls - List retailer URLs from CSV
# =============================================================================

@app.get("/api/retailers/urls")
async def get_retailer_urls():
    """Get list of all retailer URLs from CSV file"""
    import csv
    from config import RETAILER_URLS

    project_root = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(project_root, "data", "url_validation_full_updated_filtered_online_only.csv")

    urls = []
    try:
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("Web Address", "").strip()
                    if url and url.startswith("http"):
                        urls.append(url)
        else:
            urls = RETAILER_URLS
    except Exception as e:
        print(f"Error reading CSV: {e}")
        urls = RETAILER_URLS

    return {
        "urls": sorted(urls),
        "count": len(urls)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
