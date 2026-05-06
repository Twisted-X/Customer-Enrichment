"""
FastAPI server for Twisted X Scraper API

Rearchitected for Celigo integration:
- POST /api/check:  Quick yes/no — does this URL sell Twisted X?
- POST /api/scrape: Fetch product blocks from a URL (no LLM)
- POST /api/verify: Verify LLM-extracted products against source blocks
- GET  /api/retailers/urls: List retailer URLs from CSV
"""
import json
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
from brand_config import PRIMARY_BRAND_PAIR
from checker._types import new_check_result

# Brand terms used by in-browser DOM scans. Sourced from config/brand_indicators.json.
_PRIMARY_BRAND_TERMS_JS = json.dumps(list(PRIMARY_BRAND_PAIR) + ["twisted-x"])


app = FastAPI(
    title="Twisted X Scraper API",
    description="Dumb fetcher + verifier API for Celigo integration (no LLM calls)",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Basic routes
# =============================================================================

@app.get("/")
async def root():
    return {
        "message": "Twisted X Scraper API (Celigo Rearchitecture)",
        "version": "2.0.0",
        "endpoints": {
            "health":    "/health",
            "check":     "/api/check (POST - quick yes/no: does this URL sell Twisted X?)",
            "scrape":    "/api/scrape (POST - fetch product blocks, no LLM)",
            "verify":    "/api/verify (POST - verify LLM-extracted products)",
            "retailers": "/api/retailers/urls (GET - list retailer URLs from CSV)"
        }
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/api/test")
async def test_endpoint():
    return {"message": "API is working", "timestamp": datetime.now().isoformat()}


# =============================================================================
# POST /api/check — Quick Twisted X detection (yes/no + proof)
# =============================================================================

CHECK_TIMEOUT_SECONDS = 180


def _check_url_sync(url: str) -> dict:
    """
    Delegates to the `checker` package:
      Layer 1 (HTTP-first) → Layer 2 (sitemap) → Layer 3 (Playwright)
    Each layer short-circuits on a definitive YES to avoid launching a browser.
    See checker/__init__.py for the full flow.
    """
    from checker import run_check
    return run_check(url)


@app.post("/api/check", response_model=CheckResponse)
async def check_endpoint(request: CheckRequest):
    """
    Quick check: does this URL sell Twisted X products?

    Returns a yes/no with proof. No product extraction, no pagination.
    Typical response time: 15-60 seconds. Times out after 180 seconds.
    """
    import asyncio
    loop   = asyncio.get_event_loop()
    result = None

    for attempt in range(2):  # retry once on transient browser crash
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(None, _check_url_sync, request.url),
                timeout=CHECK_TIMEOUT_SECONDS,
            )
            if attempt == 0 and result.get("error") and "Connection to page was lost" in (result.get("error") or ""):
                await asyncio.sleep(2)
                continue
            break
        except asyncio.TimeoutError:
            from config import get_retailer_name
            result = new_check_result(
                request.url,
                get_retailer_name(request.url) or "unknown",
                error="Check timed out; please verify manually.",
            )
            result["sells_twisted_x"] = None
            result["proof"] = [
                "Twisted X: unknown (check timed out). Manual check required.",
                f"Check timed out after {CHECK_TIMEOUT_SECONDS} seconds. "
                "Site may be slow or unresponsive. Please verify manually.",
            ]
            break

    if result is None:
        result = new_check_result(request.url, "unknown", error="Check failed.")
    return CheckResponse(**result)


# =============================================================================
# POST /api/scrape — Fetch product blocks (no LLM)
# =============================================================================

def _new_scrape_result(url: str, retailer: str, errors: Optional[List[str]] = None) -> dict:
    return {
        "url":               url,
        "retailer":          retailer,
        "scraped_at":        datetime.now().isoformat(),
        "method":            "error",
        "store_type":        "unknown",
        "sells_online":      False,
        "online_confidence": "low",
        "online_indicators": [],
        "blockers":          [],
        "product_count":     0,
        "products":          [],
        "errors":            list(errors) if errors else [],
    }


def _click_next_page(page) -> bool:
    """
    Try to find and click a 'Next page' button/link.
    Returns True if successfully navigated to the next page.
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
            if not (el and el.is_visible()):
                continue
            classes        = (el.get_attribute("class") or "").lower()
            aria_disabled  = (el.get_attribute("aria-disabled") or "").lower()
            if "disabled" in classes or aria_disabled == "true":
                continue

            current_url = page.url
            # Snapshot content before click so we can verify the page changed
            try:
                before_text = page.inner_text('body')[:500]
            except Exception:
                before_text = ""

            el.click()
            page.wait_for_load_state("domcontentloaded", timeout=15000)
            page.wait_for_timeout(2000)

            # Only count as success if URL changed OR visible content changed.
            # Returning True on an unchanged page causes duplicate extraction.
            try:
                after_text = page.inner_text('body')[:500]
            except Exception:
                after_text = ""

            url_changed     = page.url != current_url
            content_changed = after_text != before_text
            if url_changed or content_changed:
                return True
            # Click had no effect — not a real pagination control
            continue
        except Exception:
            continue

    return False


def _navigate_to_best_tx_page(page, base_url: str) -> None:
    """
    Navigate to the page that exposes Twisted X products before extraction.

    Delegates to the existing checker machinery:
    - checker._platform.detect_platform  — Shopify / WooCommerce / NetSuite / normal
    - checker._search._search_urls_for_platform — platform-correct TX search URLs
      (Shopify: auto-detects actual search path from store nav links)
    - checker._search._scroll_to_load / _page_has_content — scroll + content checks

    Tries platform search URLs first (TX-specific). Falls back to generic
    collection/brand URL patterns if none return content.
    """
    from urllib.parse import urlparse
    from checker._platform import detect_platform
    from checker._search import (
        _search_urls_for_platform, _scroll_to_load, _page_has_content,
    )

    parsed   = urlparse(base_url)
    origin   = f"{parsed.scheme}://{parsed.netloc}"
    platform = detect_platform(page, base_url)

    # Platform-specific TX search URLs (already built + ordered correctly).
    search_urls = _search_urls_for_platform(page, platform, origin)

    # Generic fallback paths tried when platform search fails.
    # Order matters: brand/collection paths that explicitly name TX come first
    # so we land on the right page without trying the full generic search.
    fallback_paths = [
        "/brands/twisted-x/",
        "/brands/twisted-x",
        "/twisted-x",
        "/product-category/twisted-x/",
        "/collections/twisted-x",
        "/search?q=Twisted+X",
    ]
    fallbacks = [origin + p for p in fallback_paths]

    _tx_terms = ('twisted-x', 'twistedx', 'twisted x')
    # Phrases that indicate a 404/error page — these must not be confused with
    # a real TX product listing even if the URL contains "twisted".
    _error_phrases = (
        'page not found', "page doesn't exist", 'we couldn\'t find this page',
        'the page you are looking for', 'this page is no longer available',
        'error 404',
    )

    def _page_is_tx_relevant(p) -> bool:
        """
        True when the current page is showing real TX products (not a 404).
        Requires:
          - Meaningful body content (_page_has_content)
          - No 404/error phrases in the body
          - TX brand terms appear in the final landed URL OR in the page body
        """
        if not _page_has_content(p):
            return False
        try:
            landed = p.url.lower()
            body   = p.inner_text('body').lower()
        except Exception:
            return False
        if any(phrase in body for phrase in _error_phrases):
            return False
        return any(t in landed or t in body for t in _tx_terms)

    def _goto_and_check(url: str) -> bool:
        """Navigate, scroll to trigger lazy loading, return True if TX-relevant."""
        try:
            page.goto(url, timeout=14000, wait_until='domcontentloaded')
            page.wait_for_timeout(2000)
            _scroll_to_load(page)
            return _page_is_tx_relevant(page)
        except Exception:
            return False

    # Try platform-specific URLs first — stop on the first with TX content.
    for url in search_urls:
        if _goto_and_check(url):
            return  # page is now on the TX search results

    # Platform search failed — try generic fallbacks.
    for url in fallbacks:
        if _goto_and_check(url):
            return

    if page.url != best_url:
        try:
            page.goto(best_url, timeout=12000, wait_until='domcontentloaded')
            page.wait_for_timeout(2000)
        except Exception:
            pass


def _scrape_url_sync(url: str, search_term: str = "Twisted X", max_pages: int = 15, timeout: int = 30000) -> dict:
    """
    Synchronously scrape a URL and return product blocks (no LLM).

    Steps:
      1. Playwright navigation + search
      2. Store type detection (ecommerce / company_store / brand_site)
      3. DOM cleaning + product block extraction (with pagination)
    """
    from url_validator import check_url as validate_url, normalize_url
    from config import HEADLESS, get_retailer_name
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    normalized = normalize_url(url)
    if not normalized:
        return _new_scrape_result(url, "unknown", errors=["Invalid URL format"])

    retailer_name = get_retailer_name(normalized)
    result        = _new_scrape_result(normalized, retailer_name)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            Stealth().apply_stealth_sync(context)
            page = context.new_page()

            try:
                # Step 1: URL validation (store type + Twisted X detection)
                validation = validate_url(normalized, page)

                result["sells_online"]      = validation["sells_online"]
                result["online_confidence"] = validation.get("online_sales", {}).get("confidence", "low")
                result["online_indicators"] = validation.get("online_sales", {}).get("indicators", [])
                result["blockers"]          = validation.get("online_sales", {}).get("blockers", [])

                combined  = validation.get("combined_status", "")
                final_url = validation.get("final_url", normalized)

                is_brand_site = "twistedx.com" in final_url.lower() or "twisted-x.com" in final_url.lower()

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

                # After validation, the browser may be on a footwear detection page
                # (/boots). Navigate back to where TX products were actually found
                # so extraction runs on the right page.
                # found_on_url is the URL where detect_twisted_x located products
                # (e.g. the search results page after typing "Twisted X" in the search bar).
                # If TX was not found at all, fall back to the _navigate_to_best_tx_page
                # heuristic which tries known URL patterns for the platform.
                if not is_brand_site:
                    found_on_url = validation.get("found_on_url")
                    if found_on_url and found_on_url != page.url:
                        try:
                            page.goto(found_on_url, timeout=12000, wait_until='domcontentloaded')
                            page.wait_for_timeout(2000)
                        except Exception:
                            pass
                    elif not found_on_url:
                        _navigate_to_best_tx_page(page, normalized)

                # Step 2: DOM cleaning + product block extraction (with pagination)
                all_products = []
                method_used  = None
                page_num     = 1

                for page_num in range(1, max_pages + 1):
                    cleaning_result = cleaning.clean_and_extract(page)

                    if not method_used:
                        method_used = cleaning_result["method"]

                    page_products = cleaning_result.get("products", [])
                    if page_products:
                        all_products.extend(page_products)

                    if cleaning_result.get("error"):
                        result["errors"].append(f"Page {page_num}: {cleaning_result['error']}")

                    if page_num >= max_pages:
                        break

                    if not _click_next_page(page):
                        break

                result["method"]        = method_used or "error"
                result["products"]      = all_products
                result["product_count"] = len(all_products)

            finally:
                browser.close()

    except Exception as exc:
        result["errors"].append(f"Scraper error: {str(exc)[:200]}")

    return result


@app.post("/api/scrape", response_model=ScrapeResponse)
async def scrape_endpoint(request: ScrapeRequestNew):
    """
    Scrape a URL and return product blocks (no LLM extraction).

    Designed for Celigo integration:
      1. Navigate to URL using Playwright
      2. Detect store type (ecommerce / company_store / brand_site)
      3. Clean DOM and extract product blocks
      4. Return structured blocks for Celigo to send to Claude

    Typical response time: 15-60 seconds per URL.
    """
    import asyncio
    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _scrape_url_sync,
        request.url,
        request.search_term,
        request.max_pages,
        request.timeout,
    )
    return ScrapeResponse(**result)


# =============================================================================
# POST /api/verify — Verify LLM-extracted products
# =============================================================================

@app.post("/api/verify", response_model=VerifyResponse)
async def verify_endpoint(request: VerifyRequest):
    """
    Cross-check LLM-extracted products against original product blocks.

    Designed for Celigo integration:
      1. Receive LLM-extracted products from Celigo
      2. Receive original ProductBlocks from /api/scrape
      3. Cross-check each product against its source block
      4. Return verified and flagged products

    Pure deterministic logic — no LLM calls.
    """
    from verifier import verify_products_against_blocks

    try:
        result = verify_products_against_blocks(
            request.extracted_products,
            request.original_products,
        )
        return VerifyResponse(
            verified_products=result["verified_products"],
            flagged_products=result["flagged_products"],
            verification_stats=result["verification_stats"],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Verification error: {str(exc)[:200]}")


# =============================================================================
# GET /api/retailers/urls — List retailer URLs from CSV
# =============================================================================

@app.get("/api/retailers/urls")
async def get_retailer_urls():
    """Get list of all retailer URLs from CSV file."""
    import csv
    from config import RETAILER_URLS

    project_root = os.path.dirname(os.path.abspath(__file__))
    csv_path     = os.path.join(project_root, "data", "url_validation_full_updated_filtered_online_only.csv")

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
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("Error reading retailer CSV: %s", exc)
        urls = RETAILER_URLS

    return {"urls": sorted(urls), "count": len(urls)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
