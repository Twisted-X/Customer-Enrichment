"""
FastAPI server for Twisted X Scraper API

Rearchitected for Celigo integration:
- POST /api/check:    Quick yes/no — does this URL sell Twisted X?
- POST /api/scrape:   Fetch product blocks from a URL (no LLM)
- POST /api/products: Return every TX product at a URL (compliance map)
- POST /api/verify:   Verify LLM-extracted products against source blocks
- GET  /api/retailers/urls: List retailer URLs from CSV
"""
import asyncio
import hmac
import os
import time
from datetime import datetime, timezone
from typing import List, Optional

# Idempotency cache for /api/enrich/batch — prevents re-burning Google API
# quota when Celigo retries a timed-out request. Key → (response, expires_at).
_IDEMPOTENCY_CACHE: dict = {}
_IDEMPOTENCY_TTL_S = 1800  # 30 minutes

# Max concurrent Playwright browsers — each costs ~150MB RAM.
# Raise this once the server's available RAM is confirmed.
_BROWSER_SEMAPHORE = asyncio.Semaphore(3)

# Load .env before any config import so TWO_CAPTCHA_API_KEY and other env
# vars are available when config.py reads them with os.getenv().
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass

from fastapi import Depends, FastAPI, HTTPException, Request, Security  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.security.api_key import APIKeyHeader  # noqa: E402

from models import (  # noqa: E402
    ScrapeRequestNew, ScrapeResponse,
    VerifyRequest, VerifyResponse,
    CheckRequest, CheckResponse,
    EnrichRequest, EnrichResponse, EnrichPipelineResponse,
    UrlPingItem, UrlPingDetail, UrlPingResponse,
    BatchEnrichItem, BatchEnrichResponse,
    AddressValidateRequest, AddressValidateResponse,
    ClassifyRetailRequest, ClassifyRetailResponse,
)
import cleaning  # noqa: E402
from checker._types import new_check_result  # noqa: E402
from checker._platform import _goto_safe  # noqa: E402
from browser_utils import pw_proxy  # noqa: E402


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
# /api/enrich — auth setup
# =============================================================================

# Loaded once at module level.  The server will refuse to start (see startup
# event below) when the variable is absent, so downstream code can assume it
# is always a non-empty string.
_ENRICH_API_KEY: str = os.environ.get("ENRICH_API_KEY", "")

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


@app.on_event("startup")
def _check_enrich_api_key() -> None:
    """Fail fast at startup rather than accepting requests with no key configured."""
    if not _ENRICH_API_KEY:
        raise RuntimeError(
            "ENRICH_API_KEY env var is not set. "
            "Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(32))\" "
            "and add it to .env before starting the server."
        )


async def _require_enrich_key(key: str = Security(_API_KEY_HEADER)) -> None:
    """FastAPI dependency — rejects requests with missing or wrong X-API-Key header."""
    if not key or not hmac.compare_digest(key, _ENRICH_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


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
    loop   = asyncio.get_running_loop()
    result = None

    async with _BROWSER_SEMAPHORE:
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


def _click_next_page(page, current_page_num: int = 1) -> bool:
    """
    Advance to the next page of product results using three strategies in order:

    1. Standard next-page link/button (rel="next", aria-label, text "Next", etc.)
    2. "Load More" / "Show More" button — appends products in-place rather than
       navigating; succeeds when visible body content grows after the click.
    3. URL-based page increment — when no clickable control is found, try common
       query-string and path patterns (?page=N, ?p=N, ?start=N*12, /page/N/).
       Succeeds when the new page's content differs from the current page.

    Returns True if new content was successfully loaded, False to stop pagination.
    """
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

    # ── Strategy 1: standard next-page click ─────────────────────────────────
    next_selectors = [
        'a[rel="next"]',
        'a[aria-label="Next"]',
        'a[aria-label="Next Page"]',
        'a[aria-label="next page"]',
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
        'nav[aria-label="pagination"] a:last-child',
    ]

    def _snapshot() -> str:
        try:
            return page.inner_text('body')[:800]
        except Exception:
            return ""

    def _try_click(selector: str) -> bool:
        try:
            el = page.query_selector(selector)
            if not (el and el.is_visible()):
                return False
            classes       = (el.get_attribute("class") or "").lower()
            aria_disabled = (el.get_attribute("aria-disabled") or "").lower()
            if "disabled" in classes or aria_disabled == "true":
                return False
            before       = _snapshot()
            current_url  = page.url
            el.click()
            page.wait_for_load_state("domcontentloaded", timeout=15000)
            page.wait_for_timeout(2000)
            return page.url != current_url or _snapshot() != before
        except Exception:
            return False

    for sel in next_selectors:
        if _try_click(sel):
            return True

    # ── Strategy 2: "Load More" / "Show More" button ──────────────────────────
    load_more_selectors = [
        'button:has-text("Load More")',
        'button:has-text("Show More")',
        'button:has-text("View More")',
        'a:has-text("Load More")',
        'a:has-text("Show More")',
        'a:has-text("View More")',
        '[class*="load-more"]',
        '[class*="loadmore"]',
        '[class*="show-more"]',
        '[id*="load-more"]',
        '[id*="loadmore"]',
    ]

    before_load = _snapshot()
    before_count = before_load.count('\n')   # proxy for number of content lines
    for sel in load_more_selectors:
        try:
            el = page.query_selector(sel)
            if not (el and el.is_visible()):
                continue
            classes       = (el.get_attribute("class") or "").lower()
            aria_disabled = (el.get_attribute("aria-disabled") or "").lower()
            if "disabled" in classes or aria_disabled == "true":
                continue
            el.click()
            page.wait_for_timeout(3000)   # give JS time to inject new cards
            after = _snapshot()
            # Require the page to have grown meaningfully (>10 new lines)
            if after.count('\n') > before_count + 10:
                return True
        except Exception:
            continue

    # ── Strategy 3: URL-based page increment ─────────────────────────────────
    # Tries common query-string and path pagination patterns.  The next page
    # number is current_page_num + 1 (caller tracks the page counter).
    next_n = current_page_num + 1

    current_url = page.url
    parsed      = urlparse(current_url)
    qs          = parse_qs(parsed.query, keep_blank_values=True)
    before_url_snap = _snapshot()

    def _try_url(candidate: str) -> bool:
        """Navigate to candidate; return True if content differs from current."""
        try:
            page.goto(candidate, timeout=14000, wait_until='domcontentloaded')
            page.wait_for_timeout(2000)
            after = _snapshot()
            if after != before_url_snap and len(after) > 200:
                return True
            # Content unchanged — go back
            page.go_back(timeout=10000, wait_until='domcontentloaded')
            page.wait_for_timeout(1000)
            return False
        except Exception:
            return False

    # Build candidate URLs for common patterns
    url_candidates = []

    # ?page=N  (most common — Episerver/Optimizely, BigCommerce, custom)
    paged_qs        = {**qs, 'page': [str(next_n)]}
    url_candidates.append(urlunparse(parsed._replace(
        query=urlencode(paged_qs, doseq=True)
    )))

    # ?p=N  (Magento, some WooCommerce)
    p_qs = {**qs, 'p': [str(next_n)]}
    url_candidates.append(urlunparse(parsed._replace(
        query=urlencode(p_qs, doseq=True)
    )))

    # ?start=N  (offset-based: N = (page-1) * 12, 24, or 48)
    for page_size in (12, 24, 48):
        offset = (next_n - 1) * page_size
        s_qs   = {**qs, 'start': [str(offset)]}
        url_candidates.append(urlunparse(parsed._replace(
            query=urlencode(s_qs, doseq=True)
        )))

    # /page/N/  (WordPress / WooCommerce path-based)
    import re as _re
    if _re.search(r'/page/\d+/?$', parsed.path):
        new_path = _re.sub(r'/page/\d+/?$', f'/page/{next_n}/', parsed.path)
    else:
        new_path = parsed.path.rstrip('/') + f'/page/{next_n}/'
    url_candidates.append(urlunparse(parsed._replace(path=new_path, query=parsed.query)))

    for candidate in url_candidates:
        if candidate == current_url:
            continue
        if _try_url(candidate):
            return True

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

    # Track the URL with the most TX-term occurrences seen so far.
    # Initialised to base_url / 0 so there is always a valid fallback.
    best_url:   str = base_url
    best_score: int = 0

    def _score_page(p) -> int:
        """Count total TX term occurrences on the current page body."""
        try:
            body = p.inner_text('body').lower()
            return sum(body.count(t) for t in _tx_terms)
        except Exception:
            return 0

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
        """
        Navigate to url, scroll for lazy loading, score TX content, return
        True if the page passes the TX-relevant threshold.

        Side-effect: updates best_url / best_score if this page scores higher
        than any previously visited URL so that the final fallback always lands
        on the page with the most TX content seen.
        """
        nonlocal best_url, best_score
        try:
            _goto_safe(page, url, timeout=14000)
            page.wait_for_timeout(2000)
            _scroll_to_load(page)
            score = _score_page(page)
            if score > best_score:
                best_score = score
                best_url   = page.url   # use final landed URL (handles redirects)
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

    # All searches failed — navigate to the URL that had the most TX content.
    # This gives the extractor the best possible starting point rather than
    # falling back blindly to the homepage.
    if page.url != best_url:
        try:
            _goto_safe(page, best_url, timeout=12000)
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
    from patchright.sync_api import sync_playwright

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
                proxy=pw_proxy(),
            )
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
                            _goto_safe(page, found_on_url, timeout=12000)
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

                    if not _click_next_page(page, current_page_num=page_num):
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
    loop   = asyncio.get_running_loop()
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
# POST /api/enrich — Single-record customer enrichment
# =============================================================================

def _enrich_customer_sync(
    company: str,
    address: str,
    city: str,
    state: str,
    zip_code: str,
    current_url: Optional[str],
    internal_id: Optional[str],
) -> dict:
    """Blocking wrapper for enrich_single_customer — runs in a thread-pool executor."""
    from enrichment import enrich_single_customer
    return enrich_single_customer(
        company=company,
        address=address,
        city=city,
        state=state,
        zip_code=zip_code,
        current_url=current_url,
        internal_id=internal_id,
    )


@app.post(
    "/api/enrich",
    response_model=EnrichResponse,
    dependencies=[Depends(_require_enrich_key)],
    summary="Enrich a single customer record via Google Places",
    tags=["enrichment"],
)
async def enrich_endpoint(request: EnrichRequest):
    """
    Enrich a customer record with Google Places data.

    **Primary path**: Google Address Validation API → Places Details API
    (uses physical address as the lookup key — more reliable than text search
    for small/unusual shop names).

    **Fallback**: Google Places Text Search (existing pipeline logic).

    **Auth**: requires `X-API-Key` header matching `ENRICH_API_KEY` env var.

    **Error handling**:
    - Google API failures → HTTP 200, `enrichment_source="enrichment_error"`
    - Do NOT retry on 200; retry only on HTTP 5xx.
    - Unhandled server crash → HTTP 500, `{"detail": "Internal server error"}`
    """
    import logging as _logging
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            _enrich_customer_sync,
            request.company,
            request.address,
            request.city,
            request.state,
            request.zip_code,
            request.current_url,
            request.internal_id,
        )
        return EnrichResponse(**result)
    except HTTPException:
        raise  # let auth/validation errors pass through unchanged
    except Exception:
        _logging.getLogger(__name__).exception("Unhandled error in /api/enrich")
        raise HTTPException(status_code=500, detail="Internal server error")


# =============================================================================
# POST /api/enrich/pipeline — Full batch enrichment pipeline
# =============================================================================

def _run_pipeline_sync() -> dict:
    """
    Blocking wrapper for run_pipeline() — runs in a thread-pool executor so
    FastAPI's event loop is not blocked during the (potentially long) batch run.
    """
    from enrichment import run_pipeline

    t0           = time.monotonic()
    started_at   = datetime.now(timezone.utc).isoformat()
    run_pipeline()
    completed_at = datetime.now(timezone.utc).isoformat()
    duration_sec = round(time.monotonic() - t0, 2)
    return {
        "status":       "completed",
        "message":      f"Pipeline finished in {duration_sec}s",
        "started_at":   started_at,
        "completed_at": completed_at,
        "duration_sec": duration_sec,
    }


@app.post(
    "/api/enrich/pipeline",
    response_model=EnrichPipelineResponse,
    dependencies=[Depends(_require_enrich_key)],
    summary="Run the full enrichment batch pipeline",
    tags=["enrichment"],
)
async def enrich_pipeline_endpoint():
    """
    Trigger the full enrichment batch pipeline.

    **Flow per row**: Google Address Validation API → location-biased Text Search
    → Text Search fallback (same logic as `POST /api/enrich` but applied to every
    stale row in the input CSV).

    **Long-running**: blocks until the pipeline finishes (may take many minutes
    for large files — set a generous HTTP client timeout).

    **Auth**: requires `X-API-Key` header matching `ENRICH_API_KEY` env var.

    **Input/output**: controlled by `INPUT_FILE` / `OUTPUT_FILE` env vars
    (or SFTP when `USE_SFTP=true`). See `enrichment/_config.py` for all options.

    Returns a summary with start/end timestamps and total duration in seconds.
    """
    import logging as _logging
    try:
        loop   = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _run_pipeline_sync)
        return EnrichPipelineResponse(**result)
    except HTTPException:
        raise
    except Exception:
        _logging.getLogger(__name__).exception("Unhandled error in /api/enrich/pipeline")
        raise HTTPException(status_code=500, detail="Internal server error")


# =============================================================================
# POST /api/enrich/url-ping — Bulk URL liveness check
# =============================================================================

@app.post(
    "/api/enrich/url-ping",
    response_model=UrlPingResponse,
    dependencies=[Depends(_require_enrich_key)],
    summary="Ping URLs for liveness — identify dead/missing URLs before enrichment",
    tags=["enrichment"],
)
async def url_ping_endpoint(items: List[UrlPingItem]):
    """
    Concurrently ping each URL and classify it as **alive**, **dead**, or **missing**.

    | Status | Meaning | Bucket |
    |---|---|---|
    | `active` | 200, URL unchanged | alive |
    | `redirected` | Final URL differs from input | alive |
    | `blocked` | 401/403/429/503 — server alive but rejecting bots | alive |
    | `dead` | Network error or non-200 | dead |
    | `missing` | Null / blank / placeholder URL | missing |

    Uses the same async concurrent checker as the batch pipeline.
    Works for a single record (`[{...}]`) or a batch.

    **Note**: `blocked` is grouped into **alive** because the server is reachable —
    the existing URL is valid, just protected. No enrichment needed.
    """
    from enrichment._url import bulk_check_urls

    urls    = [item.url or "" for item in items]
    results = await bulk_check_urls(urls)

    alive:   list[str] = []
    dead:    list[str] = []
    missing: list[str] = []
    details: list[UrlPingDetail] = []

    for item, result in zip(items, results):
        status = result["status"]
        if status in ("active", "redirected", "blocked"):
            alive.append(item.internal_id)
        elif status == "missing":
            missing.append(item.internal_id)
        else:
            dead.append(item.internal_id)

        details.append(UrlPingDetail(
            internal_id=item.internal_id,
            status=status,
            http_code=result.get("http_code"),
            final_url=result.get("final_url"),
        ))

    return UrlPingResponse(alive=alive, dead=dead, missing=missing, details=details)


# =============================================================================
# POST /api/enrich/batch — Parallel multi-record enrichment
# =============================================================================

@app.post(
    "/api/enrich/batch",
    response_model=BatchEnrichResponse,
    dependencies=[Depends(_require_enrich_key)],
    summary="Enrich multiple customer records concurrently",
    tags=["enrichment"],
)
async def enrich_batch_endpoint(request: Request, items: List[EnrichRequest]):
    """
    Enrich up to **100 records** in a single call.  Records are processed
    concurrently via a thread-pool, so a 20-record batch takes roughly the
    same time as a single record rather than 20× longer.

    Results are returned in the **same order** as the request. Each item
    echoes back the `internal_id` so Celigo can match results without relying
    on position.

    Same enrichment flow as `POST /api/enrich` per record:
    Address Validation → location-biased Text Search → Text Search fallback.

    **Idempotency**: pass `X-Idempotency-Key: <uuid>` to make retries safe.
    The server caches the response for 30 minutes — a retry with the same key
    returns the cached result immediately without re-calling Google APIs.
    """
    import logging as _logging

    if len(items) > 100:
        raise HTTPException(status_code=422, detail="Batch size exceeds maximum of 100 records")

    # ── Idempotency check ────────────────────────────────────────────────────
    idem_key = request.headers.get("X-Idempotency-Key", "").strip() or None
    if idem_key:
        cached = _IDEMPOTENCY_CACHE.get(idem_key)
        if cached and time.monotonic() < cached[1]:
            _logging.getLogger(__name__).info(
                "enrich/batch idempotency hit for key %s — returning cached response", idem_key[:16]
            )
            return cached[0]

    loop = asyncio.get_running_loop()
    t0   = time.monotonic()

    async def _enrich_one(item: EnrichRequest) -> dict:
        return await loop.run_in_executor(
            None,
            _enrich_customer_sync,
            item.company, item.address, item.city, item.state,
            item.zip_code, item.current_url, item.internal_id,
        )

    try:
        raw_results = await asyncio.gather(*[_enrich_one(item) for item in items])
    except HTTPException:
        raise
    except Exception:
        _logging.getLogger(__name__).exception("Unhandled error in /api/enrich/batch")
        raise HTTPException(status_code=500, detail="Internal server error")

    duration_sec = round(time.monotonic() - t0, 2)
    batch_results = [
        BatchEnrichItem(internal_id=item.internal_id or "", result=EnrichResponse(**res))
        for item, res in zip(items, raw_results)
    ]
    total_google_calls = sum(r.get("google_api_calls", 0) for r in raw_results)
    total_quota_errors = sum(1 for r in raw_results if r.get("enrichment_source") == "quota")
    response = BatchEnrichResponse(
        results=batch_results,
        total=len(batch_results),
        duration_sec=duration_sec,
        google_api_calls=total_google_calls,
        quota_errors=total_quota_errors,
    )

    # ── Cache response for idempotency ───────────────────────────────────────
    if idem_key:
        _IDEMPOTENCY_CACHE[idem_key] = (response, time.monotonic() + _IDEMPOTENCY_TTL_S)
        # Evict expired entries to prevent unbounded memory growth
        now = time.monotonic()
        expired = [k for k, (_, exp) in _IDEMPOTENCY_CACHE.items() if exp < now]
        for k in expired:
            _IDEMPOTENCY_CACHE.pop(k, None)

    return response


# =============================================================================
# POST /api/enrich/address-validate — Address Validation API (debug/inspect)
# =============================================================================

@app.post(
    "/api/enrich/address-validate",
    response_model=AddressValidateResponse,
    dependencies=[Depends(_require_enrich_key)],
    summary="Validate a physical address via Google Address Validation API",
    tags=["enrichment"],
)
async def address_validate_endpoint(request: AddressValidateRequest):
    """
    Call the Google Address Validation API and return the geocoded result.

    Useful for **debugging** enrichment mismatches:
    - `geocoded=false` → the address is too vague or malformed for Google to resolve
    - `geocoded=true, place_id_present=false` → coordinates found but no business listing nearby
    - `geocoded=true, place_id_present=true` → address resolves cleanly; enrichment should work

    Does **not** call the Places API — cheaper than `/api/enrich`, one API call only.

    **Auth**: requires `X-API-Key` header.
    """
    import logging as _logging
    from enrichment._address_validation import validate_address

    try:
        loop   = asyncio.get_running_loop()
        result, err = await loop.run_in_executor(
            None, validate_address,
            request.address, request.city, request.state, request.zip_code,
        )
    except HTTPException:
        raise
    except Exception:
        _logging.getLogger(__name__).exception("Unhandled error in /api/enrich/address-validate")
        raise HTTPException(status_code=500, detail="Internal server error")

    if result is None:
        return AddressValidateResponse(
            geocoded=False, latitude=None, longitude=None,
            formatted_address="", place_id_present=False,
            is_business=False, error=err or "upstream_5xx",
        )

    lat = result.get("latitude")
    lng = result.get("longitude")
    return AddressValidateResponse(
        geocoded=bool(lat and lng),
        latitude=lat,
        longitude=lng,
        formatted_address=result.get("formatted_address") or "",
        place_id_present=bool(result.get("place_id")),
        is_business=bool(result.get("is_business", False)),
        error=None,
    )




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


# =============================================================================
# POST /api/enrich/classify-retail — Retail type classification
# =============================================================================

@app.post(
    "/api/enrich/classify-retail",
    response_model=ClassifyRetailResponse,
    dependencies=[Depends(_require_enrich_key)],
    summary="Classify a business as retail / not_retail / unknown",
    tags=["enrichment"],
)
async def classify_retail_endpoint(request: ClassifyRetailRequest):
    """
    Classify a business record as `retail`, `not_retail`, or `unknown` using
    Google Places `primary_type` and context flags.

    **Classification tiers:**

    | Condition | Result |
    |---|---|
    | `is_channel_row=true` (ecom/online suffix) | `not_retail` |
    | `primary_type` is warehouse / storage / distribution | `not_retail` |
    | `primary_type` is a known store type (shoe_store, clothing_store, …) | `retail` |
    | Has opening hours but no recognised store type | `retail` |
    | None of the above | `unknown` |

    Pure logic — no external API calls, instant response.
    """
    from enrichment._retail import classify_retail_type

    retail_type = classify_retail_type(
        row_is_channel=request.is_channel_row,
        primary_type=request.primary_type,
        has_opening_hours=request.has_opening_hours,
    )
    return ClassifyRetailResponse(retail_type=retail_type)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
