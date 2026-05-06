"""
Layer-3: Full Playwright browser orchestration for the /api/check endpoint.

playwright_check(url, normalized, retailer_name) → dict

This module is the only place in the checker package that opens a browser.
It wires together the smaller focused modules:

  _platform  → detect_platform, detect_blocked
  _search    → search_netsuite, search_shopify_or_woo, search_generic
  _scanners  → scan_page_for_skus, find_brand_in_product_context
  url_validator.check_url → full URL validation (sells_online, store_type, etc.)

Result assembly (_build_proof, _apply_block_state, _determine_store_type) is
kept here because it depends on the combined output of all three sources above
(search outcome, validation, block state).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse as _urlparse

from config import HEADLESS, TX_STYLE_CODES
from ._types import SearchOutcome, ScanResult, empty_scan, empty_search
from ._platform import detect_platform, detect_blocked
from ._search import search_netsuite, search_shopify_or_woo, search_generic
from ._scanners import scan_page_for_skus, find_brand_in_product_context

log = logging.getLogger(__name__)

# Desktop Chrome user-agent — most retailer sites work with this
_DESKTOP_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def playwright_check(url: str, normalized: str, retailer_name: str) -> dict:
    """
    Open a real browser, navigate to the retailer site, and determine whether
    they sell Twisted X.

    Flow:
      1. Platform detection (Shopify? NetSuite? Generic?)
      2. Platform-aware search (find the Twisted X product listing page)
      3. SKU fingerprint scan on the search results
      4. Full url_validator check (online sales capability, footwear, store type)
      5. Result assembly with proof strings

    Returns a dict matching the CheckResponse Pydantic model shape.
    Never raises — catches all browser errors and returns a result with error set.
    """
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth
    from url_validator import check_url as validate_url

    result = _empty_result(normalized, retailer_name)
    base_url = f"{_urlparse(normalized).scheme}://{_urlparse(normalized).netloc}"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=_DESKTOP_UA,
            )
            Stealth().apply_stealth_sync(context)
            page = context.new_page()

            try:
                log.info("Playwright check: %s (%s)", retailer_name, normalized)

                # ── Step 1: Load homepage, detect platform and bot-blocking ──
                page.goto(normalized, timeout=15_000, wait_until='domcontentloaded')
                page.wait_for_timeout(2_000)

                is_blocked, blocked_reasons = detect_blocked(page)
                platform = detect_platform(page, normalized)
                log.info("Platform: %s | Blocked signals: %s", platform, is_blocked)

                # ── Step 2: Platform-aware search ──
                search_outcome = _run_search(page, platform, base_url, normalized)

                # ── Step 3: Full validation (online sales, footwear, store type) ──
                page.goto(normalized, timeout=15_000, wait_until='domcontentloaded')
                page.wait_for_timeout(2_000)
                validation = validate_url(normalized, page)

                # ── Step 4: Dual-page SKU check (product page + homepage) ──
                dual_scan, dual_brand_found, dual_brand_samples = _dual_page_scan(
                    page, platform, base_url, normalized, search_outcome
                )

                # ── Step 5: Assemble result ──
                result = _assemble_result(
                    url=normalized,
                    retailer_name=retailer_name,
                    validation=validation,
                    search_outcome=search_outcome,
                    dual_scan=dual_scan,
                    dual_brand_found=dual_brand_found,
                    dual_brand_samples=dual_brand_samples,
                    is_blocked=is_blocked,
                    blocked_reasons=blocked_reasons,
                    page=page,
                )

            finally:
                browser.close()

    except Exception as exc:
        err = str(exc)
        if "closed" in err.lower() or "target" in err.lower():
            msg = "Connection to page was lost. Site may be slow or unstable. Please retry."
        else:
            msg = f"Check failed: {err[:200]}"
        log.error("playwright_check error for %s: %s", url, exc)
        result["error"] = msg
        result["proof"] = [msg]

    return result


# ---------------------------------------------------------------------------
# Search dispatch
# ---------------------------------------------------------------------------

def _run_search(page, platform: str, base_url: str, normalized: str) -> SearchOutcome:
    """Dispatch to the right search strategy based on platform."""
    if platform == 'netsuite':
        return search_netsuite(page, base_url)
    if platform in ('shopify', 'woocommerce'):
        return search_shopify_or_woo(page, platform, base_url, normalized)
    return search_generic(page, base_url, normalized)


# ---------------------------------------------------------------------------
# Dual-page SKU check (product page AND homepage)
# ---------------------------------------------------------------------------

def _dual_page_scan(page, platform, base_url, normalized, search_outcome: SearchOutcome):
    """
    If the search outcome already found a match, return it directly.
    Otherwise navigate to the best product page for this platform AND the homepage,
    running SKU + brand scans on each.

    Returns (sku_scan, brand_found, brand_samples).
    """
    # Already found via search — no need for dual-page scan
    if search_outcome["found_match"] and search_outcome["sku_scan"]["matched_codes"]:
        return search_outcome["sku_scan"], False, []
    if search_outcome["found_match"] and search_outcome["brand_found"]:
        return empty_scan(), True, search_outcome["brand_samples"]

    # Navigate to the best product page for this platform
    _goto_product_page(page, platform, base_url, normalized)
    log.info("Dual-page scan — product page: %s", page.url)

    sku_scan = scan_page_for_skus(page)
    if sku_scan["matched_codes"]:
        log.info("SKU match on product page: %d codes", len(sku_scan["matched_codes"]))
        return sku_scan, False, []

    brand_found, brand_samples = find_brand_in_product_context(page)
    if brand_found:
        log.info("Brand in product context on product page: %d samples", len(brand_samples))
        return empty_scan(), True, brand_samples

    # Fall back to homepage scan
    log.info("No match on product page, scanning homepage")
    try:
        page.goto(normalized, timeout=15_000, wait_until='domcontentloaded')
        page.wait_for_timeout(3_000)
        sku_scan = scan_page_for_skus(page)
        if sku_scan["matched_codes"]:
            return sku_scan, False, []
        brand_found, brand_samples = find_brand_in_product_context(page)
        if brand_found:
            return empty_scan(), True, brand_samples
    except Exception as exc:
        log.warning("Homepage scan failed: %s", exc)

    return empty_scan(), False, []


def _goto_product_page(page, platform: str, base_url: str, normalized: str) -> None:
    """Navigate to the platform's canonical search/product page."""
    urls = {
        'netsuite':    f"{base_url}/catalog/productsearch",
        'shopify':     f"{base_url}/search?q=Twisted+X",
        'woocommerce': f"{base_url}/?s=Twisted+X&post_type=product",
    }
    target = urls.get(platform)
    if target:
        try:
            page.goto(target, timeout=15_000, wait_until='domcontentloaded')
            page.wait_for_timeout(3_500)
            return
        except Exception as exc:
            log.warning("_goto_product_page failed for %s: %s", platform, exc)

    # Generic: use Playwright search-bar interaction
    import url_validator
    try:
        page.goto(normalized, timeout=15_000, wait_until='domcontentloaded')
        page.wait_for_timeout(1_500)
        url_validator._search_on_site(page, 'Twisted X')
        page.wait_for_timeout(4_000)
    except Exception as exc:
        log.warning("Generic product page navigation failed: %s", exc)


# ---------------------------------------------------------------------------
# Result assembly
# ---------------------------------------------------------------------------

def _assemble_result(
    url: str,
    retailer_name: str,
    validation: dict,
    search_outcome: SearchOutcome,
    dual_scan: ScanResult,
    dual_brand_found: bool,
    dual_brand_samples: list,
    is_blocked: bool,
    blocked_reasons: list,
    page,
) -> dict:
    result = _empty_result(url, retailer_name)

    # Populate from validation
    result["sells_twisted_x"] = validation.get("has_twisted_x", False)
    result["sells_online"]    = validation.get("sells_online", False)
    result["sells_footwear"]  = validation.get("sells_footwear")
    result["store_type"]      = _determine_store_type(validation, page.url)

    sku_matched = bool(dual_scan["matched_codes"])
    proof: List[str] = []

    if sku_matched:
        result, proof = _apply_sku_match(result, dual_scan, proof, page)
    elif dual_brand_found and dual_brand_samples:
        result, proof = _apply_brand_match(result, dual_brand_samples, proof, page)
    elif search_outcome["brand_found"] and search_outcome["brand_samples"]:
        result, proof = _apply_brand_match(result, search_outcome["brand_samples"], proof, page)
    else:
        result, proof = _apply_no_match(result, validation, proof, page)

    # Online sales evidence (ecommerce signals, offline blockers)
    online_info = validation.get("online_sales", {})
    for sig in online_info.get("indicators", [])[:5]:
        proof.append(f"E-commerce signals: {sig}")
    for blk in online_info.get("blockers", [])[:3]:
        proof.append(f"Offline signals: {blk}")
    if online_conf := online_info.get("confidence"):
        proof.append(f"Online sales confidence: {online_conf}")

    # Footwear summary
    sf = result.get("sells_footwear")
    proof.append(
        "Footwear: Y (boots/shoes/footwear categories or content found)" if sf is True
        else "Footwear: N (no footwear categories found)" if sf is False
        else "Footwear: unknown"
    )

    # Bot-blocking state
    got_usable = (
        sku_matched
        or result.get("sells_twisted_x") is True
        or result.get("sells_online") is True
    )
    if not got_usable and is_blocked:
        result = _apply_block_state(result, blocked_reasons, proof)
        proof = result["proof"]  # block state rewrites proof
    else:
        result["blocked"] = False
        result["proof"]   = proof

    log.info(
        "Result: sells_twisted_x=%s store_type=%s confidence=%s",
        result.get("sells_twisted_x"), result["store_type"], result["confidence"],
    )
    return result


def _apply_sku_match(result: dict, sku_scan: ScanResult, proof: List[str], page) -> tuple:
    codes = sorted(sku_scan["matched_codes"])[:10]
    result["sells_twisted_x"]  = True
    result["confidence"]        = "high"
    result["page_url"]          = page.url
    result["sample_products"]   = sku_scan["sample_products"][:5]

    proof += [
        f"VERIFIED: {len(codes)} Twisted X style code(s) found on page",
        f"Matched SKUs: {', '.join(codes[:8])}",
        *[f"  Found: {loc}" for loc in sku_scan["matched_in"][:3]],
        f"Search page: {page.url}",
    ]
    for i, sp in enumerate(sku_scan["sample_products"][:5], 1):
        sku  = sp.get("sku", "")
        name = sp.get("name", "Unknown")[:70]
        price = sp.get("price", "N/A")
        proof.append(f"  {i}. {'[' + sku + '] ' if sku else ''}{name} — {price}")
    return result, proof


def _apply_brand_match(result: dict, samples: list, proof: List[str], page) -> tuple:
    result["sells_twisted_x"] = True
    result["confidence"]       = "high"
    result["page_url"]         = page.url
    result["sample_products"]  = samples[:5]

    proof += [
        "VERIFIED: Twisted X found in product context (brand name in product links/cards)",
        f"Brand-in-product match: {len(samples)} product(s) contain 'Twisted X'",
        *[f"  {i}. {sp.get('name', '?')[:80]}" for i, sp in enumerate(samples[:5], 1)],
        f"Found on: {page.url}",
        "Note: No SKU fingerprint match (retailer may use different style codes)",
    ]
    return result, proof


def _apply_no_match(result: dict, validation: dict, proof: List[str], page) -> tuple:
    result["sells_twisted_x"] = False
    result["confidence"]       = "high"
    proof += [
        "No Twisted X products found on this site",
        f"SKU scan: 0 of {len(TX_STYLE_CODES)} known style codes found on page",
    ]
    if method := validation.get("twisted_x_method"):
        if method != "not_found":
            proof.append(f"Detection method tried: {method}")
    if validation.get("error"):
        proof.append(f"Note: {validation['error']}")
    return result, proof


def _apply_block_state(result: dict, blocked_reasons: list, proof: List[str]) -> dict:
    result["blocked"]          = True
    result["blocked_reasons"]  = ", ".join(blocked_reasons[:5]) if blocked_reasons else "Unknown"
    result["confidence"]       = "low"
    result["sells_twisted_x"]  = None
    result["sells_footwear"]   = None

    proof.insert(0, "Twisted X: unknown (site blocked automated access). Manual check required.")
    proof.append("Site may be blocking automated access. Please verify manually.")
    if blocked_reasons:
        proof.append(f"Indicators: {', '.join(blocked_reasons[:5])}")
    result["proof"] = proof
    return result


def _determine_store_type(validation: dict, final_url: str) -> str:
    if "twistedx.com" in final_url.lower() or "twisted-x.com" in final_url.lower():
        return "brand_site"
    combined = validation.get("combined_status", "")
    mapping = {
        "has_products_sells_online":   "ecommerce",
        "has_products_in_store_only":  "company_store",
        "ecommerce_no_twisted_x":      "ecommerce",
    }
    if combined in mapping:
        return mapping[combined]
    if combined == "no_products_no_online" and validation.get("has_physical_store_indicators"):
        return "company_store"
    return "unknown"


def _empty_result(url: str, retailer: str) -> dict:
    return {
        "url": url, "retailer": retailer,
        "sells_twisted_x": False, "sells_footwear": None,
        "confidence": "low", "store_type": "unknown",
        "sells_online": False, "proof": [], "sample_products": [],
        "page_url": None, "checked_at": datetime.now().isoformat(),
        "error": None, "blocked": False, "blocked_reasons": None,
    }
