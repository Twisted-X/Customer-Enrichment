"""
Shared TypedDict definitions and result factories for the checker pipeline.

Every function in this package returns one of these typed dicts so callers never
have to guess which keys are present or what type they hold.
"""
from __future__ import annotations
from datetime import datetime
from typing import List, Optional
from typing_extensions import TypedDict


class SampleProduct(TypedDict):
    name: str
    price: str
    sku: str
    image: str
    product_url: str


class ScanResult(TypedDict):
    """Returned by scan_page_for_skus / scan_html_for_skus."""
    matched_codes: set          # set[str] — e.g. {"MCA0070", "ICA0035"}
    matched_in: List[str]       # human-readable provenance, e.g. "MCA0070 in page text"
    sample_products: List[SampleProduct]



class SearchOutcome(TypedDict):
    """
    Returned by each platform search strategy (_search.py).

    found_match: True  → SKU or brand found; caller should stop trying other strategies.
    sku_scan: the scan result from the winning page (may be empty if brand_found).
    brand_found: True  → brand detected in product context (no SKU match).
    brand_samples: products found via brand context scan.
    page_url: URL of the page where we found the match (for proof).
    """
    found_match: bool
    sku_scan: ScanResult
    brand_found: bool
    brand_samples: List[SampleProduct]
    page_url: Optional[str]


def empty_scan() -> ScanResult:
    """Convenience: return a scan result with nothing found."""
    return {"matched_codes": set(), "matched_in": [], "sample_products": []}


def empty_search() -> SearchOutcome:
    """Convenience: return a search outcome with nothing found."""
    return {
        "found_match": False,
        "sku_scan": empty_scan(),
        "brand_found": False,
        "brand_samples": [],
        "page_url": None,
    }


def new_check_result(url: str, retailer: str, error: Optional[str] = None) -> dict:
    """
    Build the default CheckResponse-shaped dict.

    All early-return and error paths use this factory so the response shape
    is always consistent — no missing fields, no wrong defaults.
    """
    return {
        "url":             url,
        "retailer":        retailer,
        "sells_twisted_x": False,
        "sells_footwear":  None,
        "confidence":      "low",
        "store_type":      "unknown",
        "sells_online":    False,
        "proof":           [],
        "sample_products": [],
        "page_url":        None,
        "checked_at":      datetime.now().isoformat(),
        "error":           error,
        "blocked":         False,
    }
