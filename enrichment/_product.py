"""
Product signal checking and NetSuite online-sales status computation.

Public API:
    check_product_signals(url)           -> dict  {sells_anything, sells_shoes, sells_twisted_x}
    compute_online_sales_status(row)     -> str   NetSuite dropdown value
"""
from __future__ import annotations

import logging

import requests

from ._config import (
    CHECK_API_URL, CHECK_API_TIMEOUT,
    URL_BLACKLIST, URL_COL,
)
from ._retail import domain_signals

log = logging.getLogger(__name__)


def check_product_signals(url: str) -> dict:
    """
    Determine whether a URL sells anything, sells shoes, and sells Twisted X.

    Fast path: checks KNOWN_DOMAIN_SIGNALS first — no HTTP call needed.
    Slow path: calls POST /api/check (requires api_server running).
    Never raises — errors and timeouts return all "unknown".

    Returns:
        {"sells_anything": "yes"|"no"|"unknown",
         "sells_shoes":    "yes"|"no"|"unknown",
         "sells_twisted_x":"yes"|"no"|"unknown"}
    """
    known = domain_signals(url)
    if known is not None:
        log.info("Product check: known domain hit → %s", url)
        return known

    def _b(val) -> str:
        if val is True:
            return "yes"
        if val is False:
            return "no"
        return "unknown"

    try:
        resp = requests.post(
            CHECK_API_URL,
            json={"url": url},
            timeout=CHECK_API_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        sells_online    = data.get("sells_online")
        sells_footwear  = data.get("sells_footwear")
        sells_twisted_x = data.get("sells_twisted_x")

        # sells_anything = yes if the site sells or carries anything at all.
        # sells_online alone can be False for physical-store sites that show
        # products without a buy button, so we fold in the other signals.
        if sells_online is True or sells_footwear is True or sells_twisted_x is True:
            sells_anything = "yes"
        elif sells_online is False and sells_footwear is False and sells_twisted_x is False:
            sells_anything = "no"
        else:
            sells_anything = "unknown"

        return {
            "sells_anything":  sells_anything,
            "sells_shoes":     _b(sells_footwear),
            "sells_twisted_x": _b(sells_twisted_x),
        }

    except Exception as exc:
        log.warning("Product check failed for %s: %s", url, exc)
        return {"sells_anything": "unknown", "sells_shoes": "unknown", "sells_twisted_x": "unknown"}


def compute_online_sales_status(row) -> str:
    """
    Map product-check signals to a NetSuite dropdown value.

    Priority order (first match wins):
      1. No website                                      → "No Website"
      2. sells_twisted_x = yes                          → "Ecommerce Site : Sells Twisted X"
      3. sells_anything=yes + sells_shoes=yes + tx≠yes  → "Ecommerce Site : Opportunity"
      4. sells_anything=yes + sells_shoes=no            → "Ecommerce Site : Does Not Sell Twisted X"
      5. sells_anything = no                            → "No Ecommerce"
      6. anything else                                  → "" (blank — not enough data)
    """
    website = str(row.get(URL_COL, "") or "").strip()
    if not website or website.lower() in URL_BLACKLIST:
        return "No Website"

    tx   = str(row.get("sells_twisted_x", "") or "").strip().lower()
    any_ = str(row.get("sells_anything",  "") or "").strip().lower()
    shoe = str(row.get("sells_shoes",     "") or "").strip().lower()

    if tx == "yes":
        return "Ecommerce Site : Sells Twisted X"
    if any_ == "yes" and shoe == "yes":
        return "Ecommerce Site : Opportunity"
    if any_ == "yes" and shoe == "no":
        return "Ecommerce Site : Does Not Sell Twisted X"
    if any_ == "no":
        return "No Ecommerce"
    return ""
