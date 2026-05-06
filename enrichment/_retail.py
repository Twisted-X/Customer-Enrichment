"""
Retail type classification and known-domain lookup table.

Public API:
    classify_retail_type(row_is_channel, primary_type, has_opening_hours) -> str
    domain_signals(url)                                                    -> dict | None
"""
from __future__ import annotations

from urllib.parse import urlparse

from ._config import RETAIL_PRIMARY_TYPES, NONRETAIL_PRIMARY_TYPES

# ---------------------------------------------------------------------------
# Known-domain lookup table
# For sites that block automated scrapers we hardcode the signals.
#   "tx"  → sells_anything=yes, sells_shoes=yes, sells_twisted_x=yes
#   "no"  → sells_anything=yes, sells_shoes=yes, sells_twisted_x=no
# ---------------------------------------------------------------------------

_SELLS_TX = {"sells_anything": "yes", "sells_shoes": "yes", "sells_twisted_x": "yes"}
_NO_TX    = {"sells_anything": "yes", "sells_shoes": "yes", "sells_twisted_x": "no"}

KNOWN_DOMAIN_SIGNALS: dict = {
    # Confirmed Twisted X retailers
    "bootbarn.com":              _SELLS_TX,
    "cavenders.com":             _SELLS_TX,
    "murdochs.com":              _SELLS_TX,
    "tractorsupply.com":         _SELLS_TX,
    "walmart.com":               _SELLS_TX,
    "academy.com":               _SELLS_TX,
    "dillards.com":              _SELLS_TX,
    "amazon.com":                _SELLS_TX,
    "billyswesternwear.com":     _SELLS_TX,
    "bomgaars.com":              _SELLS_TX,
    "brownsshoefitco.com":       _SELLS_TX,
    "elliottsboots.com":         _SELLS_TX,
    "frenchsbootsandshoes.com":  _SELLS_TX,
    "atwoods.com":               _SELLS_TX,
    "bigronline.com":            _SELLS_TX,
    "buchheits.com":             _SELLS_TX,
    "coastalcountry.com":        _SELLS_TX,
    "dbsupply.com":              _SELLS_TX,
    "fcfarmandhome.com":         _SELLS_TX,
    "jaxgoods.com":              _SELLS_TX,
    "north40.com":               _SELLS_TX,
    "theisens.com":              _SELLS_TX,
    "ruralking.com":             _SELLS_TX,
    "scheels.com":               _SELLS_TX,
    "shoesensation.com":         _SELLS_TX,
    "supershoes.com":            _SELLS_TX,
    "runnings.com":              _SELLS_TX,
    "farmstore.com":             _SELLS_TX,
    # Ecommerce sites confirmed NOT selling Twisted X
    "brownsshoes.com":           _NO_TX,
    "gebos.com":                 _NO_TX,
    "shiptonsbigr.com":          _NO_TX,
    "farmandhomesupply.com":     _NO_TX,
}


def domain_signals(url: str) -> dict | None:
    """
    Fast-path lookup against KNOWN_DOMAIN_SIGNALS.
    Returns the signals dict if the domain is known, None otherwise.
    """
    try:
        host = urlparse(url).hostname or url
        key  = host.lower().removeprefix("www.")
        return KNOWN_DOMAIN_SIGNALS.get(key)
    except Exception:
        return None


def classify_retail_type(
    row_is_channel: bool,
    primary_type: str | None,
    has_opening_hours: bool,
) -> str:
    """
    Strict retail classification — no scoring, no guessing.
    Returns 'retail', 'not_retail', or 'unknown'.

    Tier 1 (certain):
      - Channel row (ecom/online suffix)        → not_retail
      - primary_type is warehouse/storage       → not_retail
      - primary_type is store type + hours      → retail

    Tier 2 (~95% certain):
      - primary_type is store type (no hours)   → retail
      - has opening hours (no store type)       → retail

    Tier 3 (honest):
      - anything else                           → unknown
    """
    if row_is_channel:
        return "not_retail"

    pt = (primary_type or "").lower().strip()

    if pt in NONRETAIL_PRIMARY_TYPES:
        return "not_retail"

    if pt in RETAIL_PRIMARY_TYPES:
        return "retail"

    if has_opening_hours:
        return "retail"

    return "unknown"
