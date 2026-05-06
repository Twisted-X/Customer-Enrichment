"""Loader for the shared brand/product-line keyword list.

The list lives in config/brand_indicators.json. Every consumer (verifier.py,
api_server.py, prompt-generation tools) imports from here so the keyword set
stays in sync — adding a new brand to the JSON propagates everywhere on next
restart.
"""
from __future__ import annotations

import json
import os
from typing import List

_HERE = os.path.dirname(os.path.abspath(__file__))
_PATH = os.path.join(_HERE, "config", "brand_indicators.json")

with open(_PATH, "r", encoding="utf-8") as _f:
    _DATA = json.load(_f)

BRANDS: List[str] = list(_DATA.get("brands", []))
PRODUCT_LINES: List[str] = list(_DATA.get("product_lines", []))
PRIMARY_BRAND_PAIR: List[str] = list(_DATA.get("primary_brand_pair", []))

# Combined list used by verifier.py for brand presence checks. Pre-lowercased
# at load time so callers don't need to lower() on every comparison.
ALL_INDICATORS: List[str] = [s.lower() for s in BRANDS + PRODUCT_LINES]
