"""
url_validator — Playwright-based pre-filter for Twisted X retailer URLs.

All imports that worked against the old url_validator.py module continue to work:

    from url_validator import check_url, normalize_url, validate_urls
    import url_validator; url_validator._search_on_site(page, term)

Sub-modules
-----------
_constants   Selector lists and configuration constants (data only)
_brand       URL normalisation, brand detection, site classification
_browser     Playwright helpers: popup dismissal, search, category navigation
_detect      detect_twisted_x, detect_online_sales_capability, detect_footwear
_check       check_url orchestrator
_batch       validate_urls batch processor and CSV helpers
"""

from ._constants import TIMEOUT_MS, VALIDATION_TIMEOUT, SEARCH_GROWTH_RATIO
from ._brand import normalize_url
from ._browser import _search_on_site          # accessed as url_validator._search_on_site
from ._detect import detect_twisted_x, detect_online_sales_capability, detect_footwear
from ._check import check_url
from ._batch import validate_urls

__all__ = [
    # Constants
    'TIMEOUT_MS',
    'VALIDATION_TIMEOUT',
    'SEARCH_GROWTH_RATIO',
    # Public functions
    'normalize_url',
    'detect_twisted_x',
    'detect_online_sales_capability',
    'detect_footwear',
    'check_url',
    'validate_urls',
    # Private but accessed by checker/ via `import url_validator`
    '_search_on_site',
]
