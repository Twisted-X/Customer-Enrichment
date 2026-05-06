"""
Root conftest — must run before any module with startup checks is imported.

Sets environment variables at module load time so that:
  - enrichment/_config.py does not raise for missing GOOGLE_PLACES_API_KEY
  - config.py does not raise for an empty/missing SKU database in CI
"""
import os
import sys

# MUST be at module level (not inside fixtures) so env vars are in place
# before pytest imports test modules — which trigger top-level module imports.
os.environ.setdefault("GOOGLE_PLACES_API_KEY", "test_api_key_for_ci_only")
os.environ.setdefault("MIN_EXPECTED_STYLE_CODES", "0")
os.environ.setdefault("ENRICHMENT_TTL_DAYS", "30")
os.environ.setdefault("ENABLE_PRODUCT_CHECK", "false")

# Ensure the repo root is on sys.path so all modules are importable from tests/
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
