"""
Tests for checker/_serp.py — in-memory TTL cache.

No network calls are made — SerpApi is fully mocked.
"""
import time
from unittest.mock import patch, MagicMock

import pytest

from checker._serp import serp_check, _cache, _cache_lock, _CACHE_TTL_S


def _make_response(items):
    """Build a minimal SerpApi JSON response."""
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = {
        "organic_results": items,
        "search_information": {"total_results": len(items)},
    }
    return mock


_BOOT_ITEM = {
    "title": "Twisted X Men's Work Boot",
    "link":  "https://www.bootbarn.com/products/twisted-x-work-boot",
    "snippet": "Buy now — $189.99. Free shipping.",
}


@pytest.fixture(autouse=True)
def clear_cache():
    """Wipe the cache before and after every test."""
    with _cache_lock:
        _cache.clear()
    yield
    with _cache_lock:
        _cache.clear()


def test_cache_miss_calls_api():
    """First call for a domain hits the API."""
    with patch("checker._serp.SERPAPI_KEY", "fake-key"), \
         patch("checker._serp.requests.get", return_value=_make_response([_BOOT_ITEM])) as mock_get:
        serp_check("https://www.bootbarn.com/")
        assert mock_get.call_count == 1


def test_cache_hit_skips_api():
    """Second call for the same domain returns cached result without hitting the API."""
    with patch("checker._serp.SERPAPI_KEY", "fake-key"), \
         patch("checker._serp.requests.get", return_value=_make_response([_BOOT_ITEM])) as mock_get:
        r1 = serp_check("https://www.bootbarn.com/")
        r2 = serp_check("https://www.bootbarn.com/")
        assert mock_get.call_count == 1  # API called only once
        assert r1 == r2


def test_cache_hit_strips_www():
    """www.bootbarn.com and bootbarn.com resolve to the same cache entry."""
    with patch("checker._serp.SERPAPI_KEY", "fake-key"), \
         patch("checker._serp.requests.get", return_value=_make_response([_BOOT_ITEM])) as mock_get:
        serp_check("https://www.bootbarn.com/")
        serp_check("https://bootbarn.com/some/path")
        assert mock_get.call_count == 1


def test_cache_different_domains_call_api_separately():
    """Different domains each trigger their own API call."""
    with patch("checker._serp.SERPAPI_KEY", "fake-key"), \
         patch("checker._serp.requests.get", return_value=_make_response([_BOOT_ITEM])) as mock_get:
        serp_check("https://www.bootbarn.com/")
        serp_check("https://www.cavenders.com/")
        assert mock_get.call_count == 2


def test_cache_expires_after_ttl():
    """A cache entry is re-fetched after its TTL expires."""
    with patch("checker._serp.SERPAPI_KEY", "fake-key"), \
         patch("checker._serp.requests.get", return_value=_make_response([_BOOT_ITEM])) as mock_get:
        serp_check("https://www.bootbarn.com/")
        # Manually expire the cache entry
        with _cache_lock:
            domain = "bootbarn.com"
            result, _ = _cache[domain]
            _cache[domain] = (result, time.time() - 1)  # expired 1 second ago
        serp_check("https://www.bootbarn.com/")
        assert mock_get.call_count == 2


def test_no_key_skips_cache():
    """When SERPAPI_KEY is blank the function returns immediately without caching."""
    with patch("checker._serp.SERPAPI_KEY", ""), \
         patch("checker._serp.requests.get") as mock_get:
        result = serp_check("https://www.bootbarn.com/")
        assert mock_get.call_count == 0
        assert result["definitive"] is False
        with _cache_lock:
            assert len(_cache) == 0
