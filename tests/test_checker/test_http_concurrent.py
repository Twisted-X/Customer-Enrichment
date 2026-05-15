"""
Tests for checker/_http.py — concurrent brand-path probing.

No network calls — http_get is fully mocked.
"""
import time
from unittest.mock import MagicMock, patch

import pytest

from checker._http import http_first_check, _TX_BRAND_PATHS


def _mock_response(status=200, html="", url="https://example.com/brands/twisted-x"):
    resp = MagicMock()
    resp.status_code = status
    resp.url = url
    resp.text = html
    resp.headers = {"content-type": "text/html"}
    resp.content = html.encode()
    return resp


_SKU_HTML = """
<html><head><title>Twisted X Boots</title></head><body>
  <div class="product-grid">
    <div class="product-tile">
      <h2>Twisted X Men's Work Boot</h2>
      <span class="sku">MCA0070</span>
      <span class="price">$149.99</span>
      <a href="/products/mca0070">Shop Now</a>
    </div>
    <div class="product-tile">
      <h2>Twisted X Women's Casual Boot</h2>
      <span class="sku">WCA0020</span>
      <span class="price">$129.99</span>
      <a href="/products/wca0020">Shop Now</a>
    </div>
  </div>
</body></html>
"""

_EMPTY_HTML = (
    "<html><head><title>No Results</title></head><body>"
    + "<p>No products found matching your search.</p>" * 20
    + "</body></html>"
)


@pytest.fixture(autouse=True)
def patch_http_get():
    """Replace http_get with a mock that returns 404 by default."""
    with patch("checker._http.http_get") as mock:
        mock.return_value = _mock_response(status=404)
        yield mock


def test_hit_on_first_matching_path(patch_http_get):
    """Returns definitive=True when any path contains a TX SKU."""
    hit_url = f"https://bootjack.com{_TX_BRAND_PATHS[0]}"

    def side_effect(url, timeout=5):
        if url == hit_url:
            return _mock_response(status=200, html=_SKU_HTML, url=url)
        return _mock_response(status=404)

    patch_http_get.side_effect = side_effect

    result = http_first_check("https://bootjack.com/")
    assert result["definitive"] is True
    assert result["sells_twisted_x"] is True
    assert result["confidence"] == "high"


def test_no_hit_returns_non_definitive(patch_http_get):
    """Returns definitive=False when no path has a TX SKU."""
    patch_http_get.return_value = _mock_response(status=404)

    result = http_first_check("https://example.com/")
    assert result["definitive"] is False
    assert result["sells_twisted_x"] is False


def test_all_paths_probed_when_no_hit(patch_http_get):
    """All brand paths are attempted when none produce a hit."""
    patch_http_get.return_value = _mock_response(status=404)

    http_first_check("https://example.com/")
    assert patch_http_get.call_count == len(_TX_BRAND_PATHS)


def test_200_with_no_sku_is_not_definitive(patch_http_get):
    """A 200 response without TX SKUs does not count as a hit."""
    patch_http_get.return_value = _mock_response(status=200, html=_EMPTY_HTML)

    result = http_first_check("https://example.com/")
    assert result["definitive"] is False


def test_concurrent_faster_than_sequential(patch_http_get):
    """
    Concurrent mode should finish well under n_paths * delay seconds.
    Each mock request sleeps 0.1s; sequential would take ~2s, concurrent ~0.15s.
    """
    def slow_response(url, timeout=5):
        time.sleep(0.1)
        return _mock_response(status=404)

    patch_http_get.side_effect = slow_response

    start = time.time()
    http_first_check("https://example.com/")
    elapsed = time.time() - start

    sequential_worst = len(_TX_BRAND_PATHS) * 0.1
    assert elapsed < sequential_worst * 0.5, (
        f"Took {elapsed:.2f}s — expected concurrent to be <{sequential_worst * 0.5:.2f}s"
    )


def test_blocked_page_not_reported_as_hit(patch_http_get):
    """A page returning Cloudflare challenge HTML is skipped."""
    blocked_html = "<html><body>Checking your browser before accessing...</body></html>"
    patch_http_get.return_value = _mock_response(status=200, html=blocked_html)

    result = http_first_check("https://example.com/")
    assert result["definitive"] is False


def test_hit_on_last_path(patch_http_get):
    """Returns a hit even if only the last path matches (all others are 404)."""
    last_path = _TX_BRAND_PATHS[-1]
    last_url = f"https://example.com{last_path}"

    def side_effect(url, timeout=5):
        if url == last_url:
            return _mock_response(status=200, html=_SKU_HTML, url=url)
        return _mock_response(status=404)

    patch_http_get.side_effect = side_effect

    result = http_first_check("https://example.com/")
    assert result["definitive"] is True
