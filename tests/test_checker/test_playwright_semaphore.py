"""
Tests for checker/_playwright.py — browser concurrency semaphore.

No real browser is launched — sync_playwright and validate_url are mocked.
"""
import threading
import time
from unittest.mock import MagicMock, patch

from checker._playwright import _BROWSER_SEMAPHORE, playwright_check


def _make_mock_page(url="https://example.com"):
    page = MagicMock()
    page.url = url
    page.content.return_value = "<html><body></body></html>"
    page.inner_text.return_value = ""
    page.query_selector.return_value = None
    page.query_selector_all.return_value = []
    page.evaluate.return_value = None
    return page


def _patch_playwright(page):
    """Return a context manager stack that stubs out the full browser stack."""
    mock_browser  = MagicMock()
    mock_context  = MagicMock()
    mock_pw       = MagicMock()

    mock_context.new_page.return_value = page
    mock_browser.new_context.return_value = mock_context
    mock_pw.chromium.launch.return_value = mock_browser
    mock_pw.__enter__ = lambda s: mock_pw
    mock_pw.__exit__  = MagicMock(return_value=False)

    return mock_pw


def test_semaphore_starts_at_3():
    """_BROWSER_SEMAPHORE should allow exactly 3 concurrent acquisitions."""
    assert _BROWSER_SEMAPHORE._value == 3


def test_semaphore_limits_to_3_concurrent(monkeypatch):
    """
    Fire 6 playwright_check calls simultaneously.
    At most 3 should hold the semaphore at the same time.
    """
    peak_concurrent = []
    active = [0]
    lock = threading.Lock()

    page = _make_mock_page()
    mock_pw = _patch_playwright(page)

    original_check = None

    def slow_validate(url, page, **kwargs):
        with lock:
            active[0] += 1
            peak_concurrent.append(active[0])
        time.sleep(0.15)
        with lock:
            active[0] -= 1
        return {
            "sells_online": False, "sells_footwear": None,
            "combined_status": "", "twisted_x_method": "not_found",
            "error": None, "has_physical_store_indicators": False,
            "online_sales": {},
        }

    with patch("patchright.sync_api.sync_playwright", return_value=mock_pw), \
         patch("url_validator.check_url", side_effect=slow_validate), \
         patch("checker._playwright.detect_platform", return_value="generic"), \
         patch("checker._playwright.detect_blocked", return_value=(False, [])), \
         patch("checker._playwright._run_search", return_value={
             "found_match": False, "sku_scan": {"matched_codes": set(), "matched_in": [], "sample_products": []},
             "brand_found": False, "brand_samples": [], "page_url": None,
         }), \
         patch("checker._playwright._dual_page_scan", return_value=(
             {"matched_codes": set(), "matched_in": [], "sample_products": []}, False, []
         )):

        threads = [
            threading.Thread(
                target=playwright_check,
                args=("https://example.com/", "https://example.com/", "Example"),
            )
            for _ in range(6)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert max(peak_concurrent) <= 3, (
        f"Peak concurrent browsers was {max(peak_concurrent)}, expected ≤ 3"
    )


def test_semaphore_released_after_success(monkeypatch):
    """Semaphore value returns to 3 after a successful check."""
    page = _make_mock_page()
    mock_pw = _patch_playwright(page)

    before = _BROWSER_SEMAPHORE._value

    with patch("patchright.sync_api.sync_playwright", return_value=mock_pw), \
         patch("url_validator.check_url", return_value={
             "sells_online": False, "sells_footwear": None,
             "combined_status": "", "twisted_x_method": "not_found",
             "error": None, "has_physical_store_indicators": False,
             "online_sales": {},
         }), \
         patch("checker._playwright.detect_platform", return_value="generic"), \
         patch("checker._playwright.detect_blocked", return_value=(False, [])), \
         patch("checker._playwright._run_search", return_value={
             "found_match": False, "sku_scan": {"matched_codes": set(), "matched_in": [], "sample_products": []},
             "brand_found": False, "brand_samples": [], "page_url": None,
         }), \
         patch("checker._playwright._dual_page_scan", return_value=(
             {"matched_codes": set(), "matched_in": [], "sample_products": []}, False, []
         )):
        playwright_check("https://example.com/", "https://example.com/", "Example")

    assert _BROWSER_SEMAPHORE._value == before


def test_semaphore_released_after_exception(monkeypatch):
    """Semaphore value returns to 3 even when the browser raises an exception."""
    mock_pw = MagicMock()
    mock_pw.chromium.launch.side_effect = RuntimeError("browser crash")
    mock_pw.__enter__ = lambda s: mock_pw
    mock_pw.__exit__  = MagicMock(return_value=False)

    before = _BROWSER_SEMAPHORE._value

    with patch("patchright.sync_api.sync_playwright", return_value=mock_pw):
        result = playwright_check("https://example.com/", "https://example.com/", "Example")

    assert _BROWSER_SEMAPHORE._value == before
    assert result["error"] is not None
