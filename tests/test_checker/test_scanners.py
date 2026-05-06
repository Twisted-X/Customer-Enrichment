"""
Tests for checker/_scanners.py — scan_html_for_skus (no Playwright needed).

scan_page_for_skus and find_brand_in_product_context require a live Playwright
page — those are integration tests only.

TX_STYLE_CODES is patched per-test to a small known set so tests are fast
and independent of the xlsx file.
"""
import unittest.mock
import pytest
import config as _config_module

from checker._scanners import scan_html_for_skus

_FAKE_CODES = {"MCA0070", "ICA0035", "WDM0093", "BACKPKECO001"}


@pytest.fixture(autouse=True)
def patch_sku_codes():
    """Replace TX_STYLE_CODES with a small known set for every test in this module."""
    with unittest.mock.patch.object(_config_module, "TX_STYLE_CODES", _FAKE_CODES):
        yield


class TestScanHtmlForSkus:
    def test_no_skus_returns_empty_scan(self):
        html = "<html><body>No products here, just a landing page.</body></html>"
        result = scan_html_for_skus(html)
        assert result["matched_codes"] == set()
        assert result["matched_in"] == []
        assert result["sample_products"] == []

    def test_known_sku_in_body_text_detected(self):
        html = "<html><body><p>MCA0070 Western Boot $129.99</p></body></html>"
        result = scan_html_for_skus(html)
        assert "MCA0070" in result["matched_codes"]

    def test_known_sku_in_href_detected(self):
        html = '<html><body><a href="/products/MCA0070-western-boot">Boot</a></body></html>'
        result = scan_html_for_skus(html)
        assert "MCA0070" in result["matched_codes"]

    def test_multiple_skus_all_detected(self):
        html = "<body>MCA0070 and ICA0035 are both in stock</body>"
        result = scan_html_for_skus(html)
        assert "MCA0070" in result["matched_codes"]
        assert "ICA0035" in result["matched_codes"]

    def test_sku_in_script_tag_not_detected(self):
        # Script tags are stripped before scanning
        html = "<html><body><script>var sku='MCA0070';</script><p>No products</p></body></html>"
        result = scan_html_for_skus(html)
        assert "MCA0070" not in result["matched_codes"]

    def test_sku_in_style_tag_not_detected(self):
        html = "<html><head><style>.MCA0070 { color: red; }</style></head><body>No products</body></html>"
        result = scan_html_for_skus(html)
        assert "MCA0070" not in result["matched_codes"]

    def test_short_token_not_matched(self):
        # Tokens must be 4-15 chars; random 3-char words never match real codes
        html = "<body>ABC DEF GHI product page</body>"
        result = scan_html_for_skus(html)
        assert result["matched_codes"] == set()

    def test_matched_in_contains_provenance_string(self):
        html = "<body><a href='/p/MCA0070'>Boot</a></body>"
        result = scan_html_for_skus(html)
        assert any("MCA0070" in s for s in result["matched_in"])

    def test_sample_products_have_product_url_key(self):
        html = '<body><a href="/products/MCA0070-boot">View boot</a></body>'
        result = scan_html_for_skus(html)
        if result["sample_products"]:
            assert all("product_url" in p for p in result["sample_products"])

    def test_empty_html_returns_empty_scan(self):
        result = scan_html_for_skus("")
        assert result["matched_codes"] == set()

    def test_none_html_returns_empty_scan(self):
        result = scan_html_for_skus(None)
        assert result["matched_codes"] == set()

    def test_returns_at_most_5_matched_in(self):
        # Even with many matching codes, matched_in is capped at 5
        skus = " ".join(_FAKE_CODES) * 3
        html = f"<body>{skus}</body>"
        result = scan_html_for_skus(html)
        assert len(result["matched_in"]) <= 5

    def test_case_insensitive_matching(self):
        # Tokens are uppercased before set intersection
        html = "<body>mca0070 western boot</body>"
        result = scan_html_for_skus(html)
        assert "MCA0070" in result["matched_codes"]

    def test_empty_tx_style_codes_returns_empty(self):
        with unittest.mock.patch.object(_config_module, "TX_STYLE_CODES", set()):
            result = scan_html_for_skus("<body>MCA0070</body>")
            assert result["matched_codes"] == set()

    def test_long_sku_beyond_15_chars_not_matched(self):
        # Token regex only matches 4-15 char tokens; 16+ chars skip
        html = "<body>TOOLONGSKUCODEABCDEFGH12345</body>"
        result = scan_html_for_skus(html)
        assert result["matched_codes"] == set()
