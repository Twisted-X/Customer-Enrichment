"""
Tests for enrichment/_product.py — compute_online_sales_status (all 6 paths).

check_product_signals is not tested here because it requires either a
running api_server or network access; that belongs in integration tests.
"""
from enrichment._product import compute_online_sales_status
from enrichment._config import URL_COL


def _row(url="https://example.com", tx="unknown", anything="unknown", shoes="unknown"):
    return {URL_COL: url, "sells_twisted_x": tx, "sells_anything": anything, "sells_shoes": shoes}


class TestComputeOnlineSalesStatus:
    # ── Path 1: No website ────────────────────────────────────────────────────

    def test_no_url_returns_no_website(self):
        assert compute_online_sales_status({URL_COL: ""}) == "No Website"

    def test_none_url_returns_no_website(self):
        assert compute_online_sales_status({URL_COL: None}) == "No Website"

    def test_na_url_returns_no_website(self):
        assert compute_online_sales_status({URL_COL: "n/a"}) == "No Website"

    def test_tbd_url_returns_no_website(self):
        assert compute_online_sales_status({URL_COL: "TBD"}) == "No Website"

    def test_missing_url_key_returns_no_website(self):
        assert compute_online_sales_status({}) == "No Website"

    # ── Path 2: Sells Twisted X ───────────────────────────────────────────────

    def test_sells_twisted_x_yes_returns_correct_label(self):
        row = _row(tx="yes")
        assert compute_online_sales_status(row) == "Ecommerce Site : Sells Twisted X"

    def test_sells_twisted_x_yes_overrides_shoes(self):
        # Even if shoes=no, tx=yes wins
        row = _row(tx="yes", anything="yes", shoes="no")
        assert compute_online_sales_status(row) == "Ecommerce Site : Sells Twisted X"

    # ── Path 3: Opportunity ───────────────────────────────────────────────────

    def test_sells_shoes_but_not_tx_is_opportunity(self):
        row = _row(tx="no", anything="yes", shoes="yes")
        assert compute_online_sales_status(row) == "Ecommerce Site : Opportunity"

    def test_sells_shoes_unknown_tx_is_opportunity(self):
        row = _row(tx="unknown", anything="yes", shoes="yes")
        assert compute_online_sales_status(row) == "Ecommerce Site : Opportunity"

    # ── Path 4: Does not sell TX ──────────────────────────────────────────────

    def test_sells_anything_but_no_shoes_is_no_tx(self):
        row = _row(tx="no", anything="yes", shoes="no")
        assert compute_online_sales_status(row) == "Ecommerce Site : Does Not Sell Twisted X"

    # ── Path 5: No ecommerce ──────────────────────────────────────────────────

    def test_sells_nothing_is_no_ecommerce(self):
        row = _row(tx="no", anything="no", shoes="no")
        assert compute_online_sales_status(row) == "No Ecommerce"

    def test_sells_anything_no_returns_no_ecommerce(self):
        row = _row(tx="unknown", anything="no", shoes="unknown")
        assert compute_online_sales_status(row) == "No Ecommerce"

    # ── Path 6: Insufficient data (blank) ─────────────────────────────────────

    def test_all_unknown_returns_blank(self):
        row = _row(tx="unknown", anything="unknown", shoes="unknown")
        assert compute_online_sales_status(row) == ""

    def test_missing_sells_fields_returns_blank(self):
        row = {URL_COL: "https://example.com"}
        assert compute_online_sales_status(row) == ""

    # ── Edge cases ────────────────────────────────────────────────────────────

    def test_uppercase_yes_treated_correctly(self):
        # Values are lowercased internally; "YES" should match
        row = _row(tx="YES")
        assert compute_online_sales_status(row) == "Ecommerce Site : Sells Twisted X"

    def test_whitespace_in_values_ignored(self):
        row = _row(tx=" yes ")
        assert compute_online_sales_status(row) == "Ecommerce Site : Sells Twisted X"
