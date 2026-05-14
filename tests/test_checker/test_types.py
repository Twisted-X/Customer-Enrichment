"""
Tests for checker/_types.py — result factory functions.
"""
from datetime import datetime
from checker._types import empty_scan, empty_search, new_check_result


class TestEmptyScan:
    def test_has_matched_codes_as_empty_set(self):
        result = empty_scan()
        assert "matched_codes" in result
        assert isinstance(result["matched_codes"], set)
        assert len(result["matched_codes"]) == 0

    def test_has_matched_in_as_empty_list(self):
        result = empty_scan()
        assert result["matched_in"] == []

    def test_has_sample_products_as_empty_list(self):
        result = empty_scan()
        assert result["sample_products"] == []

    def test_returns_independent_objects_each_call(self):
        a = empty_scan()
        b = empty_scan()
        a["matched_codes"].add("MCA0070")
        assert "MCA0070" not in b["matched_codes"]


class TestEmptySearch:
    def test_found_match_is_false(self):
        result = empty_search()
        assert result["found_match"] is False

    def test_brand_found_is_false(self):
        result = empty_search()
        assert result["brand_found"] is False

    def test_brand_samples_is_empty_list(self):
        result = empty_search()
        assert result["brand_samples"] == []

    def test_page_url_is_none(self):
        result = empty_search()
        assert result["page_url"] is None

    def test_sku_scan_is_empty_scan(self):
        result = empty_search()
        assert result["sku_scan"]["matched_codes"] == set()
        assert result["sku_scan"]["matched_in"] == []


class TestNewCheckResult:
    def test_url_field_set(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["url"] == "https://bootbarn.com"

    def test_retailer_field_set(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["retailer"] == "bootbarn"

    def test_sells_twisted_x_defaults_false(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["sells_twisted_x"] is False

    def test_sells_footwear_defaults_none(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["sells_footwear"] is None

    def test_confidence_defaults_low(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["confidence"] == "low"

    def test_sells_online_defaults_false(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["sells_online"] is False

    def test_proof_defaults_empty_list(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["proof"] == []

    def test_sample_products_defaults_empty_list(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["sample_products"] == []

    def test_page_url_defaults_none(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["page_url"] is None

    def test_error_defaults_none(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["error"] is None

    def test_error_set_when_provided(self):
        r = new_check_result("https://bootbarn.com", "bootbarn", error="timeout")
        assert r["error"] == "timeout"

    def test_blocked_defaults_false(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["blocked"] is False

    def test_store_type_defaults_unknown(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        assert r["store_type"] == "unknown"

    def test_checked_at_is_iso_datetime_string(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        # Should parse without error
        datetime.fromisoformat(r["checked_at"])

    def test_all_required_keys_present(self):
        r = new_check_result("https://bootbarn.com", "bootbarn")
        required = {
            "url", "retailer", "sells_twisted_x", "sells_footwear",
            "confidence", "store_type", "sells_online", "proof",
            "sample_products", "page_url", "checked_at", "error", "blocked",
        }
        assert required.issubset(r.keys())
