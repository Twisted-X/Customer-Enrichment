"""
Tests for enrichment/_retail.py — retail classification and known-domain lookup.
"""
import pytest
from enrichment._retail import (
    classify_retail_type,
    domain_signals,
    KNOWN_DOMAIN_SIGNALS,
    _SELLS_TX,
    _NO_TX,
)


# ---------------------------------------------------------------------------
# classify_retail_type
# ---------------------------------------------------------------------------

class TestClassifyRetailType:
    def test_channel_row_is_not_retail(self):
        assert classify_retail_type(True, "clothing_store", True) == "not_retail"

    def test_channel_row_overrides_store_type(self):
        # Even if primary_type says "clothing_store", channel rows are not_retail
        assert classify_retail_type(True, "clothing_store", False) == "not_retail"

    def test_warehouse_primary_type_is_not_retail(self):
        assert classify_retail_type(False, "warehouse", True) == "not_retail"

    def test_storage_primary_type_is_not_retail(self):
        assert classify_retail_type(False, "storage", False) == "not_retail"

    def test_distribution_center_is_not_retail(self):
        assert classify_retail_type(False, "distribution_center", True) == "not_retail"

    def test_clothing_store_is_retail(self):
        assert classify_retail_type(False, "clothing_store", True) == "retail"

    def test_shoe_store_is_retail(self):
        assert classify_retail_type(False, "shoe_store", False) == "retail"

    def test_sporting_goods_store_is_retail(self):
        assert classify_retail_type(False, "sporting_goods_store", True) == "retail"

    def test_farm_supply_store_is_retail(self):
        assert classify_retail_type(False, "farm_supply_store", False) == "retail"

    def test_unknown_type_with_hours_is_retail(self):
        # No recognised primary_type but has_opening_hours → retail (Tier 2)
        assert classify_retail_type(False, "some_unknown_type", True) == "retail"

    def test_unknown_type_no_hours_is_unknown(self):
        assert classify_retail_type(False, "some_unknown_type", False) == "unknown"

    def test_none_primary_type_with_hours_is_retail(self):
        assert classify_retail_type(False, None, True) == "retail"

    def test_none_primary_type_no_hours_is_unknown(self):
        assert classify_retail_type(False, None, False) == "unknown"

    def test_empty_string_primary_type_no_hours_is_unknown(self):
        assert classify_retail_type(False, "", False) == "unknown"


# ---------------------------------------------------------------------------
# domain_signals
# ---------------------------------------------------------------------------

class TestDomainSignals:
    def test_known_tx_retailer(self):
        result = domain_signals("https://www.bootbarn.com/")
        assert result == _SELLS_TX

    def test_known_tx_retailer_without_www(self):
        result = domain_signals("https://bootbarn.com/boots/")
        assert result == _SELLS_TX

    def test_known_tx_retailer_http(self):
        result = domain_signals("http://cavenders.com/")
        assert result == _SELLS_TX

    def test_known_no_tx_retailer(self):
        result = domain_signals("https://brownsshoes.com/")
        assert result == _NO_TX

    def test_unknown_domain_returns_none(self):
        result = domain_signals("https://somerandomboutique.com/")
        assert result is None

    def test_subdomain_returns_none(self):
        # "shop.bootbarn.com" is not in the table (exact or www-stripped only)
        result = domain_signals("https://shop.bootbarn.com/")
        assert result is None

    def test_walmart_is_known_tx(self):
        assert domain_signals("https://www.walmart.com/browse/shoes") == _SELLS_TX

    def test_amazon_is_known_tx(self):
        assert domain_signals("https://amazon.com/s?k=twisted+x") == _SELLS_TX

    def test_invalid_url_returns_none(self):
        result = domain_signals("not_a_url")
        assert result is None

    def test_empty_string_returns_none(self):
        result = domain_signals("")
        assert result is None


# ---------------------------------------------------------------------------
# KNOWN_DOMAIN_SIGNALS completeness checks
# ---------------------------------------------------------------------------

class TestKnownDomainSignalsTable:
    def test_all_values_are_either_sells_tx_or_no_tx(self):
        for domain, signals in KNOWN_DOMAIN_SIGNALS.items():
            assert signals in (_SELLS_TX, _NO_TX), f"Unexpected signals for {domain}: {signals}"

    def test_sells_tx_signals_have_correct_shape(self):
        assert _SELLS_TX["sells_anything"] == "yes"
        assert _SELLS_TX["sells_shoes"] == "yes"
        assert _SELLS_TX["sells_twisted_x"] == "yes"

    def test_no_tx_signals_have_correct_shape(self):
        assert _NO_TX["sells_anything"] == "yes"
        assert _NO_TX["sells_shoes"] == "yes"
        assert _NO_TX["sells_twisted_x"] == "no"

    def test_no_domains_have_leading_www(self):
        for domain in KNOWN_DOMAIN_SIGNALS:
            assert not domain.startswith("www."), f"Domain should not have www prefix: {domain}"
