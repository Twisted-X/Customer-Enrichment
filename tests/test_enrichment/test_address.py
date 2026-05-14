"""
Tests for enrichment/_address.py — all pure functions, no network calls.
"""
import math
from enrichment._address import (
    normalize_zip,
    normalize_address_for_match,
    parse_places_address,
    address_match_confidence,
    address_matches,
)


# ---------------------------------------------------------------------------
# normalize_zip
# ---------------------------------------------------------------------------

class TestNormalizeZip:
    def test_5_digit_zip(self):
        assert normalize_zip("78701") == "78701"

    def test_9_digit_zip_with_dash(self):
        assert normalize_zip("78701-1234") == "78701"

    def test_9_digit_zip_no_dash(self):
        assert normalize_zip("787011234") == "78701"

    def test_4_digit_zip_returns_as_is(self):
        # Only 4 digits → returns all 4 (less than 5 available)
        assert normalize_zip("1234") == "1234"

    def test_none_returns_empty(self):
        assert normalize_zip(None) == ""

    def test_nan_returns_empty(self):
        assert normalize_zip(math.nan) == ""

    def test_empty_string_returns_empty(self):
        assert normalize_zip("") == ""

    def test_alpha_only_returns_empty(self):
        assert normalize_zip("ABCDE") == ""

    def test_mixed_alpha_numeric_extracts_digits(self):
        # "TX 78701" → digits "78701" → "78701"
        assert normalize_zip("TX 78701") == "78701"

    def test_integer_input(self):
        assert normalize_zip(78701) == "78701"

    def test_float_zip(self):
        assert normalize_zip(78701.0) == "78701"

    def test_zip_with_leading_zeros(self):
        assert normalize_zip("01234") == "01234"


# ---------------------------------------------------------------------------
# normalize_address_for_match
# ---------------------------------------------------------------------------

class TestNormalizeAddressForMatch:
    def test_normal_values(self):
        city, state, zip5 = normalize_address_for_match("Austin", "TX", "78701")
        assert city == "AUSTIN"
        assert state == "TX"
        assert zip5 == "78701"

    def test_lowercase_state_uppercased(self):
        _, state, _ = normalize_address_for_match("houston", "tx", "77002")
        assert state == "TX"

    def test_none_city_returns_empty(self):
        city, _, _ = normalize_address_for_match(None, "TX", "78701")
        assert city == ""

    def test_nan_state_returns_empty(self):
        _, state, _ = normalize_address_for_match("Austin", math.nan, "78701")
        assert state == ""

    def test_extra_whitespace_collapsed(self):
        city, _, _ = normalize_address_for_match("  San   Antonio  ", "TX", "78201")
        assert city == "SAN ANTONIO"

    def test_all_none(self):
        city, state, zip5 = normalize_address_for_match(None, None, None)
        assert city == state == zip5 == ""


# ---------------------------------------------------------------------------
# parse_places_address
# ---------------------------------------------------------------------------

class TestParsePlacesAddress:
    def test_standard_us_format(self):
        # "Street, City, STATE ZIP" — 3-part format
        city, state, zip5 = parse_places_address("123 Main St, Austin, TX 78701")
        assert city == "AUSTIN"
        assert state == "TX"
        assert zip5 == "78701"

    def test_with_usa_suffix(self):
        # "Street, City, STATE ZIP, USA" — 4-part format
        city, state, zip5 = parse_places_address("100 Boot Dr, Denver, CO 80203, USA")
        assert city == "DENVER"
        assert state == "CO"
        assert zip5 == "80203"

    def test_none_returns_empty_triple(self):
        assert parse_places_address(None) == ("", "", "")

    def test_empty_string_returns_empty_triple(self):
        assert parse_places_address("") == ("", "", "")

    def test_single_segment_returns_empty_triple(self):
        assert parse_places_address("Austin TX 78701") == ("", "", "")

    def test_9_digit_zip_truncated(self):
        _, _, zip5 = parse_places_address("100 Elm St, Houston, TX 77002-1234")
        assert zip5 == "77002"

    def test_city_with_multiple_words(self):
        city, state, _ = parse_places_address("45 Ranch Rd, San Antonio, TX 78205")
        assert city == "SAN ANTONIO"
        assert state == "TX"

    def test_city_with_multiple_words_and_usa(self):
        city, state, zip5 = parse_places_address("45 Ranch Rd, San Antonio, TX 78205, USA")
        assert city == "SAN ANTONIO"
        assert state == "TX"
        assert zip5 == "78205"

    def test_no_zip_in_state_zip_part(self):
        city, state, zip5 = parse_places_address("100 Main, Portland, OR 97201")
        assert zip5 == "97201"

    def test_returns_uppercase_city(self):
        city, _, _ = parse_places_address("10 Park Ave, new york, NY 10001")
        assert city == "NEW YORK"


# ---------------------------------------------------------------------------
# address_match_confidence
# ---------------------------------------------------------------------------

class TestAddressMatchConfidence:
    # Use the 4-part Google Places format (with ", USA") so state is parsed correctly.
    _AUSTIN_ADDR  = "100 Main St, Austin, TX 78701, USA"
    _HOUSTON_ADDR = "100 Elm St, Houston, TX 77002, USA"
    _KATY_ADDR    = "101 Katy Fwy, Katy, TX 77450, USA"

    def test_exact_zip_match_is_high(self):
        conf = address_match_confidence("AUSTIN", "TX", "78701", self._AUSTIN_ADDR)
        assert conf == "high"

    def test_same_city_different_zip_is_medium(self):
        conf = address_match_confidence("AUSTIN", "TX", "78702", self._AUSTIN_ADDR)
        assert conf == "medium"

    def test_state_mismatch_is_none(self):
        conf = address_match_confidence("AUSTIN", "CA", "78701", self._AUSTIN_ADDR)
        assert conf == "none"

    def test_city_mismatch_is_none(self):
        conf = address_match_confidence("HOUSTON", "TX", "77002", self._AUSTIN_ADDR)
        assert conf == "none"

    def test_no_our_data_is_none(self):
        conf = address_match_confidence("", "", "", self._AUSTIN_ADDR)
        assert conf == "none"

    def test_our_zip_absent_but_city_matches_is_medium(self):
        conf = address_match_confidence("AUSTIN", "TX", "", self._AUSTIN_ADDR)
        assert conf == "medium"

    def test_state_only_match_when_no_city_is_low(self):
        conf = address_match_confidence("", "TX", "", self._AUSTIN_ADDR)
        assert conf == "low"

    def test_hq_vs_store_zip_same_city_is_medium(self):
        # Zip 77449 (HQ) vs 77450 (store) — same Katy TX city
        conf = address_match_confidence("KATY", "TX", "77449", self._KATY_ADDR)
        assert conf == "medium"


# ---------------------------------------------------------------------------
# address_matches
# ---------------------------------------------------------------------------

class TestAddressMatches:
    _AUSTIN_ADDR = "100 Main St, Austin, TX 78701, USA"

    def test_true_on_high_confidence(self):
        assert address_matches("AUSTIN", "TX", "78701", self._AUSTIN_ADDR) is True

    def test_true_on_medium_confidence(self):
        assert address_matches("AUSTIN", "TX", "78702", self._AUSTIN_ADDR) is True

    def test_true_on_low_confidence(self):
        assert address_matches("", "TX", "", self._AUSTIN_ADDR) is True

    def test_false_on_state_mismatch(self):
        assert address_matches("AUSTIN", "CA", "78701", self._AUSTIN_ADDR) is False

    def test_false_when_no_data(self):
        assert address_matches("", "", "", self._AUSTIN_ADDR) is False
