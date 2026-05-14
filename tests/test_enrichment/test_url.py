"""
Tests for enrichment/_url.py — URL classification utilities.

check_url and bulk_check_urls are async + network; they belong in integration tests.
"""
from enrichment._url import (
    is_url_blank_or_invalid,
    classify_url,
    extract_root_domain,
)


# ---------------------------------------------------------------------------
# is_url_blank_or_invalid
# ---------------------------------------------------------------------------

class TestIsUrlBlankOrInvalid:
    def test_none_is_invalid(self):
        assert is_url_blank_or_invalid(None) is True

    def test_empty_string_is_invalid(self):
        assert is_url_blank_or_invalid("") is True

    def test_whitespace_only_is_invalid(self):
        assert is_url_blank_or_invalid("   ") is True

    def test_na_is_invalid(self):
        assert is_url_blank_or_invalid("n/a") is True

    def test_na_uppercase_is_invalid(self):
        assert is_url_blank_or_invalid("N/A") is True

    def test_tbd_is_invalid(self):
        assert is_url_blank_or_invalid("TBD") is True

    def test_none_word_is_invalid(self):
        assert is_url_blank_or_invalid("none") is True

    def test_dash_is_invalid(self):
        assert is_url_blank_or_invalid("-") is True

    def test_https_url_is_valid(self):
        assert is_url_blank_or_invalid("https://example.com") is False

    def test_http_url_is_valid(self):
        assert is_url_blank_or_invalid("http://example.com") is False

    def test_bare_domain_is_valid(self):
        assert is_url_blank_or_invalid("example.com") is False

    def test_domain_with_path_is_valid(self):
        assert is_url_blank_or_invalid("bootbarn.com/boots") is False

    def test_random_word_is_invalid(self):
        assert is_url_blank_or_invalid("notaurl") is True

    def test_number_only_is_invalid(self):
        assert is_url_blank_or_invalid("12345") is True

    def test_nan_float_is_invalid(self):
        import math
        assert is_url_blank_or_invalid(math.nan) is True


# ---------------------------------------------------------------------------
# classify_url
# ---------------------------------------------------------------------------

class TestClassifyUrl:
    def test_regular_website(self):
        assert classify_url("https://bootbarn.com") == "website"

    def test_facebook_is_social(self):
        assert classify_url("https://www.facebook.com/annswesternwear/") == "social"

    def test_instagram_is_social(self):
        assert classify_url("https://instagram.com/twistedxboots") == "social"

    def test_twitter_is_social(self):
        assert classify_url("https://twitter.com/twistedx") == "social"

    def test_x_com_is_social(self):
        assert classify_url("https://x.com/twistedx") == "social"

    def test_youtube_is_social(self):
        assert classify_url("https://youtube.com/channel/abc") == "social"

    def test_myshopify_is_marketplace(self):
        assert classify_url("https://mybrand.myshopify.com") == "marketplace"

    def test_etsy_is_marketplace(self):
        assert classify_url("https://www.etsy.com/shop/mystore") == "marketplace"

    def test_amazon_is_marketplace(self):
        assert classify_url("https://amazon.com/dp/B001234") == "marketplace"

    def test_google_maps_is_maps(self):
        assert classify_url("https://www.google.com/maps/place/Boot+Barn") == "maps"

    def test_maps_google_is_maps(self):
        assert classify_url("https://maps.google.com/?q=boot+barn") == "maps"

    def test_goo_gl_maps_is_maps(self):
        assert classify_url("https://goo.gl/maps/abc123") == "maps"

    def test_none_is_not_found(self):
        assert classify_url(None) == "not_found"

    def test_empty_string_is_not_found(self):
        assert classify_url("") == "not_found"

    def test_website_not_found_label_is_not_found(self):
        from enrichment._config import WEBSITE_NOT_FOUND_LABEL
        assert classify_url(WEBSITE_NOT_FOUND_LABEL) == "not_found"

    def test_linktr_ee_is_marketplace(self):
        assert classify_url("https://linktr.ee/myprofile") == "marketplace"


# ---------------------------------------------------------------------------
# extract_root_domain
# ---------------------------------------------------------------------------

class TestExtractRootDomain:
    def test_strips_path_from_website(self):
        assert extract_root_domain("https://www.bootbarn.com/boots/western/") == "https://www.bootbarn.com"

    def test_keeps_scheme_and_netloc(self):
        assert extract_root_domain("http://example.com/product/123") == "http://example.com"

    def test_adds_https_for_bare_domain(self):
        result = extract_root_domain("bootbarn.com")
        assert result == "https://bootbarn.com"

    def test_social_keeps_full_url(self):
        url = "https://www.facebook.com/annswesternwear/"
        assert extract_root_domain(url) == url

    def test_marketplace_keeps_full_url(self):
        url = "https://mybrand.myshopify.com/products"
        assert extract_root_domain(url) == url

    def test_maps_returns_empty(self):
        assert extract_root_domain("https://www.google.com/maps/place/Boot+Barn") == ""

    def test_none_returns_empty(self):
        assert extract_root_domain(None) == ""

    def test_empty_string_returns_empty(self):
        assert extract_root_domain("") == ""

    def test_website_not_found_label_returns_empty(self):
        from enrichment._config import WEBSITE_NOT_FOUND_LABEL
        assert extract_root_domain(WEBSITE_NOT_FOUND_LABEL) == ""
