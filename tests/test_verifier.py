"""
Tests for verifier.py — confidence scoring and product verification.
"""
from verifier import (
    calculate_confidence,
    add_confidence_scores,
    verify_product_against_block,
    verify_products_against_blocks,
)


# ---------------------------------------------------------------------------
# calculate_confidence
# ---------------------------------------------------------------------------

class TestCalculateConfidence:
    def _product(self, **kwargs):
        base = {"name": "", "price": "", "sku": "", "product_url": "", "evidence": {}}
        base.update(kwargs)
        return base

    def test_all_signals_present_is_high(self):
        product = self._product(
            name="Twisted X Men's Western Boot",
            price="$129.99",
            sku="MCA0070",
            product_url="https://bootbarn.com/products/mca0070",
            evidence={"name_snippet": "Twisted X Men's", "price_snippet": "$129"},
        )
        assert calculate_confidence(product) == "high"

    def test_exact_brand_name_alone_gives_medium(self):
        # _W_BRAND_EXACT = 3 == _MEDIUM_THRESHOLD
        product = self._product(name="Twisted X Slip-On")
        assert calculate_confidence(product) == "medium"

    def test_exact_brand_plus_sku_gives_high(self):
        # 3 (brand_exact) + 2 (sku) = 5 < 6, still medium
        # plus 1 (price) = 6 → high
        product = self._product(name="Twisted X Boot", sku="MCA0070", price="$99")
        assert calculate_confidence(product) == "high"

    def test_brand_exact_plus_sku_without_price_is_medium(self):
        # 3 + 2 = 5 → medium
        product = self._product(name="Twisted X Boot", sku="MCA0070")
        assert calculate_confidence(product) == "medium"

    def test_partial_brand_only_is_low(self):
        # _W_BRAND_PARTIAL = 1 < _MEDIUM_THRESHOLD = 3
        product = self._product(name="Twisted Leather Boot")
        assert calculate_confidence(product) == "low"

    def test_partial_brand_plus_sku_plus_price_is_medium(self):
        # 1 + 2 + 1 = 4 → medium
        product = self._product(name="Twisted Boot", sku="MCA0070", price="$99")
        assert calculate_confidence(product) == "medium"

    def test_no_signals_is_low(self):
        product = self._product(name="Random Boot")
        assert calculate_confidence(product) == "low"

    def test_empty_product_is_low(self):
        assert calculate_confidence({}) == "low"

    def test_price_alone_is_low(self):
        product = self._product(price="$99.99")
        assert calculate_confidence(product) == "low"

    def test_url_alone_is_low(self):
        product = self._product(product_url="https://site.com/products/abc")
        assert calculate_confidence(product) == "low"

    def test_sku_alone_is_low(self):
        # 2 < 3 → low
        product = self._product(sku="MCA0070")
        assert calculate_confidence(product) == "low"

    def test_name_snippet_contributes_point(self):
        # brand_exact (3) + name_snippet (1) = 4 → medium still
        # brand_exact (3) + sku (2) + name_snippet (1) = 6 → high
        product = self._product(
            name="Twisted X Boot",
            sku="MCA0070",
            evidence={"name_snippet": "Twisted X"},
        )
        assert calculate_confidence(product) == "high"

    def test_exact_brand_check_is_case_insensitive(self):
        # name.lower() check — "TWISTED X" should count as exact
        product = self._product(name="TWISTED X WESTERN BOOT")
        assert calculate_confidence(product) in ("medium", "high")

    def test_partial_brand_does_not_double_count(self):
        # "twisted x" has both "twisted x" (exact) AND "twisted" → only exact counted
        product = self._product(name="Twisted X Boot")
        conf = calculate_confidence(product)
        # Score should be 3 (exact), not 4 (exact + partial)
        assert conf == "medium"


# ---------------------------------------------------------------------------
# add_confidence_scores
# ---------------------------------------------------------------------------

class TestAddConfidenceScores:
    def test_adds_confidence_to_products_without_it(self):
        products = [
            {"name": "Twisted X Boot", "sku": "MCA0070", "price": "$99", "product_url": "u"},
            {"name": "Random Boot"},
        ]
        result = add_confidence_scores(products)
        assert "confidence" in result[0]
        assert "confidence" in result[1]

    def test_does_not_overwrite_existing_confidence(self):
        products = [{"name": "Twisted X Boot", "confidence": "high"}]
        result = add_confidence_scores(products)
        assert result[0]["confidence"] == "high"

    def test_returns_same_list_object(self):
        products = [{"name": "Boot"}]
        result = add_confidence_scores(products)
        assert result is products

    def test_empty_list_returns_empty(self):
        assert add_confidence_scores([]) == []


# ---------------------------------------------------------------------------
# verify_product_against_block
# ---------------------------------------------------------------------------

class TestVerifyProductAgainstBlock:
    def _block(self, text="", html_snippet="", links=None, images=None):
        return {
            "text": text,
            "html_snippet": html_snippet,
            "links": links or [],
            "images": images or [],
        }

    def test_verified_when_name_and_brand_in_block(self):
        block = self._block(text="Twisted X Men's Western Boot MCA0070 $129.99")
        product = {
            "name": "Twisted X Men's Western Boot",
            "price": "$129.99",
            "sku": "MCA0070",
            "product_url": "",
            "image_url": "",
        }
        result = verify_product_against_block(product, block)
        assert result["verified"] is True
        assert result["issues"] == []

    def test_flagged_when_name_missing(self):
        block = self._block(text="Twisted X Western Boot")
        product = {"name": "", "price": "", "sku": "", "product_url": "", "image_url": ""}
        result = verify_product_against_block(product, block)
        assert result["verified"] is False
        assert any("name" in i.lower() for i in result["issues"])

    def test_flagged_when_brand_not_in_name_or_block(self):
        block = self._block(text="Generic Boot $50")
        product = {"name": "Generic Boot", "price": "$50", "sku": "", "product_url": "", "image_url": ""}
        result = verify_product_against_block(product, block)
        assert result["verified"] is False
        assert any("twisted x" in i.lower() for i in result["issues"])

    def test_price_mismatch_adds_issue_but_does_not_reject(self):
        block = self._block(text="Twisted X Western Boot $129.99")
        product = {
            "name": "Twisted X Western Boot",
            "price": "$999.00",
            "sku": "",
            "product_url": "",
            "image_url": "",
        }
        result = verify_product_against_block(product, block)
        # Price issue is non-critical; brand is present → verified
        price_issues = [i for i in result["issues"] if "price" in i.lower()]
        assert len(price_issues) == 1
        assert result["verified"] is True  # only name/brand issues reject

    def test_sku_not_in_block_adds_issue_but_does_not_reject(self):
        block = self._block(text="Twisted X Western Boot $129.99")
        product = {
            "name": "Twisted X Western Boot",
            "price": "$129.99",
            "sku": "FAKECODE",
            "product_url": "",
            "image_url": "",
        }
        result = verify_product_against_block(product, block)
        sku_issues = [i for i in result["issues"] if "sku" in i.lower()]
        assert len(sku_issues) == 1
        assert result["verified"] is True

    def test_product_url_not_in_links_adds_issue(self):
        block = self._block(
            text="Twisted X Boot",
            links=[{"href": "https://site.com/boots"}],
        )
        product = {
            "name": "Twisted X Boot",
            "price": "",
            "sku": "",
            "product_url": "https://site.com/completely-different",
            "image_url": "",
        }
        result = verify_product_against_block(product, block)
        url_issues = [i for i in result["issues"] if "url" in i.lower()]
        assert len(url_issues) == 1

    def test_result_product_includes_verification_status(self):
        block = self._block(text="Twisted X Men's Boot MCA0070")
        product = {"name": "Twisted X Men's Boot", "price": "", "sku": "MCA0070",
                   "product_url": "", "image_url": ""}
        result = verify_product_against_block(product, block)
        assert "verification_status" in result["product"]
        assert result["product"]["verification_status"] in ("verified", "flagged")


# ---------------------------------------------------------------------------
# verify_products_against_blocks
# ---------------------------------------------------------------------------

class TestVerifyProductsAgainstBlocks:
    def _block(self, text):
        return {"text": text, "html_snippet": "", "links": [], "images": []}

    def test_verified_products_have_confidence(self):
        blocks = [self._block("Twisted X Men's Boot MCA0070 $129.99")]
        products = [{"name": "Twisted X Men's Boot", "price": "$129.99", "sku": "MCA0070",
                     "product_url": "", "image_url": ""}]
        result = verify_products_against_blocks(products, blocks)
        assert len(result["verified_products"]) == 1
        assert "confidence" in result["verified_products"][0]

    def test_stats_total_equals_input_count(self):
        blocks = [self._block("Twisted X Boot"), self._block("Generic Boot")]
        products = [
            {"name": "Twisted X Boot", "price": "", "sku": "", "product_url": "", "image_url": ""},
            {"name": "Generic Boot", "price": "", "sku": "", "product_url": "", "image_url": ""},
        ]
        result = verify_products_against_blocks(products, blocks)
        stats = result["verification_stats"]
        assert stats["total_input"] == 2
        assert stats["verified"] + stats["flagged"] == 2

    def test_extra_product_without_block_is_flagged(self):
        blocks = [self._block("Twisted X Boot")]
        products = [
            {"name": "Twisted X Boot", "price": "", "sku": "", "product_url": "", "image_url": ""},
            {"name": "Extra Product", "price": "", "sku": "", "product_url": "", "image_url": ""},
        ]
        result = verify_products_against_blocks(products, blocks)
        assert result["verification_stats"]["flagged"] >= 1

    def test_empty_inputs_return_empty_results(self):
        result = verify_products_against_blocks([], [])
        assert result["verified_products"] == []
        assert result["flagged_products"] == []
        assert result["verification_stats"]["total_input"] == 0
