"""
Verifier - Post-extraction validation to catch LLM hallucinations

Verifies LLM-extracted products against original product blocks
from the /api/scrape endpoint. Pure deterministic logic, no LLM calls.
"""
import re
from typing import Dict, List
from difflib import SequenceMatcher

from brand_config import ALL_INDICATORS as _BRAND_INDICATORS


# ── Confidence scoring weights ───────────────────────────────────────────────
# A product accumulates points across signals; the totals collapse into a
# tri-level confidence string ('high' / 'medium' / 'low'). The weights below
# are heuristic — they reflect "what would a reviewer feel reassured by?":
#   - Brand explicitly in the product name is the strongest single signal.
#   - SKU presence is the next strongest (a fabricated SKU would have to match
#     the regex shape and be plausible).
#   - Each individual auxiliary field (price, URL, evidence snippet) adds 1.
#
# Adjust these only with care — they are surfaced in /api/verify responses and
# any change shifts what reviewers see at the boundary.
_W_BRAND_EXACT = 3        # "twisted x" appears in product name
_W_BRAND_PARTIAL = 1      # only "twisted" appears in product name
_W_HAS_PRICE = 1
_W_HAS_SKU = 2
_W_HAS_URL = 1
_W_HAS_NAME_SNIPPET = 1
_W_HAS_PRICE_SNIPPET = 1

_HIGH_THRESHOLD = 6   # "name + sku + price + url + a snippet" reaches 6
_MEDIUM_THRESHOLD = 3 # "name + at least one supporting field" reaches 3

# ── Name-vs-source fuzzy match thresholds ────────────────────────────────────
# When the LLM-extracted name doesn't appear verbatim in the source block,
# we fall back to a fuzzy ratio. NAME_SIMILARITY_FLOOR is the minimum
# difflib SequenceMatcher ratio at which we still consider the name a plausible
# match (below it, we flag the product). 0.3 is intentionally permissive
# because names are often truncated or reformatted on retailer sites.
NAME_SIMILARITY_FLOOR = 0.3
NAME_PREFIX_FOR_MATCH = 50      # only the first N chars of extracted name participate
SOURCE_PREFIX_FOR_MATCH = 200   # ...compared against the first M chars of source text


def calculate_confidence(product: Dict) -> str:
    """
    Calculate confidence level for a verified product.

    Returns:
        'high', 'medium', or 'low'
    """
    score = 0

    name = product.get("name", "")
    if "twisted x" in name.lower():
        score += _W_BRAND_EXACT
    elif "twisted" in name.lower():
        score += _W_BRAND_PARTIAL

    if product.get("price"):
        score += _W_HAS_PRICE
    if product.get("sku"):
        score += _W_HAS_SKU
    if product.get("product_url"):
        score += _W_HAS_URL

    evidence = product.get("evidence", {})
    if evidence.get("name_snippet"):
        score += _W_HAS_NAME_SNIPPET
    if evidence.get("price_snippet"):
        score += _W_HAS_PRICE_SNIPPET

    if score >= _HIGH_THRESHOLD:
        return "high"
    elif score >= _MEDIUM_THRESHOLD:
        return "medium"
    else:
        return "low"


def add_confidence_scores(products: List[Dict]) -> List[Dict]:
    """Add confidence scores to products that don't already have one."""
    for product in products:
        if "confidence" not in product:
            product["confidence"] = calculate_confidence(product)
    return products


def verify_product_against_block(extracted_product: Dict, original_block: Dict) -> Dict:
    """
    Verify an extracted product against its original ProductBlock.
    
    Checks name, price, SKU, product URL, and image URL against
    the source block's text, HTML, links, and images.
    
    Args:
        extracted_product: LLM-extracted product dict from Celigo
        original_block: ProductBlock dict from /api/scrape
        
    Returns:
        Dict with verified (bool), issues (list), product (dict with metadata)
    """
    issues = []
    original_text = original_block.get("text", "").lower()
    original_html = original_block.get("html_snippet", "").lower()
    original_links = [link.get("href", "").lower() for link in original_block.get("links", [])]
    original_images = [img.get("src", "").lower() for img in original_block.get("images", [])]

    # Name check
    name = extracted_product.get("name", "")
    if not name:
        issues.append("Missing product name")
    else:
        name_lower = name.lower()
        if name_lower not in original_text:
            name_words = name_lower.split()[:3]
            if len(name_words) >= 2:
                partial_name = " ".join(name_words)
                if partial_name not in original_text:
                    similarity = SequenceMatcher(
                        None,
                        name_lower[:NAME_PREFIX_FOR_MATCH],
                        original_text[:SOURCE_PREFIX_FOR_MATCH],
                    ).ratio()
                    if similarity < NAME_SIMILARITY_FLOOR:
                        issues.append(f"Name '{name[:50]}...' not found in source block")
            else:
                issues.append(f"Name '{name[:50]}...' not found in source block")

    # Price check
    price = extracted_product.get("price", "")
    if price:
        price_clean = re.sub(r'[^0-9.]', '', str(price))
        if price_clean and price_clean not in original_text:
            price_parts = price_clean.split('.')
            price_found = any(len(part) >= 2 and part in original_text for part in price_parts)
            if not price_found:
                issues.append(f"Price '{price}' not found in source block")

    # SKU check
    sku = extracted_product.get("sku", "")
    if sku and sku.lower() not in original_text:
        issues.append(f"SKU '{sku}' not found in source block")

    # Product URL check
    product_url = extracted_product.get("product_url", "")
    if product_url:
        url_lower = product_url.lower()
        url_found = any(url_lower in link or link in url_lower for link in original_links)
        if not url_found:
            issues.append("Product URL not found in source block links")

    # Image URL check
    image_url = extracted_product.get("image_url", "")
    if image_url:
        img_lower = image_url.lower()
        img_found = any(img_lower in img or img in img_lower for img in original_images)
        if not img_found:
            issues.append("Image URL not found in source block images")

    # Brand verification — single source of truth in config/brand_indicators.json
    name_lower = name.lower() if name else ""
    name_has_brand = any(ind in name_lower for ind in _BRAND_INDICATORS)
    block_has_brand = any(ind in original_text or ind in original_html for ind in _BRAND_INDICATORS)

    if not name_has_brand and not block_has_brand:
        issues.append("Cannot confirm this is a Twisted X Global Brands product")

    # Only name/brand issues cause rejection
    critical_issues = [i for i in issues if "name" in i.lower() or "brand" in i.lower()]
    verified = len(critical_issues) == 0

    # Build evidence snippets so calculate_confidence can award name/price points.
    # Snippets are short excerpts from the source block that confirm the field.
    name_snippet  = ""
    price_snippet = ""

    if name:
        name_key = name.lower()[:30]
        if name_key in original_text:
            idx = original_text.index(name_key)
            name_snippet = original_text[max(0, idx - 10): idx + 70].strip()

    if price:
        price_digits = re.sub(r'[^0-9.]', '', str(price))
        if price_digits and price_digits in original_text:
            idx = original_text.index(price_digits)
            price_snippet = original_text[max(0, idx - 5): idx + 25].strip()

    result_product = extracted_product.copy()
    result_product["verification_status"] = "verified" if verified else "flagged"
    result_product["verification_issues"] = issues
    result_product["evidence"] = {
        "name_snippet":  name_snippet,
        "price_snippet": price_snippet,
    }

    return {
        "verified": verified,
        "issues":   issues,
        "product":  result_product,
    }


def verify_products_against_blocks(extracted_products: List[Dict], original_products: List[Dict]) -> Dict:
    """
    Verify multiple LLM-extracted products against their original ProductBlocks.
    
    Matching strategy:
    1. Match by index (extracted[i] -> original[i])
    2. If index out of range, match by name similarity
    3. If no match found, flag the product
    
    Args:
        extracted_products: LLM-extracted products from Celigo
        original_products: ProductBlocks from /api/scrape
        
    Returns:
        Dict with verified_products, flagged_products, verification_stats
    """
    verified_products = []
    flagged_products = []

    for i, extracted in enumerate(extracted_products):
        # Use name-based fuzzy matching as the primary strategy for all products.
        # Index alignment is used only as a small tiebreaker so that when two
        # blocks have equal similarity scores the positionally correct one wins.
        # This handles LLM output that reorders or deduplicates products.
        extracted_name = extracted.get("name", "").lower()
        best_match     = None
        best_score     = 0.0

        for j, block in enumerate(original_products):
            block_text = block.get("text", "").lower()
            score = SequenceMatcher(None, extracted_name[:50], block_text[:200]).ratio()
            if j == i:
                score += 0.01   # slight boost for index alignment as tiebreaker
            if score > best_score:
                best_score = score
                best_match = block

        matching_block = best_match if best_score > 0.25 else None

        if matching_block:
            result = verify_product_against_block(extracted, matching_block)
            if result["verified"]:
                verified_products.append(result["product"])
            else:
                flagged_products.append({
                    "product": result["product"],
                    "issues": result["issues"]
                })
        else:
            flagged_products.append({
                "product": extracted,
                "issues": ["No matching product block found"]
            })

    verified_products = add_confidence_scores(verified_products)

    return {
        "verified_products": verified_products,
        "flagged_products": flagged_products,
        "verification_stats": {
            "total_input": len(extracted_products),
            "verified": len(verified_products),
            "flagged": len(flagged_products)
        }
    }
