"""
Verifier - Post-extraction validation to catch LLM hallucinations

Verifies LLM-extracted products against original product blocks
from the /api/scrape endpoint. Pure deterministic logic, no LLM calls.
"""
import re
from typing import Dict, List
from difflib import SequenceMatcher


def calculate_confidence(product: Dict) -> str:
    """
    Calculate confidence level for a verified product.
    
    Returns:
        'high', 'medium', or 'low'
    """
    score = 0

    name = product.get("name", "")
    if "twisted x" in name.lower():
        score += 3
    elif "twisted" in name.lower():
        score += 1

    if product.get("price"):
        score += 1
    if product.get("sku"):
        score += 2
    if product.get("product_url"):
        score += 1

    evidence = product.get("evidence", {})
    if evidence.get("name_snippet"):
        score += 1
    if evidence.get("price_snippet"):
        score += 1

    if score >= 6:
        return "high"
    elif score >= 3:
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
                    similarity = SequenceMatcher(None, name_lower[:50], original_text[:200]).ratio()
                    if similarity < 0.3:
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

    # Brand verification
    name_lower = name.lower() if name else ""
    brand_indicators = [
        "twisted x", "twistedx", "twisted x work",
        "black star", "black star boots",
        "cellsole", "cell sole", "cellstretch",
        "hooey",
        "tech x", "tech-x", "feather x",
        "chukka", "driving moc", "driving moccasin",
        "top hand", "zero-x", "zero x", "ruff stock",
        "all around", "horseman", "western work"
    ]

    name_has_brand = any(ind in name_lower for ind in brand_indicators)
    block_has_brand = any(ind in original_text or ind in original_html for ind in brand_indicators)

    if not name_has_brand and not block_has_brand:
        issues.append("Cannot confirm this is a Twisted X Global Brands product")

    # Only name/brand issues cause rejection
    critical_issues = [i for i in issues if "name" in i.lower() or "brand" in i.lower()]
    verified = len(critical_issues) == 0

    result_product = extracted_product.copy()
    result_product["verification_status"] = "verified" if verified else "flagged"
    result_product["verification_issues"] = issues

    return {
        "verified": verified,
        "issues": issues,
        "product": result_product
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
        matching_block = None

        if i < len(original_products):
            matching_block = original_products[i]
        else:
            # Try fuzzy matching by name
            extracted_name = extracted.get("name", "").lower()
            best_match = None
            best_similarity = 0
            for block in original_products:
                block_text = block.get("text", "").lower()
                if extracted_name:
                    similarity = SequenceMatcher(None, extracted_name[:50], block_text[:200]).ratio()
                    if similarity > best_similarity:
                        best_similarity = similarity
                        best_match = block

            if best_similarity > 0.3:
                matching_block = best_match

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
