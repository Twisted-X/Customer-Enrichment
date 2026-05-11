"""
DOM Cleaning Module - Extract product blocks from rendered pages

Production-ready extraction with three strategies:
1. Targeted: CSS selectors to find individual product cards
2. Segmented: Split cleaned page text into product blocks using price/action patterns
3. Fullpage: Last resort - return truncated cleaned text as single block

All strategies return a flat list of compact product blocks.
"""
import re
from typing import Dict, List
from patchright.sync_api import Page

# ── Payload limits ──
MAX_PRODUCTS = 300
MAX_TEXT_PER_BLOCK = 500
MAX_HTML_PER_BLOCK = 500
MAX_IMAGES_PER_BLOCK = 2
MAX_LINKS_PER_BLOCK = 3
MAX_FULLPAGE_TEXT = 8000
MAX_FULLPAGE_IMAGES = 20

# ── Junk image URL fragments (lowercase) ──
JUNK_IMAGE_PATTERNS = [
    "pixel", "1x1", "placeholder", "tracking", "spacer",
    "blank.gif", "logo", "icon", "badge", "banner",
    "social", "facebook", "twitter", "instagram", "pinterest",
    "payment", "visa", "mastercard", "paypal", "amex",
    "google-tag", "analytics", "cookiebot", ".gif?",
    "sprite", "spinner", "loading", "recaptcha",
    "data:image", "base64",
]

# ── Product card CSS selectors (ordered: specific → broad) ──
PRODUCT_SELECTORS = [
    # Data attributes (most reliable)
    "[data-product-id]",
    "[data-product]",
    "[data-testid*='product']",
    "[data-item-id]",
    "article[data-product-id]",
    # Common class patterns
    ".product-card", ".product-item", ".product-tile",
    ".product-grid-item", ".product-listing",
    ".productCard", ".ProductCard",
    # Platform-specific
    ".grid-product",            # Shopify
    ".collection-product",      # Shopify
    "li.product",               # WooCommerce
    ".s-result-item",           # Amazon-like
    ".search-result-item",
    ".plp-item",
    # Class-contains patterns
    "[class*='ProductCard']",
    "[class*='product-card']",
    "[class*='productCard']",
    "[class*='product-item']",
    "[class*='product-tile']",
    "[class*='product-grid']",
]

# ── Action text that signals end of a product listing ──
# Keep these specific to actual product actions (avoid banner "Shop Now" etc.)
# NOTE: "see price in cart" is NOT an action -- it's a price display that
# appears right before "Add to Cart" and would create a false boundary.
ACTION_PATTERNS = [
    "add to cart", "add to bag",
    "quick view", "quick shop", "view details",
    "select options", "choose options", "view product",
]

# ── Price alternatives (sites that hide the dollar amount) ──
PRICE_ALT_PATTERNS = [
    "see price in cart", "see price",
    "call for price", "price on request",
    "price in cart", "login for price", "log in for price",
]

# ── Lines/segments to skip in text segmentation ──
SKIP_PATTERNS = [
    "sort by", "filter", "show more", "load more", "sign in",
    "create account", "forgot password", "available filters",
    "of pages", "previous", "next page", "back to top",
    "enable accessibility", "skip to main", "skip to content",
    "store locator", "contact us", "order lookup", "search results",
    "customer service", "shipping", "return policy",
    "sign up", "subscribe", "newsletter",
]


def _is_product_image(src: str) -> bool:
    """Return True if src looks like a product image, not junk."""
    if not src or not src.startswith("http"):
        return False
    src_lower = src.lower()
    return not any(p in src_lower for p in JUNK_IMAGE_PATTERNS)


def _dedupe_images(images: List[Dict]) -> List[Dict]:
    """Remove duplicate images by src URL."""
    seen = set()
    unique = []
    for img in images:
        src = img.get("src", "")
        if src and src not in seen:
            seen.add(src)
            unique.append(img)
    return unique


def _segment_text_into_products(text: str, images: List[Dict]) -> List[Dict]:
    """
    Split cleaned page text into product-like segments.

    Strategy: scan lines for action buttons (Add to Cart, etc.) or price patterns.
    Each action line marks the END of a product segment.
    Lines between boundaries become one product block.
    Images are matched to segments by position order (1:1).
    """
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    if not lines:
        return []

    price_re = re.compile(r"\$\d[\d,.]*")

    # Find boundary indices (the last line of each product listing)
    boundaries = []
    for i, line in enumerate(lines):
        line_lower = line.lower()
        is_action = any(p in line_lower for p in ACTION_PATTERNS)
        if is_action:
            boundaries.append(i)

    # If no action buttons found, fall back to price lines as boundaries
    if not boundaries:
        for i, line in enumerate(lines):
            if price_re.search(line) and i > 0:
                boundaries.append(i)

    if not boundaries:
        return []

    # Merge boundaries that are too close (e.g. "Save $X" right after "Add to Cart")
    merged = [boundaries[0]]
    for b in boundaries[1:]:
        if b - merged[-1] >= 2:
            merged.append(b)
    boundaries = merged

    # Junk line patterns to strip from segment starts
    junk_line_re = re.compile(
        r"^(alphabetical|price:\s|default|relevance|sort\s|newest|"
        r"best\s?sell|most\s?popular|featured|low.high|high.low|"
        r"a.z|z.a|\d+\s+products?|showing\s|results?\s)",
        re.IGNORECASE
    )

    # Extract product segments using lookback from each boundary.
    # Each boundary marks the END of a product. We look back up to
    # LOOKBACK lines to capture brand + name + price + action,
    # without pulling in header junk that precedes the first product.
    LOOKBACK = 7
    raw_segments = []
    prev_end = 0

    for boundary in boundaries:
        # Look back at most LOOKBACK lines, but not before previous boundary
        start = max(prev_end, boundary - LOOKBACK + 1)
        segment_lines = lines[start:boundary + 1]
        prev_end = boundary + 1

        if not segment_lines:
            continue

        # Strip leading junk lines (sort options, counts, etc.)
        while segment_lines and junk_line_re.match(segment_lines[0]):
            segment_lines.pop(0)

        if not segment_lines:
            continue

        segment_text = "\n".join(segment_lines)

        # Skip tiny segments
        if len(segment_text) < 15:
            continue

        # Must contain a price or price-alternative to be a real product
        seg_lower = segment_text.lower()
        has_price = bool(price_re.search(segment_text))
        has_price_alt = any(p in seg_lower for p in PRICE_ALT_PATTERNS)
        if not has_price and not has_price_alt:
            continue

        # Skip segments that are mostly navigation/filter content
        skip_count = sum(1 for p in SKIP_PATTERNS if p in seg_lower)
        if skip_count >= 2:
            continue

        # Skip pure price-range filter lines (e.g. "$0-$15 (32)")
        if re.match(r"^[\$\d\s,.\-()]+$", segment_text.strip()):
            continue

        raw_segments.append(segment_text[:MAX_TEXT_PER_BLOCK])

    products = []
    for i, seg_text in enumerate(raw_segments):
        block_images = _find_image_for_segment(seg_text, images, fallback_idx=i)
        products.append({
            "text":         seg_text,
            "html_snippet": "",
            "images":       block_images[:MAX_IMAGES_PER_BLOCK],
            "links":        [],
        })
        if len(products) >= MAX_PRODUCTS:
            break

    return products


def _find_image_for_segment(
    seg_text: str,
    images: List[Dict],
    fallback_idx: int,
) -> List[Dict]:
    """
    Find the best-matching image for a product text segment.

    Strategy:
      1. Content match — look for an image whose alt text or src URL shares a
         meaningful word (>4 chars) with the segment.  This handles carousels
         and lazy-load wrappers that break the positional assumption.
      2. Positional fallback — return images[fallback_idx] when no content
         match is found (preserves the previous behaviour for simple pages).
    """
    if not images:
        return []

    seg_words = {w for w in seg_text.lower().split() if len(w) > 4}
    for img in images:
        alt = img.get("alt", "").lower()
        src = img.get("src", "").lower()
        if any(w in alt or w in src for w in seg_words):
            return [img]

    if fallback_idx < len(images):
        return [images[fallback_idx]]
    return []


def _remove_dom_junk(page: Page) -> None:
    """Remove non-product DOM elements to clean the page."""
    page.evaluate("""() => {
        const junk = [
            'nav', 'header', 'footer', 'script', 'style', 'noscript',
            'iframe', 'svg', 'link', 'meta',
            '[role="navigation"]', '[role="banner"]', '[role="contentinfo"]',
            '.cookie-banner', '.newsletter-popup', '.mini-cart',
            '.announcement-bar', '.breadcrumb',
            '[class*="cookie"]', '[class*="popup"]', '[class*="modal"]',
            '[class*="newsletter"]', '[class*="header"]', '[class*="footer"]',
            '[class*="navigation"]', '[class*="menu"]', '[class*="sidebar"]',
            '[class*="advertisement"]', '[class*="social-share"]',
            '[id*="cookie"]', '[id*="popup"]', '[id*="header"]', '[id*="footer"]'
        ];
        junk.forEach(sel => {
            try { document.querySelectorAll(sel).forEach(el => el.remove()); }
            catch(e) {}
        });
    }""")


def _extract_page_text(page: Page) -> str:
    """Extract cleaned body text with short lines only."""
    return page.evaluate("""() => {
        return document.body.innerText
            .split('\\n')
            .map(line => line.trim())
            .filter(line => line.length > 0 && line.length < 300)
            .join('\\n');
    }""")


def _extract_page_images(page: Page) -> List[Dict]:
    """Extract all <img> src/alt from the page."""
    return page.evaluate("""() => {
        return [...document.querySelectorAll('img[src]')].map(img => ({
            src: img.src || img.dataset.src || img.dataset.lazySrc || '',
            alt: img.alt || ''
        })).filter(img => img.src && img.src.startsWith('http'));
    }""")


def clean_and_extract(page: Page) -> Dict:
    """
    Extract product blocks from a Playwright page.

    Returns:
        {
            "method": "targeted" | "segmented" | "fullpage_cleaned",
            "products": [{"text", "html_snippet", "images", "links"}, ...],
            "product_count": int,
            "error": str | None
        }

    Products list is always populated (even fullpage wraps text in one block).
    All blocks are payload-capped for production use.
    """

    # Scroll to the bottom in steps so lazy-loaded products render before extraction.
    try:
        page.evaluate("""async () => {
            const step = Math.floor(window.innerHeight * 0.8);
            let pos = 0;
            while (pos < document.body.scrollHeight) {
                window.scrollTo(0, pos);
                await new Promise(r => setTimeout(r, 300));
                pos += step;
            }
            window.scrollTo(0, 0);
        }""")
        page.wait_for_timeout(1000)
    except Exception:
        pass

    # ══════════════════════════════════════════════════════
    # Pass 1: Targeted CSS selector extraction
    # ══════════════════════════════════════════════════════
    for selector in PRODUCT_SELECTORS:
        try:
            elements = page.query_selector_all(selector)
            if len(elements) < 2:
                continue

            products = []
            seen_texts = set()

            for el in elements:
                try:
                    data = el.evaluate("""(el) => {
                        const imgs = [...el.querySelectorAll('img')].map(img => ({
                            src: img.src || img.dataset.src || img.dataset.lazySrc || '',
                            alt: img.alt || ''
                        })).filter(img => img.src && img.src.startsWith('http'));

                        // Include el itself if it's an <a> — product cards are often fully wrapped in <a>
                        const linkEls = (el.tagName === 'A' && el.href)
                            ? [el, ...el.querySelectorAll('a[href]')]
                            : [...el.querySelectorAll('a[href]')];
                        const links = linkEls.map(a => ({
                            href: a.href,
                            text: a.textContent.trim().substring(0, 100)
                        })).filter(l => l.href.startsWith('http'));

                        return {
                            text: el.innerText.trim(),
                            html: el.innerHTML.substring(0, 500),
                            images: imgs.slice(0, 3),
                            links: links.slice(0, 5)
                        };
                    }""")

                    text = data["text"]
                    if not text or len(text) < 15:
                        continue

                    # Deduplicate by first 80 chars
                    text_key = text[:80].lower()
                    if text_key in seen_texts:
                        continue
                    seen_texts.add(text_key)

                    products.append({
                        "text": text[:MAX_TEXT_PER_BLOCK],
                        "html_snippet": data["html"][:MAX_HTML_PER_BLOCK],
                        "images": [
                            img for img in data["images"]
                            if _is_product_image(img.get("src", ""))
                        ][:MAX_IMAGES_PER_BLOCK],
                        "links": data["links"][:MAX_LINKS_PER_BLOCK],
                    })

                except Exception:
                    continue

            if len(products) >= 2:
                return {
                    "method": "targeted",
                    "products": products[:MAX_PRODUCTS],
                    "product_count": min(len(products), MAX_PRODUCTS),
                    "error": None,
                }
        except Exception:
            continue

    # ══════════════════════════════════════════════════════
    # Pass 2: Smart text segmentation
    # ══════════════════════════════════════════════════════
    try:
        _remove_dom_junk(page)
        body_text = _extract_page_text(page)
        raw_images = _extract_page_images(page)

        # Filter and dedupe images
        clean_images = _dedupe_images([
            img for img in raw_images
            if _is_product_image(img.get("src", ""))
        ])

        # Try segmentation
        segments = _segment_text_into_products(body_text, clean_images)

        if len(segments) >= 2:
            return {
                "method": "segmented",
                "products": segments,
                "product_count": len(segments),
                "error": None,
            }

        # ══════════════════════════════════════════════════
        # Pass 3: Fullpage fallback (single block)
        # ══════════════════════════════════════════════════
        fallback_block = {
            "text": body_text[:MAX_FULLPAGE_TEXT],
            "html_snippet": "",
            "images": clean_images[:MAX_FULLPAGE_IMAGES],
            "links": [],
        }

        return {
            "method": "fullpage_cleaned",
            "products": [fallback_block] if body_text.strip() else [],
            "product_count": 1 if body_text.strip() else 0,
            "error": None,
        }

    except Exception as e:
        return {
            "method": "fullpage_cleaned",
            "products": [],
            "product_count": 0,
            "error": str(e)[:200],
        }
