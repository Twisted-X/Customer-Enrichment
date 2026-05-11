"""
SKU fingerprint and brand-context scanners.

These are the two core detection primitives used by every layer of the check pipeline:

  scan_page_for_skus(page)      — Playwright Page → matched style codes + sample products
  scan_html_for_skus(html)      — raw HTML string → matched style codes (no browser needed)
  find_brand_in_product_context — Playwright Page → brand name found in product links/cards

Both scanners are pure reads — they never navigate, click, or mutate the page.
"""
from __future__ import annotations

import re
import logging
from typing import List, Optional, Tuple

from brand_config import PRIMARY_BRAND_PAIR
from ._types import ScanResult, SampleProduct, empty_scan

log = logging.getLogger(__name__)

# Token shape: 4–15 alphanumeric chars, e.g. WDM0093, BACKPKECO001
_TOKEN_RE = re.compile(r'[A-Za-z0-9]{4,15}')

# JavaScript brand terms injected into in-browser DOM scans
_BRAND_TERMS_JS = __import__("json").dumps(list(PRIMARY_BRAND_PAIR) + ["twisted-x"])

# JavaScript for brand-in-product-context detection (looks at product link/card text)
_BRAND_CONTEXT_JS = """() => {
    const brandTerms = __BRAND_TERMS__;
    const isBrand = (t) => brandTerms.some(term => t.includes(term));

    // Selectors that typically wrap individual product cards
    const productSelectors = [
        '.product-card a', '.product-item a', '.product-tile a',
        '[class*="product"] a[href*="/product"]',
        '[class*="product"] a[href*="/p/"]',
        'a[href*="/product/"]', 'a[href*="/products/"]',
        '.search-results a', '.catalog-product a',
    ];

    const results = [];
    const seen = new Set();

    for (const sel of productSelectors) {
        const links = document.querySelectorAll(sel);
        for (const link of links) {
            const text = link.textContent.trim().toLowerCase();
            if (!isBrand(text)) continue;
            if (text.length < 5 || text.length > 300) continue;
            const key = text.slice(0, 40);
            if (seen.has(key)) continue;
            seen.add(key);
            results.push({ name: link.textContent.trim(), product_url: link.href || '' });
            if (results.length >= 5) break;
        }
        if (results.length >= 5) break;
    }
    return results;
}""".replace("__BRAND_TERMS__", _BRAND_TERMS_JS)


def _extract_tokens(text: str) -> set:
    """Return the set of uppercase alphanumeric tokens (4–15 chars) from `text`."""
    return {t.upper() for t in _TOKEN_RE.findall(text or "")}


def _clean_html(html: str) -> str:
    """Strip <script> and <style> blocks to prevent minified-JS false SKU hits."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<style[^>]*>.*?</style>',  ' ', html, flags=re.IGNORECASE | re.DOTALL)
    return html


def scan_page_for_skus(page) -> ScanResult:
    """
    Scan a live Playwright page for Twisted X style codes.

    Strategy:
      1. Extract all tokens from both the rendered text AND raw HTML.
      2. Intersect against the TX_STYLE_CODES set (3,025 known codes).
      3. For each matched code, find the surrounding product card to extract
         name, price, image, and product URL.

    Returns a ScanResult. Never raises — returns empty_scan() on any error.
    """
    from config import TX_STYLE_CODES

    if not TX_STYLE_CODES:
        return empty_scan()

    try:
        page_text = page.inner_text('body')
        page_html = page.content()
    except Exception as exc:
        log.warning("scan_page_for_skus: could not read page content — %s", exc)
        return empty_scan()

    text_tokens = _extract_tokens(page_text)
    html_tokens = _extract_tokens(page_html)
    matched_codes: set = TX_STYLE_CODES & (text_tokens | html_tokens)

    if not matched_codes:
        return empty_scan()

    matched_in = [
        f"{code} in page text" if code in text_tokens else f"{code} in page HTML/URLs"
        for code in sorted(matched_codes)[:10]
    ]

    sample_products = _extract_samples_via_links(page, page_text, matched_codes)
    log.debug("scan_page_for_skus: %d codes matched, %d samples", len(matched_codes), len(sample_products))

    return {
        "matched_codes": matched_codes,
        "matched_in": matched_in[:5],
        "sample_products": sample_products[:5],
    }


def scan_html_for_skus(html: str) -> ScanResult:
    """
    Scan raw HTML (no browser) for Twisted X style codes.

    Used by Layer-1 (HTTP-first) to avoid launching Playwright for sites
    that render products in static HTML.

    Sample extraction tries two sources in order:
      1. href attributes — SKU in product link paths (e.g. /products/wdm0003-boot)
      2. img src attributes — SKU in image filenames (e.g. wdm0003__42223.jpg),
         with a window scan to find the nearest href and text for the product card

    Returns a ScanResult. Never raises.
    """
    from config import TX_STYLE_CODES

    if not TX_STYLE_CODES:
        return empty_scan()

    cleaned = _clean_html(html)
    matched_codes: set = TX_STYLE_CODES & _extract_tokens(cleaned)

    if not matched_codes:
        return empty_scan()

    matched_in = [f"{code} in page HTML" for code in sorted(matched_codes)[:10]]
    code_pattern = re.compile('|'.join(re.escape(c) for c in matched_codes), re.IGNORECASE)

    # Source 1: SKU appears in an href (product link paths)
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', cleaned)
    samples: List[SampleProduct] = [
        {"product_url": h, "name": "", "price": "", "sku": code_pattern.search(h).group().upper() if code_pattern.search(h) else "", "image": ""}
        for h in hrefs if code_pattern.search(h)
    ][:5]

    if len(samples) < 5:
        # Source 2: SKU appears in img src (e.g. image filenames like wdm0003__42223.jpg).
        # Walk a context window around each img tag to recover the nearest href and
        # any visible product name text.
        seen_skus = {s["sku"] for s in samples}
        src_pattern = re.compile(r'src=["\']([^"\']+)["\']', re.IGNORECASE)
        for src_match in src_pattern.finditer(cleaned):
            if len(samples) >= 5:
                break
            src_val = src_match.group(1)
            sku_hit = code_pattern.search(src_val)
            if not sku_hit:
                continue
            sku = sku_hit.group().upper()
            if sku in seen_skus:
                continue
            seen_skus.add(sku)

            # Look at a window of HTML surrounding the <img> tag for context
            win_start = max(0, src_match.start() - 600)
            win_end   = min(len(cleaned), src_match.end() + 300)
            window    = cleaned[win_start:win_end]

            href_hit  = re.search(r'href=["\']([^"\']+)["\']', window)
            product_url = href_hit.group(1) if href_hit else ""

            # Try to extract a product name from surrounding visible text
            # (strip tags, collapse whitespace, take first meaningful chunk)
            text_only = re.sub(r'<[^>]+>', ' ', window)
            text_only = re.sub(r'\s+', ' ', text_only).strip()
            name = ""
            for chunk in text_only.split():
                if len(name) > 60:
                    break
                if chunk and not chunk.startswith(('$', 'http', 'data:')):
                    name = (name + ' ' + chunk).strip()

            samples.append({
                "product_url": product_url,
                "name":        name[:80],
                "price":       "",
                "sku":         sku,
                "image":       src_val,
            })

    return {"matched_codes": matched_codes, "matched_in": matched_in[:5], "sample_products": samples[:5]}


def find_brand_in_product_context(page) -> Tuple[bool, List[SampleProduct]]:
    """
    Check whether "Twisted X" (or a variant) appears inside product link/card text.

    Used as a fallback when SKU fingerprinting finds no codes — some retailers
    use their own internal SKU numbering so Twisted X SKUs never appear in the DOM.

    Returns (found: bool, samples: list[SampleProduct]).
    Never raises.
    """
    try:
        results = page.evaluate(_BRAND_CONTEXT_JS)
        log.debug("find_brand_in_product_context: %d product(s) with brand in context", len(results))
        return (len(results) > 0, results[:5])
    except Exception as exc:
        log.warning("find_brand_in_product_context error: %s", exc)
        return (False, [])

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_samples_via_links(
    page,
    page_text: str,
    matched_codes: set,
    limit: Optional[int] = 5,
) -> List[SampleProduct]:
    """
    Try link-based extraction first (SKU in link text/href), then fall back to
    text-based extraction (SKU in page_text, DOM walk for image+URL).

    Args:
        limit: Maximum products to return.  Pass ``None`` for no cap.
    """
    samples = _link_based_samples(page, matched_codes, limit=limit)
    if not samples:
        samples = _text_based_samples(page, page_text, matched_codes, limit=limit)
    return samples


def _link_based_samples(
    page,
    matched_codes: set,
    limit: Optional[int] = 5,
) -> List[SampleProduct]:
    """
    Walk <a href> elements; return those whose text or href contain a matched SKU.

    Args:
        limit: Maximum products to return.  Pass ``None`` for no cap.
    """
    samples: List[SampleProduct] = []
    seen: set = set()
    code_pattern = re.compile('|'.join(re.escape(c) for c in matched_codes), re.IGNORECASE)

    try:
        links = page.query_selector_all('a[href]')
        for link in links:
            try:
                href = link.evaluate("el => el.href") or ""
                text = link.inner_text().strip()
                if not code_pattern.search(text + " " + href):
                    continue
                if len(text) < 5 or len(text) > 300:
                    continue

                match = code_pattern.search(text + " " + href)
                sku = match.group().upper() if match else ""
                if sku in seen:
                    continue
                seen.add(sku)

                name, price = _parse_name_price(text)
                if not name:
                    name = sku

                image = _find_image_for_link(link)

                samples.append({"name": name, "price": price, "sku": sku,
                                 "image": image, "product_url": href})
                if limit is not None and len(samples) >= limit:
                    break
            except Exception:
                continue
    except Exception as exc:
        log.debug("_link_based_samples failed: %s", exc)

    return samples


def _text_based_samples(
    page,
    page_text: str,
    matched_codes: set,
    limit: Optional[int] = 5,
) -> List[SampleProduct]:
    """
    Fallback: find each matched SKU in page_text, then DOM-walk to recover
    the image URL and product link from the surrounding element.

    Args:
        limit: Maximum products to return.  Pass ``None`` for no cap.
    """
    samples: List[SampleProduct] = []
    lines = [l.strip() for l in page_text.split('\n') if l.strip()]

    for i, line in enumerate(lines):
        for code in list(matched_codes):
            if code.lower() not in line.lower():
                continue

            name, price = "", "N/A"
            window = lines[max(0, i - 2): min(len(lines), i + 4)]
            for wl in window:
                if '$' in wl:
                    price = wl.strip()
                elif len(wl) > 10 and not any(w in wl.lower() for w in ['filter', 'sort', 'search']):
                    if not name:
                        name = wl.strip()
            if not name:
                name = code

            image, product_url = _dom_walk_for_image_and_url(page, code)
            samples.append({"name": name, "price": price, "sku": code,
                             "image": image, "product_url": product_url})
            break

        if limit is not None and len(samples) >= limit:
            break

    return samples


def _parse_name_price(text: str) -> Tuple[str, str]:
    """Extract product name and price from link inner text."""
    name, price = "", "N/A"
    skip = {'filter', 'sort', 'search', 'results for', 'shop all'}
    for line in (l.strip() for l in text.split('\n') if l.strip()):
        ll = line.lower()
        if any(w in ll for w in skip):
            continue
        if '$' in line or 'see price' in ll:
            price = line.strip()
        elif ll not in ('twisted x', 'twistedx') and not name and len(line) > 5:
            name = line
    return name, price


def _find_image_for_link(link) -> str:
    """Walk up from a link element until we find a container that has an <img>."""
    try:
        return link.evaluate("""el => {
            let node = el;
            for (let i = 0; i < 12; i++) {
                if (!node || node === document.body) break;
                const img = node.querySelector('img');
                if (img) return img.src || img.dataset.src || img.dataset.lazySrc || '';
                node = node.parentElement;
            }
            return '';
        }""") or ""
    except Exception:
        return ""


def _dom_walk_for_image_and_url(page, sku: str) -> Tuple[str, str]:
    """
    Find the DOM text node containing `sku` (skipping script/style tags),
    walk up to a container that has an img, and return (image_src, product_href).
    """
    try:
        result = page.evaluate("""(sku) => {
            const SKIP = new Set(["SCRIPT","STYLE","NOSCRIPT","TEMPLATE","META","HEAD"]);
            const walker = document.createTreeWalker(
                document.body, NodeFilter.SHOW_TEXT,
                { acceptNode: n => SKIP.has(n.parentElement?.tagName) ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT }
            );
            let node, firstLink = null;
            while (node = walker.nextNode()) {
                if (!node.textContent.toUpperCase().includes(sku.toUpperCase())) continue;
                let el = node.parentElement;
                for (let i = 0; i < 12; i++) {
                    if (!el || el === document.body) break;
                    if (!firstLink) { const l = el.querySelector('a[href]'); if (l) firstLink = l; }
                    const img = el.querySelector('img');
                    if (img) {
                        const link = el.querySelector('a[href]') || firstLink;
                        return { imgSrc: img.src || img.dataset.src || img.dataset.lazySrc || '',
                                 linkHref: link ? link.href : '' };
                    }
                    el = el.parentElement;
                }
                return firstLink ? { imgSrc: '', linkHref: firstLink.href } : null;
            }
            return null;
        }""", sku)
        if result:
            return result.get("imgSrc", ""), result.get("linkHref", "")
    except Exception as exc:
        log.debug("_dom_walk_for_image_and_url failed for %s: %s", sku, exc)
    return "", ""
