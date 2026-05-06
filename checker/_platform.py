"""
Platform detection and bot-blocking detection for Playwright pages.

detect_platform(page, url) → "shopify" | "woocommerce" | "netsuite" | "normal"
detect_blocked(page)       → (is_blocked: bool, reasons: list[str])

Knowing the platform lets the search strategies pick the right URL pattern
(e.g. /catalog/productsearch for NetSuite, /search?q= for Shopify) instead
of blindly trying every pattern on every site.
"""
from __future__ import annotations

import logging
from typing import List, Tuple

log = logging.getLogger(__name__)


def detect_platform(page, url: str) -> str:
    """
    Identify the e-commerce platform powering this page.

    Order matters — NetSuite check must come before WooCommerce because some
    NetSuite stores also include WordPress/WP-JSON in their HTML.

    Returns one of: "shopify" | "woocommerce" | "netsuite" | "normal".
    Never raises.
    """
    try:
        html = page.content().lower()

        # Shopify — global Shopify JS object is the most reliable signal
        shopify_signals = [
            'cdn.shopify.com', 'shopify.com/s/files', 'myshopify.com',
            '-shopify-', '.shopify',
        ]
        if any(s in html for s in shopify_signals):
            return 'shopify'

        try:
            if page.evaluate("""() =>
                typeof Shopify !== 'undefined' ||
                !!(window.Shopify && window.Shopify.theme) ||
                document.querySelector('[data-shopify]') !== null
            """):
                return 'shopify'
        except Exception:
            pass

        # NetSuite / SuiteCommerce — check before WooCommerce
        netsuite_signals = [
            'netsuite', 'suitecommerce', 'suite-commerce',
            '/catalog/productsearch', 'nlapi', 'sc.analytics',
        ]
        if any(s in html for s in netsuite_signals) or '/catalog/' in url:
            return 'netsuite'

        # WooCommerce / WordPress
        woo_signals = [
            'woocommerce', 'wp-content', 'wp-includes', 'wp-json',
            'wordpress', 'wc-product', 'woocommerce-product-search',
            '/wp-content/themes/',
        ]
        if any(s in html for s in woo_signals):
            return 'woocommerce'

        if 'myshopify.com' in url:
            return 'shopify'

        return 'normal'

    except Exception as exc:
        log.warning("detect_platform failed, defaulting to 'normal': %s", exc)
        return 'normal'


def detect_blocked(page) -> Tuple[bool, List[str]]:
    """
    Detect whether the site is blocking automated access (CAPTCHA, bot detection, etc.).

    Uses strong patterns to minimise false positives — "powered by Cloudflare" and
    "unblock" pages should NOT trigger this.

    Returns (is_blocked: bool, reasons: list[str]).
    Never raises.
    """
    reasons: List[str] = []
    try:
        text = page.inner_text('body').lower()
        url  = page.url.lower()

        # Patterns that only appear on true block pages, not Cloudflare marketing pages
        block_patterns = [
            ("checking your browser", "Cloudflare browser check"),
            ("ddos-guard",            "DDoS-Guard protection"),
            ("verify you are human",  "Human verification challenge"),
            ("access denied",         "Access denied page"),
            ("please enable cookies", "Cookie-gate (bot detection)"),
            ("ray id",                "Cloudflare Ray ID (block page)"),
            ("captcha",               "CAPTCHA challenge"),
        ]
        for signal, label in block_patterns:
            if signal in text:
                reasons.append(label)

        url_block = [
            ("captcha",   "CAPTCHA in URL"),
            ("challenge", "Challenge in URL"),
            ("blocked",   "Blocked in URL"),
        ]
        for signal, label in url_block:
            if signal in url:
                reasons.append(label)

        # Only consider truly blocked if we also have minimal real content
        if reasons:
            real_content = len(text) > 500 and any(
                term in text for term in ['add to cart', 'shop', 'product', 'price', '$']
            )
            if real_content:
                # Probably just a Cloudflare CDN page with useful content — not blocked
                reasons.clear()

    except Exception as exc:
        log.debug("detect_blocked error: %s", exc)

    return (len(reasons) > 0, reasons)
