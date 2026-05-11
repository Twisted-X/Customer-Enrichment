"""
Platform detection and bot-blocking detection for Playwright pages.

detect_platform(page, url) → "shopify" | "woocommerce" | "netsuite" | "normal"
detect_blocked(page)       → (is_blocked: bool, reasons: list[str])
_solve_cloudflare_if_present(page) → bool
_goto_safe(page, url, timeout)     → None

Knowing the platform lets the search strategies pick the right URL pattern
(e.g. /catalog/productsearch for NetSuite, /search?q= for Shopify) instead
of blindly trying every pattern on every site.

_goto_safe wraps page.goto to transparently handle two WAFs:
  • Cloudflare — uses domcontentloaded, then calls _solve_cloudflare_if_present
  • Imperva/Incapsula — detects _Incapsula_Resource in early HTML and
    waits for the full `load` event + extra settle time before returning

Cloudflare solver strategy (tried in order):
  1. Non-interactive JS challenge — just wait for patchright to auto-pass it.
  2. Managed / interactive Turnstile — locate the challenge iframe, calculate
     its bounding box, and fire a realistic mouse click (ported from Scrapling).
  3. Embedded Turnstile — same click strategy with a different CSS selector.
  4. 2Captcha fallback — if TWO_CAPTCHA_API_KEY is set and the iframe click
     doesn't resolve the challenge within 15 s, submit to the 2Captcha API
     and inject the returned token.
"""
from __future__ import annotations

import logging
import os
import time
from random import randint
from re import compile as _re_compile
from typing import List, Optional, Tuple

log = logging.getLogger(__name__)

# Regex matching the Cloudflare challenge-platform iframe src
_CF_IFRAME_PATTERN = _re_compile(
    r"^https?://challenges\.cloudflare\.com/cdn-cgi/challenge-platform/.*"
)


def _goto_safe(page, url: str, timeout: int = 15_000) -> None:
    """
    WAF-aware navigation helper.  Delegates to browser_utils.goto_safe which
    handles Imperva/Incapsula settle time and Cloudflare challenge solving.
    Kept here for backward compatibility with internal checker imports.
    """
    from browser_utils import goto_safe
    goto_safe(page, url, timeout=timeout)


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


def _detect_cloudflare(page_content: str) -> Optional[str]:
    """
    Identify the Cloudflare challenge type from raw page HTML.

    Scrapling inspects the `cType` JS variable injected by Cloudflare's
    challenge script.  Returns one of:
      "non-interactive" — JS spinner that auto-resolves after ~1 s
      "managed"         — Turnstile with a checkbox (needs a mouse click)
      "interactive"     — Turnstile with a spinner before the checkbox
      "embedded"        — Turnstile widget embedded inside the page itself
      None              — no Cloudflare challenge detected
    """
    for ctype in ("non-interactive", "managed", "interactive"):
        if f"cType: '{ctype}'" in page_content:
            return ctype
    # Embedded Turnstile: the widget script is included in the page HTML
    if "challenges.cloudflare.com/turnstile/v" in page_content:
        return "embedded"
    return None


def _solve_cloudflare_if_present(page) -> bool:
    """
    Detect and solve a Cloudflare Turnstile / interstitial challenge.

    Strategy (tried in order):
      1. Non-interactive — patchright's navigator patches auto-pass it; just wait.
      2. Managed / interactive / embedded — locate the challenge iframe, get its
         bounding box, fire a realistic human-like mouse click (random offset
         within the checkbox area).  Ported from Scrapling's _cloudflare_solver.
      3. 2Captcha fallback — if TWO_CAPTCHA_API_KEY env var is set and the iframe
         click doesn't resolve the challenge within 15 s, submit the Turnstile
         sitekey to 2Captcha and inject the returned token.

    Returns True  if no challenge was present OR the challenge was resolved.
    Returns False if a challenge was present but could not be resolved.
    Never raises.
    """
    try:
        html = page.content()
        challenge_type = _detect_cloudflare(html)
        if not challenge_type:
            return True   # no challenge — page is clean

        log.info("Cloudflare '%s' challenge on %s", challenge_type, page.url)

        # ── Strategy 1: non-interactive JS spinner ───────────────────────────
        if challenge_type == "non-interactive":
            for _ in range(30):               # wait up to 30 s in 1-s steps
                page.wait_for_timeout(1_000)
                page.wait_for_load_state("domcontentloaded")
                if "<title>Just a moment...</title>" not in page.content():
                    log.info("Non-interactive CF challenge resolved")
                    return True
            log.warning("Non-interactive CF challenge timed out for %s", page.url)
            return False

        # ── Strategy 2: managed / interactive / embedded — iframe click ──────
        # For interactive challenges, wait for the "Verifying you are human"
        # spinner to disappear before trying to click the checkbox.
        if challenge_type != "embedded":
            for _ in range(20):
                if "Verifying you are human." not in page.content():
                    break
                page.wait_for_timeout(500)

        # Locate the challenge iframe and get its bounding box
        outer_box = None
        try:
            iframe = page.frame(url=_CF_IFRAME_PATTERN)
            if iframe is not None:
                # Wait for iframe element to become visible
                frame_el = iframe.frame_element()
                for _ in range(10):
                    if frame_el.is_visible():
                        break
                    page.wait_for_timeout(500)
                outer_box = frame_el.bounding_box()
        except Exception as exc:
            log.debug("CF iframe locate failed: %s", exc)

        # CSS-selector fallback when the iframe approach doesn't work
        if not outer_box:
            box_selector = (
                "#cf_turnstile div, #cf-turnstile div, .turnstile>div>div"
                if challenge_type == "embedded"
                else ".main-content p+div>div>div"
            )
            try:
                outer_box = page.locator(box_selector).last.bounding_box()
            except Exception as exc:
                log.debug("CF box selector fallback failed: %s", exc)

        if outer_box:
            # Random offset mimics natural human imprecision (from Scrapling)
            captcha_x = outer_box["x"] + randint(26, 28)
            captcha_y = outer_box["y"] + randint(25, 27)
            page.mouse.click(captcha_x, captcha_y, delay=randint(100, 200), button="left")
            log.info("Clicked CF Turnstile at (%.0f, %.0f)", captcha_x, captcha_y)

            # Wait up to 15 s for the challenge to clear after the click
            for _ in range(150):
                page.wait_for_timeout(100)
                if "<title>Just a moment...</title>" not in page.content():
                    log.info("CF Turnstile resolved after iframe click")
                    return True

        log.info("Iframe click did not resolve CF challenge — trying 2Captcha fallback")

        # ── Strategy 3: 2Captcha API fallback ────────────────────────────────
        if _solve_turnstile_2captcha(page):
            return True

        log.warning("All CF solvers exhausted for %s", page.url)
        return False

    except Exception as exc:
        log.debug("_solve_cloudflare_if_present error: %s", exc)
        return False


def _solve_turnstile_2captcha(page) -> bool:
    """
    Solve a Cloudflare Turnstile challenge via the 2Captcha API.

    Requires the TWO_CAPTCHA_API_KEY environment variable to be set.
    Cost: ~$3 per 1,000 solves (typically 5-20 s per solve).

    Flow:
      1. Extract the Turnstile sitekey from the page DOM.
      2. POST the sitekey + page URL to 2Captcha's /in.php endpoint.
      3. Poll /res.php until a token is returned (up to 90 s).
      4. Inject the token into the hidden cf-turnstile-response input
         and click the submit button (or dispatch a custom event if no
         visible form exists).

    Returns True if the challenge was solved and the page reloaded cleanly.
    Returns False if the API key is missing, the sitekey can't be found,
    or the solve request times out.
    Never raises.
    """
    api_key = os.getenv("TWO_CAPTCHA_API_KEY", "").strip()
    if not api_key:
        log.debug("TWO_CAPTCHA_API_KEY not set — skipping 2Captcha fallback")
        return False

    try:
        from curl_cffi import requests as _cffi   # already in requirements

        # Step 1 — extract sitekey
        sitekey = page.evaluate("""() => {
            const candidates = [
                document.querySelector('[data-sitekey]'),
                document.querySelector('.cf-turnstile'),
                document.querySelector('[id*="turnstile"]'),
                document.querySelector('[class*="turnstile"]'),
            ];
            for (const el of candidates) {
                if (el) {
                    const k = el.getAttribute('data-sitekey') || el.getAttribute('sitekey');
                    if (k) return k;
                }
            }
            return null;
        }""")

        if not sitekey:
            log.warning("2Captcha: could not extract Turnstile sitekey from %s", page.url)
            return False

        log.info("2Captcha: submitting Turnstile sitekey %s for %s", sitekey[:12] + "...", page.url)

        # Step 2 — submit task to 2Captcha
        submit = _cffi.post(
            "https://2captcha.com/in.php",
            data={
                "key":      api_key,
                "method":   "turnstile",
                "sitekey":  sitekey,
                "pageurl":  page.url,
                "json":     "1",
            },
            timeout=15,
        )
        submit_data = submit.json()
        if submit_data.get("status") != 1:
            log.warning("2Captcha submission failed: %s", submit_data)
            return False

        captcha_id = submit_data["request"]
        log.info("2Captcha task ID: %s — polling for token...", captcha_id)

        # Step 3 — poll for result (up to 90 s, 3-s intervals)
        token: Optional[str] = None
        for attempt in range(30):
            time.sleep(3)
            result = _cffi.get(
                "https://2captcha.com/res.php",
                params={
                    "key":    api_key,
                    "action": "get",
                    "id":     captcha_id,
                    "json":   "1",
                },
                timeout=10,
            )
            result_data = result.json()
            if result_data.get("status") == 1:
                token = result_data["request"]
                log.info("2Captcha token received after %d polls", attempt + 1)
                break
            if result_data.get("request") != "CAPCHA_NOT_READY":
                log.warning("2Captcha error response: %s", result_data)
                return False

        if not token:
            log.warning("2Captcha timed out for %s", page.url)
            return False

        # Step 4 — inject token and submit
        injected = page.evaluate("""(token) => {
            // Set all known Turnstile response field names
            const names = [
                'cf-turnstile-response',
                'g-recaptcha-response',
                'h-captcha-response',
            ];
            let found = false;
            for (const name of names) {
                const inputs = document.querySelectorAll(`[name="${name}"]`);
                inputs.forEach(inp => { inp.value = token; found = true; });
            }
            // Also try the Turnstile JS callback if available
            if (window.turnstile && window.turnstile.response) {
                window.turnstile.response = () => token;
            }
            // Try submitting the nearest form
            const form = document.querySelector('form#challenge-form, form[action*="cdn-cgi"]');
            if (form) { form.submit(); return 'form_submitted'; }
            return found ? 'token_injected' : 'no_target';
        }""", token)

        log.info("2Captcha inject result: %s — waiting for page reload", injected)
        page.wait_for_load_state("domcontentloaded", timeout=15_000)
        page.wait_for_timeout(2_000)

        if "<title>Just a moment...</title>" not in page.content():
            log.info("2Captcha Turnstile solve confirmed for %s", page.url)
            return True

        log.warning("2Captcha token injected but challenge still present for %s", page.url)
        return False

    except Exception as exc:
        log.warning("_solve_turnstile_2captcha error: %s", exc)
        return False


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
