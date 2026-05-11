"""
Shared browser navigation utilities used by both the `checker` and
`url_validator` packages.

Public API
----------
goto_safe(page, url, timeout=15_000) -> None

    WAF-aware page.goto wrapper.  Transparently handles:
      • Imperva / Incapsula  — waits for the full `load` event + settle time so
        the JS fingerprint cookie is written before callers inspect the DOM.
      • Cloudflare            — calls _solve_cloudflare_if_present after navigation.
      • All other sites       — behaves identically to a plain page.goto with
        wait_until='domcontentloaded'; no extra overhead.

Why a separate module?
    Both `checker._platform` and `url_validator` need WAF-safe navigation.
    Putting it in `checker._platform` would create a circular import if
    `url_validator` imported from `checker`.  This module sits at the project
    root and has no cross-package imports at module level (Cloudflare solver is
    lazy-imported inside the function body).
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

log = logging.getLogger(__name__)


def pw_proxy() -> Optional[Dict[str, str]]:
    """
    Return the next proxy in rotation formatted for Playwright new_context().
    Returns None when PROXY_LIST is unset.
    """
    try:
        from checker._proxy_rotator import get_global_rotator
        rot = get_global_rotator()
        return rot.get_for_playwright() if rot else None
    except Exception:
        return None


def goto_safe(page, url: str, timeout: int = 15_000) -> None:
    """
    Navigate to url and wait for the page to settle, handling WAF challenges.

    Strategy:
      1. Navigate with wait_until='domcontentloaded' (fast for most sites).
         On timeout, fall back to waiting for the 'load' state (Cloudflare /
         Imperva can delay domcontentloaded) and add a 3 s settle so JS-heavy
         sites (Magento, SPA) finish rendering the body before callers read it.
      2. If Imperva/Incapsula is detected in the early HTML (the challenge
         embeds an _Incapsula_Resource script tag), wait for the full 'load'
         event and an extra 3 s settle time so the JS cookie is written before
         any DOM inspection.
      3. Call _solve_cloudflare_if_present for any Cloudflare challenge that
         may have appeared.

    Never raises — on timeout falls back to a best-effort wait.
    """
    # Step 1 — primary navigation
    try:
        page.goto(url, timeout=timeout, wait_until='domcontentloaded')
    except Exception as exc:
        log.debug("goto_safe domcontentloaded timeout for %s: %s", url, exc)
        try:
            page.wait_for_load_state('load', timeout=20_000)
        except Exception:
            pass
        # After a domcontentloaded timeout the page is mid-render (JS-heavy
        # sites like Magento populate the body asynchronously). Wait for the
        # rendering to settle so callers read real content.
        page.wait_for_timeout(3_000)

    # Step 2 — Imperva/Incapsula detection and settle
    try:
        early_html = page.content()
        if '_Incapsula_Resource' in early_html or 'incapsula' in early_html.lower():
            log.info("Imperva/Incapsula detected on %s — waiting for full load", url)
            try:
                page.wait_for_load_state('load', timeout=20_000)
            except Exception:
                pass
            page.wait_for_timeout(3_000)   # settle time for cookie to be written
            log.info(
                "Imperva settle complete for %s — body length: %d",
                url, len(page.inner_text('body')),
            )
    except Exception as exc:
        log.debug("goto_safe Imperva check error: %s", exc)

    # Step 3 — Cloudflare solver (lazy import to avoid circular dependency)
    try:
        from checker._platform import _solve_cloudflare_if_present
        _solve_cloudflare_if_present(page)
    except Exception as exc:
        log.debug("goto_safe CF solver error: %s", exc)
