"""
URL health checking, classification, and root-domain extraction.

Public API:
    is_url_blank_or_invalid(val) -> bool
    check_url(session, url)      -> dict   (async)
    bulk_check_urls(urls)        -> list   (async)
    classify_url(url)            -> str    ("website" | "social" | "marketplace" | "maps" | "not_found")
    extract_root_domain(url)     -> str
"""
from __future__ import annotations

import asyncio
import re
from urllib.parse import urlparse

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

from ._config import (
    CONCURRENT_CHECKS, REQUEST_TIMEOUT,
    URL_BLACKLIST, WEBSITE_NOT_FOUND_LABEL,
)

_SOCIAL_DOMAINS = frozenset({
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "snapchat.com",
})
_MARKETPLACE_DOMAINS = frozenset({
    "myshopify.com", "square.site", "squareup.com", "etsy.com", "ebay.com",
    "amazon.com", "linktr.ee", "bio.link", "beacons.ai",
})


def is_url_blank_or_invalid(val) -> bool:
    """True if value is empty, blacklisted, or does not look like a URL."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return True
    s = str(val).strip().strip('​ ﻿\r\n\t')
    if not s or s.lower() in URL_BLACKLIST:
        return True
    if s.startswith(("http://", "https://")):
        return False
    if re.match(r"^[a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,}(/.*)?$", s):
        return False
    return True


async def check_url(session: aiohttp.ClientSession, url: str) -> dict:
    """
    Ping a URL and return {status, final_url, http_code}.

    Status values:
      active      — 200 and URL unchanged
      redirected  — 200 or 3xx with a different final URL
      blocked     — 401/403/405/429/503 (server alive but rejecting bot)
      dead        — any other error or non-200 response
      missing     — URL was blank/null

    Retries once after 1 s on pure connection failure (no http_code) to reduce
    false 'dead' results from transient network errors. HTTP-level errors are
    NOT retried — they carry a real status code.
    """
    if not url or pd.isna(url) or str(url).strip() == "":
        return {"status": "missing", "final_url": None, "http_code": None}

    raw = str(url).strip()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw

    async def _attempt():
        try:
            async with session.get(
                raw,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                allow_redirects=True,
                # ssl=False skips certificate verification intentionally.
                # Many small retailers use self-signed or expired certs; a TLS
                # error here would mark a live site as "dead". We are only
                # checking reachability (HTTP status + final URL), not
                # transmitting any sensitive data, so the risk is acceptable.
                ssl=False,
            ) as resp:
                final = str(resp.url)
                code  = resp.status
                if code == 200:
                    status = "active" if final.rstrip("/") == raw.rstrip("/") else "redirected"
                elif 300 <= code < 400:
                    status = "redirected"
                elif code in (401, 403, 405, 429, 503):
                    status = "blocked"
                else:
                    status = "dead"
                return {"status": status, "final_url": final, "http_code": code}
        except Exception:
            return {"status": "dead", "final_url": None, "http_code": None}

    result = await _attempt()
    if result["http_code"] is None and result["status"] == "dead":
        await asyncio.sleep(1)
        result = await _attempt()
    return result


async def bulk_check_urls(urls: list) -> list:
    """Check a list of URLs concurrently, preserving input order."""
    semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)

    async def bounded(session, url):
        async with semaphore:
            return await check_url(session, url)

    # ssl=False: same rationale as check_url above — reachability pings only.
    connector = aiohttp.TCPConnector(limit=CONCURRENT_CHECKS, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bounded(session, u) for u in urls]
        return await tqdm_asyncio.gather(*tasks, desc="  Pinging URLs")


def _url_netloc(url: str) -> str:
    """Return lowercased netloc from url, stripping leading 'www.'."""
    try:
        s = url if url.startswith(("http://", "https://")) else "https://" + url
        host = urlparse(s).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def classify_url(url) -> str:
    """
    Classify a URL as: website / social / marketplace / maps / not_found.

    Used to flag Facebook pages, Shopify storefronts, etc. that look like
    websites but are not primary domains — important for NetSuite write-back.
    """
    if not url:
        return "not_found"
    s = str(url).strip()
    if not s or s == WEBSITE_NOT_FOUND_LABEL:
        return "not_found"
    s_low = s.lower()
    if "google.com/maps" in s_low or "maps.google" in s_low or "goo.gl/maps" in s_low:
        return "maps"
    host = _url_netloc(s)
    if not host:
        return "not_found"
    if any(host == d or host.endswith("." + d) for d in _SOCIAL_DOMAINS):
        return "social"
    if any(host == d or host.endswith("." + d) for d in _MARKETPLACE_DOMAINS):
        return "marketplace"
    return "website"


def extract_root_domain(url) -> str:
    """
    For regular websites: strip to scheme + netloc (e.g. https://www.academy.com).
    For social/marketplace URLs: keep the full URL — the path IS the business
    identity (e.g. https://www.facebook.com/annswm/ must not become
    https://www.facebook.com).
    Returns '' for maps, not_found, or blank.
    """
    if not url:
        return ""
    s = str(url).strip()
    if not s or s == WEBSITE_NOT_FOUND_LABEL:
        return ""
    url_type = classify_url(s)
    if url_type in ("social", "marketplace"):
        return s if s.startswith(("http://", "https://")) else "https://" + s
    if url_type in ("maps", "not_found"):
        return ""
    try:
        full = s if s.startswith(("http://", "https://")) else "https://" + s
        p = urlparse(full)
        return f"{p.scheme}://{p.netloc}" if p.netloc else ""
    except Exception:
        return ""
