"""
Customer URL Enrichment Pipeline — Twisted X (CSV + Address Verification)
==========================================================================
Uses QueryResults_403.csv (or configurable CSV/Excel). "Broken" = blank/invalid
or dead website url. For broken records:
  1. Pings existing URL (optional); if dead/missing, looks up Google Places.
  2. Groups by company; fetches up to 20 Places candidates per company name.
  3. Hybrid two-layer resolution:
       - Company-level: picks the first candidate matching ANY known branch
         (city/state/zip) for that company. Shared found_url for all rows.
       - Branch-level: for each row, independently picks the first candidate
         matching THAT row's city/state/zip. Phone, address, maps link,
         hours, lat/lon, and ratings come from the branch candidate only.
  4. enrichment_source per row:
       not_found          — Places returned no candidates
       address_mismatch   — candidates exist but no company-level address match
                            (also when all row addresses are blank)
       hybrid_full        — company website resolved + this row's branch matched
       hybrid_website_only — company website resolved but no branch match for row
       url_alive          — URL pinged alive (no Places call needed)
       skipped            — row had a valid URL from the start
  5. address_match is True only when this specific row's branch matched a candidate.
  6. Writes enriched file with found_url, places_* fields, places_latitude,
     places_longitude, address_match.

Requirements:
    pip install pandas openpyxl requests aiohttp tqdm

Usage:
    Set GOOGLE_PLACES_API_KEY (env or below). Run: python url_enrichment_pipeline.py
"""

import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import re
import requests
from typing import Optional
from urllib.parse import urlparse
from contextlib import contextmanager
from datetime import date
import tempfile
import sys
import time
import os
import logging
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

# Load .env file if present (for local dev); on the server set env vars directly
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — env vars must be set in the shell

# ─────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────

# ── Google Places API ─────────────────────────────────────
# Must be set in environment or .env file — no hardcoded fallback.
GOOGLE_PLACES_API_KEY = os.environ["GOOGLE_PLACES_API_KEY"]

# ── SFTP ─────────────────────────────────────────────────
# Set these in your .env file or shell environment.
SFTP_HOST        = os.getenv("SFTP_HOST", "")
SFTP_PORT        = int(os.getenv("SFTP_PORT", 22))
SFTP_USER        = os.getenv("SFTP_USER", "")
# Strip inline # from .env; fix Users/... without leading /; expand ~
_SFTP_KEY_RAW    = (os.getenv("SFTP_KEY_PATH", "") or "").strip()
_SFTP_KEY_PATH   = _SFTP_KEY_RAW.split("#", 1)[0].strip() if _SFTP_KEY_RAW else ""
if _SFTP_KEY_PATH.startswith("Users/") or _SFTP_KEY_PATH.startswith("home/"):
    _SFTP_KEY_PATH = "/" + _SFTP_KEY_PATH
SFTP_KEY_PATH    = os.path.expanduser(_SFTP_KEY_PATH)
SFTP_PASSWORD    = os.getenv("SFTP_PASSWORD", "")      # used only if no key path
SFTP_INBOUND_DIR = os.getenv("SFTP_INBOUND_DIR", "/inbound")
SFTP_REVIEW_DIR  = os.getenv("SFTP_REVIEW_DIR",  "/review")
SFTP_ARCHIVE_DIR = os.getenv("SFTP_ARCHIVE_DIR", "/archive")

# Set to False to run against a local file (INPUT_FILE below) without SFTP.
# Set to True for the automated Celigo flow.
USE_SFTP = os.getenv("USE_SFTP", "false").lower() == "true"

# Used only when USE_SFTP=False (local / manual runs)
INPUT_FILE  = os.getenv("INPUT_FILE",  "QueryResults_837.csv")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "QueryResults_837_Enriched.csv")

# ── NetSuite column names ─────────────────────────────────
COMPANY_COL     = "Company"
URL_COL         = "website url"
ADDRESS_COLS    = ["address", "city", "state", "zip code"]
NETSUITE_ID_COL = "Internal ID"   # numeric ID — required for Celigo write-back

# Column rename map: Celigo / saved-search export label → pipeline-internal name.
# All renames happen at load time so the rest of the pipeline uses consistent names.
COLUMN_MAP = {
    # NetSuite / Celigo export labels → pipeline-internal names
    "internalid":            "Internal ID",
    "company":               "Company",
    "Company Name":          "Company",
    "Web Address":           "website url",
    "Shipping Address 1":    "address",
    "Shipping City":         "city",
    "Shipping State/Province": "state",
    "Shipping Zip":          "zip code",
    "Last Enrichment Date":  "last_enrichment_date",
}

# ── 30-day re-enrichment cadence ─────────────────────────
# Internal name for the date field after COLUMN_MAP rename.
NETSUITE_LAST_ENRICHED_COL = "last_enrichment_date"
ENRICHMENT_TTL_DAYS        = int(os.getenv("ENRICHMENT_TTL_DAYS", 30))

# ── Pipeline behaviour ────────────────────────────────────
PING_EXISTING_URLS          = True
FILL_BLANK_WEBSITE_WHEN_MATCHED = True
ACCEPT_UNVERIFIED_MATCH     = True
CHANNEL_KEYWORDS = {"ecommerce", "e-commerce", "ecom", "online", "web", "website"}

# Product check via api_server POST /api/check
# Set ENABLE_PRODUCT_CHECK=true in env to activate (requires api_server running)
ENABLE_PRODUCT_CHECK = os.getenv("ENABLE_PRODUCT_CHECK", "").lower() in ("1", "true", "yes")
CHECK_API_URL        = os.getenv("CHECK_API_URL", "http://localhost:8000/api/check")
CHECK_API_TIMEOUT    = int(os.getenv("CHECK_API_TIMEOUT", 60))   # 60s per domain (was 200)
CHECK_WORKERS        = int(os.getenv("CHECK_WORKERS", 5))         # parallel domain checks

# Google Places primary_type values that indicate a physical retail store
RETAIL_PRIMARY_TYPES = {
    "bakery", "beauty_salon", "bicycle_store", "book_store",
    "car_dealer", "car_parts_store", "clothing_store", "convenience_store",
    "department_store", "drugstore", "electronics_store", "florist",
    "furniture_store", "gift_shop", "grocery_store", "hardware_store",
    "home_goods_store", "jewelry_store", "liquor_store", "outdoor_sports_store",
    "pet_store", "pharmacy", "shoe_store", "shopping_mall",
    "sporting_goods_store", "supermarket", "toy_store", "wholesale_store",
    "cosmetics_store", "farm_supply_store", "office_supply_store",
    "musical_instrument_store", "paint_store", "baby_goods_store",
    "apparel_store", "variety_store", "western_apparel_store",
}

# Google Places primary_type values that are definitively NOT retail
NONRETAIL_PRIMARY_TYPES = {
    "warehouse", "storage", "self_storage_facility", "moving_company",
    "distribution_center", "fulfillment_center",
}

CONCURRENT_CHECKS = 100
REQUEST_TIMEOUT   = 5

# Values that mean "no URL" (case-insensitive after strip)
URL_BLACKLIST = {"", "n/a", "na", "-", "tbd", "none", "no website", "null"}
# ─────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════
# URL: blank / invalid / ping
# ══════════════════════════════════════════════════════════

def is_url_blank_or_invalid(val) -> bool:
    """True if value is empty, blacklisted, or does not look like a URL."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return True
    s = str(val).strip().strip('\u200b\u00a0\ufeff\r\n\t')
    if not s:
        return True
    if s.lower() in URL_BLACKLIST:
        return True
    # Minimal URL or domain pattern
    if s.startswith(("http://", "https://")):
        return False
    # Bare domain e.g. example.com
    if re.match(r"^[a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,}(/.*)?$", s):
        return False
    return True


async def check_url(session: aiohttp.ClientSession, url: str) -> dict:
    """
    Pings a URL and returns status + resolved URL + http_code.
    Retries once after 3 s on a pure connection failure (http_code=None) to
    reduce false 'dead' results from transient network errors.  HTTP-level
    errors (4xx, 5xx) are NOT retried — they carry a real status code.
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
                ssl=False,
            ) as resp:
                final = str(resp.url)
                code  = resp.status
                if code == 200:
                    status = "active" if final.rstrip("/") == raw.rstrip("/") else "redirected"
                elif 300 <= code < 400:
                    status = "redirected"
                elif code in (401, 403, 405, 429, 503):
                    # Server is alive but rejecting the bot (auth wall, rate limit,
                    # bot-protection). Treat as alive so the original URL is kept
                    # and the row never gets sent to Google Places.
                    status = "blocked"
                else:
                    status = "dead"
                return {"status": status, "final_url": final, "http_code": code}
        except Exception:
            return {"status": "dead", "final_url": None, "http_code": None}

    result = await _attempt()
    # Retry once on pure connection failure (no http_code means network error,
    # not a server rejection — worth one more try after a brief pause)
    if result["http_code"] is None and result["status"] == "dead":
        await asyncio.sleep(1)
        result = await _attempt()
    return result


async def bulk_check_urls(urls: list) -> list:
    """Checks a list of URLs concurrently, preserving input order."""
    semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)

    async def bounded(session, url):
        async with semaphore:
            return await check_url(session, url)

    connector = aiohttp.TCPConnector(limit=CONCURRENT_CHECKS, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bounded(session, u) for u in urls]
        # gather preserves order (as_completed does not — wrong results per row)
        return await tqdm_asyncio.gather(*tasks, desc="  Pinging URLs")


# ══════════════════════════════════════════════════════════
# Address normalization and matching
# ══════════════════════════════════════════════════════════

def normalize_zip(zip_val) -> str:
    """Extract first 5 digits from zip code."""
    if zip_val is None or (isinstance(zip_val, float) and pd.isna(zip_val)):
        return ""
    s = re.sub(r"\D", "", str(zip_val))
    return s[:5] if s else ""


def normalize_address_for_match(city, state, zip_val) -> tuple:
    """Return (city_norm, state_norm, zip5) for comparison."""
    def n(s):
        if s is None or (isinstance(s, float) and pd.isna(s)):
            return ""
        return " ".join(str(s).strip().upper().split())

    return (n(city), n(state), normalize_zip(zip_val))


def parse_places_address(formatted_address: str) -> tuple:
    """
    Parse Google formattedAddress into (city, state, zip) heuristics.
    Returns (city_norm, state_norm, zip5). Formatted address is typically
    "street, city, state zip" or "street, city, state zip, country".
    """
    if not formatted_address or not isinstance(formatted_address, str):
        return ("", "", "")
    parts = [p.strip() for p in formatted_address.split(",")]
    if len(parts) < 2:
        return ("", "", "")
    zip5 = ""
    state = ""
    city = ""
    # US format: often "... , City, STATE ZIP" or "... , City, STATE ZIP, USA"
    state_zip_part = parts[-2].strip().upper() if len(parts) >= 2 else ""
    if len(parts) >= 3:
        city = " ".join(parts[-3].strip().upper().split())
    # Extract zip (5 digits) and 2-letter state from "STATE ZIP" or "TX 76226"
    for token in state_zip_part.split():
        digits = re.sub(r"\D", "", token)
        if len(digits) >= 5:
            zip5 = digits[:5]
        elif len(token) == 2 and token.isalpha():
            state = token
    if not zip5:
        for p in parts:
            for token in p.split():
                digits = re.sub(r"\D", "", token)
                if len(digits) >= 5:
                    zip5 = digits[:5]
                    break
    return (" ".join(city.split()), state, zip5)


def address_match_confidence(our_city: str, our_state: str, our_zip: str,
                             places_formatted_address: str) -> str:
    """
    Return match confidence level for a candidate address:
      'high'   — exact 5-digit zip match
      'medium' — zips differ (or absent) but same city
      'low'    — city absent/differs but same state
      'none'   — no match at all (or no address data to compare)

    Used both as the match predicate (anything != 'none' = match) and to populate
    the match_confidence output column so reviewers can prioritise manual checks.
    Also handles the HQ-vs-store case: 77449 HQ and 77450 store are in the same
    city (KATY), so they get 'medium' rather than failing on zip inequality.
    """
    if not our_city and not our_state and not our_zip:
        return "none"
    p_city, p_state, p_zip = parse_places_address(places_formatted_address)

    # State mismatch is always disqualifying
    if our_state and p_state and our_state != p_state:
        return "none"

    if our_zip and p_zip and our_zip == p_zip:
        return "high"

    # Zips differ or one side is missing — fall back to city
    if our_city and p_city:
        if our_city == p_city:
            return "medium"
        else:
            return "none"  # different city within same state — too ambiguous

    # City absent on one/both sides but state matches
    if our_state and p_state and our_state == p_state:
        return "low"

    return "none"


def address_matches(our_city: str, our_state: str, our_zip: str,
                    places_formatted_address: str) -> bool:
    """True if address_match_confidence is not 'none'."""
    return address_match_confidence(
        our_city, our_state, our_zip, places_formatted_address
    ) != "none"


# ══════════════════════════════════════════════════════════
# Google Places lookup (expanded fields)
# ══════════════════════════════════════════════════════════

PLACES_URL = "https://places.googleapis.com/v1/places:searchText"
FIELD_MASK = (
    "places.id,places.displayName,places.websiteUri,places.googleMapsUri,"
    "places.formattedAddress,places.businessStatus,places.primaryType,places.primaryTypeDisplayName,"
    "places.nationalPhoneNumber,places.rating,places.userRatingCount,places.regularOpeningHours,"
    "places.location"
)


def clean_company_name(name: str) -> str:
    """
    Strip branch/location suffixes for a cleaner Places search query.

    Rule: split on the FIRST occurrence of '-' that is followed by whitespace.
    This handles all variants:
      '601 Sports - Brookhaven'  → '601 Sports'  (space-dash-space)
      'AG 1 Farmers Coop- HQ'   → 'AG 1 Farmers Coop'  (no space before dash)
      '4-D Western - HQ'         → '4-D Western'  (internal dash never has trailing space)
      'A-Bar-L Western - HQ'     → 'A-Bar-L Western'
    Edge case: company names with '- ' in mid-word (extremely rare in B2B data)
    would be truncated — acceptable tradeoff for the coverage gain.
    """
    if not name:
        return name
    return re.split(r'-\s+', name, maxsplit=1)[0].strip()


def _format_opening_hours(hours) -> str:
    """Convert regularOpeningHours to a simple string if present."""
    if not hours:
        return ""
    if isinstance(hours, dict) and "weekdayDescriptions" in hours:
        return "; ".join(hours.get("weekdayDescriptions", []))
    if isinstance(hours, list):
        return "; ".join(str(h) for h in hours[:7])
    return str(hours)


# When Places returns no website, put this in found_url instead of a Maps link
WEBSITE_NOT_FOUND_LABEL = "website not found"


def _place_api_dict_to_result(place: dict) -> dict:
    """Map one Places API place object to our enrichment dict."""
    display_name = place.get("displayName", {})
    matched_name = display_name.get("text") if isinstance(display_name, dict) else str(display_name)

    website_url = place.get("websiteUri")
    maps_url = place.get("googleMapsUri")
    formatted_address = place.get("formattedAddress") or ""
    business_status = place.get("businessStatus", "")
    primary_type = place.get("primaryType", "")
    primary_type_display = place.get("primaryTypeDisplayName", "")
    if isinstance(primary_type_display, dict):
        primary_type_display = primary_type_display.get("text", "") or ""
    national_phone = place.get("nationalPhoneNumber") or ""
    rating = place.get("rating")
    user_rating_count = place.get("userRatingCount")
    regular_hours = place.get("regularOpeningHours")
    hours_str = _format_opening_hours(regular_hours) if regular_hours else ""

    source = "website" if website_url else ("maps" if maps_url else "not_found")

    loc = place.get("location", {}) or {}

    return {
        "place_id": place.get("id"),
        "website_url": website_url,
        "maps_url": maps_url,
        "source": source,
        "matched_name": matched_name,
        "formatted_address": formatted_address,
        "business_status": business_status,
        "primary_type": primary_type,
        "primary_type_display_name": primary_type_display,
        "national_phone_number": national_phone,
        "rating": rating,
        "user_rating_count": user_rating_count,
        "regular_opening_hours": hours_str,
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
    }


def find_places_candidates(
    company_name: str,
    city: str = "",
    state: str = "",
    zip_code: str = "",
    max_result_count: int = 20,
) -> list:
    """
    Text search; returns up to max_result_count place dicts (same shape as single lookup).
    Company-level flow uses company name only (no location) so candidates can match any branch.
    """
    if not company_name or pd.isna(company_name):
        return []

    clean_name = clean_company_name(str(company_name))
    if city or state or zip_code:
        parts = [clean_name, str(city).strip(), str(state).strip(), str(zip_code).strip()]
        text_query = ", ".join(p for p in parts if p)
    else:
        text_query = clean_name

    try:
        resp = requests.post(
            PLACES_URL,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": FIELD_MASK,
            },
            json={
                "textQuery": text_query,
                "maxResultCount": min(max(1, max_result_count), 20),
                "languageCode": "en",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            # Google occasionally returns HTTP 200 with an error body
            raise ValueError(f"Places API error body: {data['error']}")
        places = data.get("places", []) or []
        return [_place_api_dict_to_result(p) for p in places]
    except Exception as e:
        log.warning(f"  Google Places failed for '{company_name}': {e}")
        return None  # None = API/network error; [] = success with zero results


def find_on_google_places(company_name: str, city: str = "", state: str = "", zip_code: str = "") -> dict:
    """
    Search Google Places by company name. Optionally include city, state, zip so
    for multi-location chains we get the branch at that address.
    Returns rich dict with website_url, maps_url, formatted_address, etc.
    Note: source="enrichment_error" when the API call failed; "not_found" when it
    succeeded but returned no results — callers can distinguish the two if needed.
    """
    cands = find_places_candidates(company_name, city, state, zip_code, max_result_count=1)
    if cands is None:           # API/network error
        out = _empty_places_result()
        out["source"] = "enrichment_error"
        return out
    if not cands:               # success, zero results
        out = _empty_places_result()
        out["source"] = "not_found"
        return out
    return cands[0]


def _empty_places_result() -> dict:
    return {
        "place_id": None,
        "website_url": None,
        "maps_url": None,
        "source": "not_found",
        "matched_name": None,
        "formatted_address": "",
        "business_status": "",
        "primary_type": "",
        "primary_type_display_name": "",
        "national_phone_number": "",
        "rating": None,
        "user_rating_count": None,
        "regular_opening_hours": "",
        "latitude": None,
        "longitude": None,
    }


# ── URL classification ────────────────────────────────────────────────────────
_SOCIAL_DOMAINS = frozenset({
    "facebook.com", "instagram.com", "tiktok.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "snapchat.com",
})
_MARKETPLACE_DOMAINS = frozenset({
    "myshopify.com", "square.site", "squareup.com", "etsy.com", "ebay.com",
    "amazon.com", "linktr.ee", "bio.link", "beacons.ai",
})


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
    Classify a URL as one of: website / social / marketplace / maps / not_found.
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
    For social/marketplace URLs: keep the full URL — the path IS the business identity
    (e.g. https://www.facebook.com/annswm/ must not become https://www.facebook.com).
    Returns '' for maps, not_found, or blank.
    """
    if not url:
        return ""
    s = str(url).strip()
    if not s or s == WEBSITE_NOT_FOUND_LABEL:
        return ""
    url_type = classify_url(s)
    if url_type in ("social", "marketplace"):
        # Preserve the full URL — the path identifies the specific business page
        full = s if s.startswith(("http://", "https://")) else "https://" + s
        return full
    if url_type in ("maps", "not_found"):
        return ""
    try:
        full = s if s.startswith(("http://", "https://")) else "https://" + s
        p = urlparse(full)
        if not p.netloc:
            return ""
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""


def classify_retail_type(
    row_is_channel: bool,
    primary_type: str | None,
    has_opening_hours: bool,
) -> str:
    """
    Strict retail classification — no scoring, no guessing.
    Returns 'retail', 'not_retail', or 'unknown'.

    Tier 1 (certain):
      - Channel row (ecom/online name suffix) → not_retail
      - primary_type is warehouse/storage     → not_retail
      - primary_type is store type + hours    → retail

    Tier 2 (~95% certain):
      - primary_type is store type (no hours) → retail
      - has opening hours (no store type)     → retail

    Tier 3 (honest):
      - anything else                         → unknown
    """
    if row_is_channel:
        return "not_retail"

    pt = (primary_type or "").lower().strip()

    if pt in NONRETAIL_PRIMARY_TYPES:
        return "not_retail"

    if pt in RETAIL_PRIMARY_TYPES:
        return "retail"

    if has_opening_hours:
        return "retail"

    return "unknown"


def _check_product_signals(url: str) -> dict:
    """
    Call POST /api/check and return normalised signals.
    Returns dict with keys: sells_anything, sells_shoes, sells_twisted_x.
    All values are "yes" / "no" / "unknown".
    Never raises — errors and timeouts return all "unknown".
    """
    def _b(val) -> str:
        if val is True:  return "yes"
        if val is False: return "no"
        return "unknown"

    try:
        resp = requests.post(
            CHECK_API_URL,
            json={"url": url},
            timeout=CHECK_API_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        sells_online    = data.get("sells_online")
        sells_footwear  = data.get("sells_footwear")
        sells_twisted_x = data.get("sells_twisted_x")

        # sells_anything = yes if the site sells/carries anything at all —
        # either it has online checkout OR it carries shoes OR it carries Twisted X.
        # sells_online alone can be False for physical-store sites that show products
        # without a buy button, so we fold in the other signals.
        if sells_online is True or sells_footwear is True or sells_twisted_x is True:
            sells_anything = "yes"
        elif sells_online is False and sells_footwear is False and sells_twisted_x is False:
            sells_anything = "no"
        else:
            sells_anything = "unknown"

        return {
            "sells_anything":  sells_anything,
            "sells_shoes":     _b(sells_footwear),
            "sells_twisted_x": _b(sells_twisted_x),
        }
    except Exception as exc:
        log.warning(f"Product check failed for {url}: {exc}")
        return {"sells_anything": "unknown", "sells_shoes": "unknown", "sells_twisted_x": "unknown"}


# Legal-entity suffixes stripped from the grouping key only (NOT from the display
# name used as the Places query). Stripping only for key prevents 'Academy' and
# 'Academy LTD' from being treated as different companies while keeping the full
# name in the API query so Google still gets a meaningful search term.
# Risk: two genuinely different companies with the same base name will merge (rare
# in B2B retail data — log a warning if count of rows under a merged key is large).
_LEGAL_SUFFIX_RE = re.compile(
    r'[\s,]+(llc|ltd|l\.l\.c\.|inc|incorporated|corp|corporation|co\.|company)\s*$',
    re.IGNORECASE,
)


def normalize_company_key(row, company_col: str) -> str:
    """
    Stable key for grouping rows by company.
    - Strips branch suffixes via clean_company_name (handles '-HQ', '- ecommerce', etc.)
    - Strips legal suffixes (LLC, LTD, Inc …) so 'Academy' and 'Academy LTD' share one key.
    - Normalises '&' → 'and' so '264 Shoes & Apparel' and '264 Shoes And Apparel' match.
    - Treats dtype=str literal 'nan' as blank.
    Key only — display name for Places query retains the original cleaned name.
    """
    v = row.get(company_col, "")
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    key = clean_company_name(str(v).strip()).lower()
    if key in ("nan", ""):
        return ""
    key = key.replace("&", "and")
    key = _LEGAL_SUFFIX_RE.sub("", key).strip().rstrip(",").strip()
    return " ".join(key.split())  # collapse any double-spaces left by replacements


def collect_branch_norms_for_company(df: pd.DataFrame, company_key: str, company_col: str) -> list:
    """
    All (city, state, zip5) norms for rows belonging to this company — used to match
    any Places candidate against any known branch in the sheet.
    """
    norms = []
    seen = set()
    for idx in df.index:
        r = df.loc[idx]
        if normalize_company_key(r, company_col) != company_key:
            continue
        city = r.get("city", "") if "city" in df.columns else ""
        state = r.get("state", "") if "state" in df.columns else ""
        z = r.get("zip code", "") if "zip code" in df.columns else ""
        trip = normalize_address_for_match(city, state, z)
        if not (trip[0] or trip[1] or trip[2]):
            continue
        if trip not in seen:
            seen.add(trip)
            norms.append(trip)
    return norms


def pick_branch_candidate_for_row(
    candidates: list, city: str, state: str, zip_code: str
) -> tuple:
    """
    Return (candidate, confidence) for the first candidate whose formattedAddress
    matches this row's city/state/zip, or (None, 'none') if no match.
    confidence is one of: 'high' / 'medium' / 'low' / 'none'
    """
    city_n, state_n, zip_n = normalize_address_for_match(city, state, zip_code)
    for cand in candidates:
        fa = cand.get("formatted_address") or ""
        if not fa:
            continue
        conf = address_match_confidence(city_n, state_n, zip_n, fa)
        if conf != "none":
            return cand, conf
    return None, "none"


def pick_places_result_for_company(candidates: list, branch_norms: list) -> Optional[dict]:
    """
    First Places candidate whose formatted_address matches any branch norm.
    Returns None if no candidates or no match (and None if branch_norms empty — cannot verify).
    """
    if not candidates:
        return None
    if not branch_norms:
        return None
    for cand in candidates:
        fa = cand.get("formatted_address") or ""
        if not fa:
            continue
        for c_city, c_state, c_zip in branch_norms:
            if address_matches(c_city, c_state, c_zip, fa):
                return cand
    return None


# Max Places text-search results to scan for a branch match (API cap 20)
PLACES_MAX_CANDIDATES = 20


# ══════════════════════════════════════════════════════════
# SFTP HELPERS
# ══════════════════════════════════════════════════════════

@contextmanager
def sftp_session():
    """
    Context manager that opens a paramiko SFTP connection and yields the client.
    Tries public key (SFTP_KEY_PATH) first, then password with keyboard-interactive
    fallback for SFTPGo and similar servers.
    """
    try:
        from sftp_connect import close_sftp, open_sftp
    except ImportError:
        raise ImportError(
            "sftp_connect.py is required for SFTP (ships with this repo)."
        )

    sftp = None
    try:
        sftp = open_sftp(
            SFTP_HOST,
            SFTP_PORT,
            SFTP_USER,
            SFTP_KEY_PATH,
            SFTP_PASSWORD,
        )
        yield sftp
    finally:
        if sftp is not None:
            close_sftp(sftp)


def resolve_input_file(sftp) -> str:
    """
    Find the oldest CSV in SFTP_INBOUND_DIR (FIFO processing order).
    Returns the full remote path.
    Raises FileNotFoundError with a clear message if the folder is empty.
    """
    try:
        entries = sftp.listdir_attr(SFTP_INBOUND_DIR)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"SFTP inbound directory not found: {SFTP_INBOUND_DIR}"
        )
    csvs = [e for e in entries if e.filename.lower().endswith(".csv")]
    if not csvs:
        raise FileNotFoundError(
            f"No CSV files found in SFTP {SFTP_INBOUND_DIR} — nothing to process."
        )
    # Pick oldest file first (FIFO)
    oldest = sorted(csvs, key=lambda e: e.st_mtime)[0]
    return f"{SFTP_INBOUND_DIR.rstrip('/')}/{oldest.filename}"


def derive_output_filename(input_remote_path: str) -> str:
    """
    Derive the enriched output filename from the input filename.
    e.g. /inbound/customers_20260401.csv → customers_20260401_Enriched.csv
    """
    basename = os.path.basename(input_remote_path)
    stem, ext = os.path.splitext(basename)
    return f"{stem}_Enriched{ext}"


# ══════════════════════════════════════════════════════════
# 30-DAY RE-ENRICHMENT LOGIC
# ══════════════════════════════════════════════════════════

def should_enrich(row) -> bool:
    """
    Return True if this row should be enriched this run.

    Rules:
      - last_enrichment_date is blank/missing → True  (never enriched)
      - last_enrichment_date < 30 days ago   → False (still fresh)
      - last_enrichment_date ≥ 30 days ago   → True  (stale, refresh needed)

    Exceptions — always re-enrich regardless of date:
      - previous enrichment_source == 'enrichment_error'  (last run failed)
      - previous enrichment_source == 'address_mismatch'  (may resolve now)
    """
    prev_src = str(row.get("enrichment_source", "") or "").strip()
    if prev_src in ("enrichment_error", "address_mismatch"):
        return True

    if NETSUITE_LAST_ENRICHED_COL not in row.index:
        return True  # column absent — treat as never enriched

    val = row.get(NETSUITE_LAST_ENRICHED_COL, "")
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return True
    val_str = str(val).strip()
    if val_str in ("", "nan"):
        return True

    try:
        last = pd.to_datetime(val_str, dayfirst=False)
        return (pd.Timestamp.today() - last).days >= ENRICHMENT_TTL_DAYS
    except Exception:
        log.warning(
            f"Could not parse {NETSUITE_LAST_ENRICHED_COL} value {val_str!r} "
            f"— defaulting to re-enrich"
        )
        return True


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def _ensure_columns(df, url_col, address_cols):
    """Ensure required columns exist; normalize names for CSV (strip)."""
    cols = list(df.columns)
    if url_col not in cols:
        for c in cols:
            if c and str(c).strip().lower() == url_col.lower():
                return str(c).strip()
        raise ValueError(f"URL column '{url_col}' not found. Columns: {cols[:15]}...")
    for ac in address_cols:
        if ac not in cols:
            for c in cols:
                if c and str(c).strip().lower() == ac.lower():
                    break
            else:
                log.warning(f"Address column '{ac}' not found; address matching may fail.")
    return url_col


def _best_website(candidates: list) -> str:
    """
    Return the root domain of the first candidate that has a website_url,
    scanning all candidates (not just the address-matched one).
    Strips store/branch paths (e.g. dillards.com/stores/texas/...) so we
    always return the main company website, not a location-specific page.
    """
    for cand in candidates:
        w = (cand.get("website_url") or "").strip()
        if w:
            root = extract_root_domain(w)
            return root if root else w
    return ""


def main():
    # ── Resolve input file (SFTP or local) ───────────────
    local_input_path  = None
    local_output_path = None
    remote_input_path = None
    remote_output_filename = None

    if USE_SFTP:
        log.info("Connecting to SFTP to resolve input file...")
        with sftp_session() as sftp:
            try:
                remote_input_path = resolve_input_file(sftp)
            except FileNotFoundError as e:
                log.info(str(e))
                sys.exit(0)  # empty inbound — nothing to process, exit cleanly

            remote_output_filename = derive_output_filename(remote_input_path)
            # Download to a local temp file for processing
            tmp_dir = tempfile.mkdtemp()
            local_input_path  = os.path.join(tmp_dir, os.path.basename(remote_input_path))
            local_output_path = os.path.join(tmp_dir, remote_output_filename)
            log.info(f"Downloading {remote_input_path} from SFTP...")
            sftp.get(remote_input_path, local_input_path)

        input_file  = local_input_path
        output_file = local_output_path
    else:
        input_file  = INPUT_FILE
        output_file = OUTPUT_FILE

    # ── Load input ───────────────────────────────────────
    log.info(f"Loading {input_file}...")
    is_csv = input_file.lower().endswith(".csv")
    if is_csv:
        df = pd.read_csv(input_file, dtype=str)
    else:
        df = pd.read_excel(input_file, dtype=str)
    log.info(f"Loaded {len(df):,} total records.")

    # ── Rename Celigo column labels → pipeline-internal names ──
    if COLUMN_MAP:
        df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns},
                  inplace=True)

    # ── Validate NetSuite Internal ID column ─────────────
    if USE_SFTP and NETSUITE_ID_COL not in df.columns:
        raise ValueError(
            f"Column '{NETSUITE_ID_COL}' not found in input. "
            f"Add 'customer.id as \"{NETSUITE_ID_COL}\"' to the saved search. "
            f"Columns present: {list(df.columns[:10])}..."
        )

    url_col = _ensure_columns(df, URL_COL, ADDRESS_COLS)

    # Strip invisible/whitespace characters from URL and address columns.
    # dtype=str can surface literal "nan" for blank cells — replace those too.
    _INVISIBLE = '\u200b\u00a0\ufeff\r\n\t'
    for _col in [url_col] + [c for c in ["address", "city", "state", "zip code"] if c in df.columns]:
        df[_col] = (
            df[_col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.strip(_INVISIBLE)
            .replace({"nan": "", "NaN": ""})  # dtype=str literal "nan" → blank
            .replace("", pd.NA)
        )

    addr_cols = [c for c in ADDRESS_COLS if c in df.columns]
    if not addr_cols:
        addr_cols = [c for c in df.columns if str(c).strip().lower() in ("address", "city", "state", "zip code")]

    # ── 30-day freshness filter ───────────────────────────────
    # Rows enriched within the last 30 days are tagged skipped_fresh and excluded
    # from both the ping step and Places lookup, saving API calls.
    today_iso = date.today().isoformat()

    if NETSUITE_LAST_ENRICHED_COL in df.columns:
        fresh_mask   = df.apply(lambda r: not should_enrich(r), axis=1)
        fresh_idx    = set(df.index[fresh_mask].tolist())
        enrich_idx   = set(df.index[~fresh_mask].tolist())
        log.info(f"Rows skipped (fresh ≤ {ENRICHMENT_TTL_DAYS} days): {len(fresh_idx):,}")
        log.info(f"Rows to enrich (blank/stale date):                {len(enrich_idx):,}")
    else:
        fresh_idx  = set()
        enrich_idx = set(df.index.tolist())
        log.info(f"'{NETSUITE_LAST_ENRICHED_COL}' column not in input — enriching all rows.")

    # ── Identify which rows have a valid-looking URL ─────────
    # Only consider rows that are due for enrichment
    enrich_series    = df.index.isin(enrich_idx)
    is_blank_or_invalid = df.apply(lambda r: is_url_blank_or_invalid(r.get(url_col)), axis=1)
    is_blank_or_invalid = is_blank_or_invalid & enrich_series
    had_valid_url = set(df.index[~is_blank_or_invalid & enrich_series].tolist())

    # ── Ping ALL rows (blank and existing) ───────────────────
    # Rows with valid-looking URLs are now also checked so dead/redirected
    # sites are caught and sent to Places instead of silently skipped.
    already_alive_idx = []
    ping_with_status = df.copy()

    if PING_EXISTING_URLS:
        log.info("Step 1: Pinging URLs for rows due for enrichment...")
        # Only ping rows that are due for enrichment — skip fresh rows entirely
        enrich_list = sorted(enrich_idx)
        urls_to_ping = df.loc[enrich_list, url_col].fillna("").astype(str).tolist()
        check_results = asyncio.run(bulk_check_urls(urls_to_ping))
        for i, idx in enumerate(enrich_list):
            ping_with_status.at[idx, "_url_status"] = check_results[i]["status"]
            ping_with_status.at[idx, "_final_url"]  = check_results[i]["final_url"]
            ping_with_status.at[idx, "_http_code"]  = check_results[i]["http_code"]
        alive_mask        = ping_with_status["_url_status"].isin(["active", "redirected", "blocked"])
        # Only rows in enrich_idx can be alive or broken
        already_alive_idx = [i for i in ping_with_status.index[alive_mask] if i in enrich_idx]
        broken_idx        = [i for i in enrich_list if i not in already_alive_idx]
    else:
        # No ping: only rows with blank/invalid URLs need Places
        ping_with_status["_url_status"] = "missing"
        ping_with_status["_final_url"]  = None
        ping_with_status["_http_code"]  = None
        broken_idx = [i for i in df.index[is_blank_or_invalid] if i in enrich_idx]

    # Unified status lookup used later when writing url_check_status per row
    broken_with_status = ping_with_status

    log.info(f"Records with blank/invalid URL: {int(is_blank_or_invalid.sum()):,}")
    log.info(f"Need Google Places lookup: {len(broken_idx):,}")

    # ── Init output columns ───────────────────────────────
    # Branch columns that must be cleared on every non-match path (single source of truth)
    BRANCH_COLS_CLEAR = [
        "places_formatted_address", "places_national_phone", "found_maps_url",
        "places_regular_opening_hours", "places_rating", "places_user_rating_count",
        "matched_name", "places_business_status", "places_primary_type",
        "places_primary_type_display_name", "places_latitude", "places_longitude",
        "places_place_id", "match_confidence",
    ]
    for col in [
        "url_check_status", "url_http_code", "found_url", "found_url_type", "found_root_domain",
        "found_maps_url", "enrichment_source", "address_match", "match_confidence", "matched_name",
        "places_business_status", "places_primary_type", "places_primary_type_display_name",
        "places_formatted_address", "places_national_phone",
        "places_rating", "places_user_rating_count", "places_regular_opening_hours",
        "places_latitude", "places_longitude", "places_place_id", "url_was_backfilled",
        "enrichment_run_date", "retail_type",
        "sells_anything", "sells_shoes", "sells_twisted_x",
    ]:
        if col not in df.columns:
            df[col] = None
    df["url_was_backfilled"] = False  # explicit False on all rows; avoids NaN/False mix
    df["enrichment_run_date"] = ""    # blank by default; filled only for enriched rows

    # Fresh rows — tag them now, leave all their existing columns untouched
    if fresh_idx:
        df.loc[list(fresh_idx), "enrichment_source"] = "skipped_fresh"
        # enrichment_run_date stays blank for fresh rows so Celigo doesn't
        # overwrite the existing NetSuite date with an empty value

    df.loc[list(enrich_idx), "enrichment_source"] = None
    # Rows not going to Places and not alive-from-blank = skipped
    df.loc[~df.index.isin(broken_idx + list(already_alive_idx)) & df.index.isin(enrich_idx),
           "enrichment_source"] = "skipped"

    # Write ping results for alive rows
    for idx in already_alive_idx:
        row = ping_with_status.loc[idx]
        status    = row.get("_url_status", "active")
        final_url = row.get("_final_url") or row.get(url_col)
        http_code = row.get("_http_code")
        df.at[idx, "url_check_status"] = status
        df.at[idx, "url_http_code"]    = str(http_code) if http_code is not None else ""
        df.at[idx, "found_url"]        = final_url
        df.at[idx, "found_url_type"]   = classify_url(final_url)
        df.at[idx, "found_root_domain"] = extract_root_domain(final_url)
        if idx in had_valid_url:
            # Had a real URL and it's alive — Places will still run for this row
            # (to get address/phone/hours/retail data). enrichment_source will be
            # overwritten by the Places loop; set a placeholder for now.
            df.at[idx, "enrichment_source"] = "url_alive"
        else:
            # Was blank/invalid but the ping somehow resolved — treat as url_alive
            df.at[idx, "enrichment_source"] = "url_alive"
            df.at[idx, "enrichment_run_date"] = today_iso

    # ── Pre-build branch norms dict once — O(n) vs O(n×m) per company ────────
    company_branch_norms: dict = {}
    for idx in df.index:
        r = df.loc[idx]
        ck = normalize_company_key(r, COMPANY_COL)
        if not ck:
            continue
        city  = r.get("city", "")     if "city"     in df.columns else ""
        state = r.get("state", "")    if "state"    in df.columns else ""
        z     = r.get("zip code", "") if "zip code" in df.columns else ""
        trip  = normalize_address_for_match(city, state, z)
        if not (trip[0] or trip[1] or trip[2]):
            continue
        norms = company_branch_norms.setdefault(ck, [])
        if trip not in norms:
            norms.append(trip)

    # ── Dedupe by company (one Places lookup per company) ──
    # alive_idx rows are included so Places still runs for them (needed to get
    # address/phone/hours/retail_type on first enrichment). Their URL is already
    # correct from the ping — it will NOT be overwritten in the write-back.
    already_alive_set = set(already_alive_idx)
    company_to_indices: dict = {}
    for idx in broken_idx + already_alive_idx:
        if idx not in df.index:
            continue
        r = df.loc[idx]
        ck = normalize_company_key(r, COMPANY_COL)
        if not ck:
            # Blank company — write full consistent failure row, skip Places
            _bws = broken_with_status.loc[idx] if idx in broken_with_status.index else {}
            df.at[idx, "enrichment_source"]  = "not_found"
            df.at[idx, "found_url"]          = WEBSITE_NOT_FOUND_LABEL
            df.at[idx, "found_url_type"]     = "not_found"
            df.at[idx, "found_root_domain"]  = ""
            df.at[idx, "address_match"]      = False
            df.at[idx, "url_was_backfilled"] = False
            df.at[idx, "url_check_status"]   = _bws.get("_url_status", "missing")
            _hc = _bws.get("_http_code")
            df.at[idx, "url_http_code"]      = str(_hc) if _hc is not None else ""
            for col in BRANCH_COLS_CLEAR:
                df.at[idx, col] = ""
            continue
        company_to_indices.setdefault(ck, []).append(idx)

    unique_companies = list(company_to_indices.keys())
    log.info(f"Companies needing Places lookup: {len(unique_companies):,}")

    # ── Google Places: hybrid company-level website + per-row branch fields ──
    if GOOGLE_PLACES_API_KEY == "YOUR_API_KEY_HERE":
        log.warning("No Google Places API key — skipping enrichment.")
    else:
        def _v(val):
            """Coerce None to '' so CSV cells are empty, not 'None'."""
            return val if val is not None else ""

        for ck in tqdm(unique_companies, desc="  Google Places lookup"):
            indices = company_to_indices[ck]
            first_idx = indices[0]
            company_display = df.loc[first_idx].get(COMPANY_COL, "")

            # Use pre-built dict (O(1)) instead of scanning full DataFrame per company
            branch_norms = company_branch_norms.get(ck, [])

            # Include city+state from any row that has them as a location hint.
            # This anchors Google to the right geography for large chains
            # (e.g. Academy Sports has 300+ stores; without a hint, Google serves
            # whichever cluster is nearest its data centre, not the customer's city).
            hint_city, hint_state = "", ""
            for _hi in indices:
                _r = df.loc[_hi]
                _c = str(_r.get("city", "") or "").strip()
                _s = str(_r.get("state", "") or "").strip()
                if _c and _c.lower() not in ("nan", ""):
                    hint_city = _c
                if _s and _s.lower() not in ("nan", ""):
                    hint_state = _s
                if hint_city and hint_state:
                    break

            candidates = find_places_candidates(
                company_display, hint_city, hint_state, "", max_result_count=PLACES_MAX_CANDIDATES
            )

            # ── Global fallback for website resolution ────────────────────
            # When a location hint is used, Google sometimes returns only the
            # nearest location (e.g. a warehouse with no website). If none of
            # the local candidates have a website, do a second hint-free search
            # to find the company's main website from their national listings.
            # Branch address matching still uses the original local candidates.
            _local_website = _best_website(candidates) if candidates else ""
            if not _local_website and (hint_city or hint_state):
                _global_candidates = find_places_candidates(
                    company_display, "", "", "", max_result_count=PLACES_MAX_CANDIDATES
                )
                _global_website = _best_website(_global_candidates) if _global_candidates else ""
            else:
                _global_website = ""
            _resolved_website = _local_website or _global_website

            # ── Detect channel rows (ecommerce/online) — Fix #6 ─────────
            # A row whose suffix after the dash is a channel keyword has no
            # physical address. Its address data in the sheet is the HQ address,
            # not a retail branch, so normal address verification will always fail.
            # These rows skip verification and accept the top candidate directly.
            def _is_channel_row(raw_company: str) -> bool:
                parts = re.split(r'-\s+', str(raw_company), maxsplit=1)
                if len(parts) < 2:
                    return False
                suffix = parts[1].strip().lower()
                return suffix in CHANNEL_KEYWORDS

            # ── Company-level resolution (shared found_url) ──────────────
            if candidates is None:
                # API/network error — distinct from zero results
                company_status = "enrichment_error"
                company_pick = None
                found_url_value = WEBSITE_NOT_FOUND_LABEL
            elif not candidates:
                company_status = "not_found"
                company_pick = None
                found_url_value = WEBSITE_NOT_FOUND_LABEL
            elif not branch_norms:
                # No address data in the sheet for any row of this company.
                # Fix #9: if ACCEPT_UNVERIFIED_MATCH is on, take top candidate.
                if ACCEPT_UNVERIFIED_MATCH:
                    company_pick = candidates[0]
                    found_url_value = _resolved_website if _resolved_website else WEBSITE_NOT_FOUND_LABEL
                    company_status = "unverified_match"
                else:
                    company_status = "address_mismatch"
                    company_pick = None
                    found_url_value = WEBSITE_NOT_FOUND_LABEL
            else:
                company_pick = pick_places_result_for_company(candidates, branch_norms)
                if company_pick is None:
                    company_status = "address_mismatch"
                    found_url_value = WEBSITE_NOT_FOUND_LABEL
                else:
                    found_url_value = _resolved_website if _resolved_website else WEBSITE_NOT_FOUND_LABEL
                    company_status = "resolved"

            # ── Per-row write-back ───────────────────────────────────────
            for idx in indices:
                row = df.loc[idx]
                _bws = broken_with_status.loc[idx] if idx in broken_with_status.index else {}

                # Fix #6: channel rows (ecommerce/online) skip address verification
                # and accept the top candidate, tagged 'unverified_match'
                row_is_channel = _is_channel_row(str(row.get(COMPANY_COL, "")))

                df.at[idx, "url_check_status"]  = _bws.get("_url_status", "missing")
                _hc = _bws.get("_http_code")
                df.at[idx, "url_http_code"]     = str(_hc) if _hc is not None else ""
                # For rows whose URL was already alive, keep the ping-resolved URL —
                # don't overwrite with Places' found_url_value.
                if idx not in already_alive_set:
                    df.at[idx, "found_url"]         = found_url_value
                    df.at[idx, "found_url_type"]    = classify_url(found_url_value)
                    df.at[idx, "found_root_domain"] = extract_root_domain(found_url_value)

                if company_status in ("not_found", "enrichment_error"):
                    enrichment_src = company_status
                    branch = None
                    match_conf = "none"
                elif company_status == "address_mismatch" and not row_is_channel:
                    enrichment_src = "address_mismatch"
                    branch = None
                    match_conf = "none"
                elif company_status in ("resolved", "unverified_match") or row_is_channel:
                    # For channel rows, use top candidate regardless of address
                    if row_is_channel and candidates:
                        branch = candidates[0]
                        match_conf = "none"  # no address verified — explicitly flagged
                        enrichment_src = "unverified_match"
                    elif company_status == "unverified_match":
                        branch = candidates[0] if candidates else None
                        match_conf = "none"
                        enrichment_src = "unverified_match"
                    else:
                        # Normal verified path — per-row branch match + confidence
                        branch, match_conf = pick_branch_candidate_for_row(
                            candidates,
                            row.get("city", ""),
                            row.get("state", ""),
                            row.get("zip code", ""),
                        )
                        enrichment_src = "hybrid_full" if branch else "hybrid_website_only"
                else:
                    # address_mismatch for channel rows treated as unverified_match
                    branch = candidates[0] if candidates else None
                    match_conf = "none"
                    enrichment_src = "unverified_match"

                df.at[idx, "enrichment_source"]   = enrichment_src
                df.at[idx, "address_match"]       = branch is not None
                df.at[idx, "match_confidence"]    = match_conf
                df.at[idx, "enrichment_run_date"] = today_iso

                if branch:
                    df.at[idx, "places_formatted_address"]         = _v(branch.get("formatted_address"))
                    df.at[idx, "places_national_phone"]            = _v(branch.get("national_phone_number"))
                    df.at[idx, "found_maps_url"]                   = _v(branch.get("maps_url"))
                    df.at[idx, "places_regular_opening_hours"]     = _v(branch.get("regular_opening_hours"))
                    df.at[idx, "places_rating"]                    = _v(branch.get("rating"))
                    df.at[idx, "places_user_rating_count"]         = _v(branch.get("user_rating_count"))
                    df.at[idx, "matched_name"]                     = _v(branch.get("matched_name"))
                    df.at[idx, "places_business_status"]           = _v(branch.get("business_status"))
                    df.at[idx, "places_primary_type"]              = _v(branch.get("primary_type"))
                    df.at[idx, "places_primary_type_display_name"] = _v(branch.get("primary_type_display_name"))
                    df.at[idx, "places_latitude"]                  = _v(branch.get("latitude"))
                    df.at[idx, "places_longitude"]                 = _v(branch.get("longitude"))
                    df.at[idx, "places_place_id"]                  = _v(branch.get("place_id"))
                else:
                    for col in BRANCH_COLS_CLEAR:
                        df.at[idx, col] = ""

                df.at[idx, "retail_type"] = classify_retail_type(
                    row_is_channel=row_is_channel,
                    primary_type=df.at[idx, "places_primary_type"] or None,
                    has_opening_hours=bool(df.at[idx, "places_regular_opening_hours"]),
                )

                if (
                    FILL_BLANK_WEBSITE_WHEN_MATCHED
                    and enrichment_src in ("hybrid_full", "hybrid_website_only", "unverified_match")
                    and company_pick
                    and company_pick.get("website_url")
                    and is_url_blank_or_invalid(df.at[idx, url_col])
                ):
                    df.at[idx, url_col] = company_pick["website_url"]
                    df.at[idx, "url_was_backfilled"] = True
                    # Re-classify with the backfilled URL
                    df.at[idx, "found_url_type"]    = classify_url(company_pick["website_url"])
                    df.at[idx, "found_root_domain"] = extract_root_domain(company_pick["website_url"])

            time.sleep(0.05)

    # ── Product check pass (optional, requires api_server) ──
    if ENABLE_PRODUCT_CHECK:
        checkable_mask = (
            df["found_url_type"].eq("website") &
            df["found_root_domain"].notna() &
            df["found_root_domain"].ne("")
        )
        unique_domains = df.loc[checkable_mask, "found_root_domain"].unique()
        log.info(f"Product check: {len(unique_domains):,} unique domains to check")

        domain_results = {}
        with ThreadPoolExecutor(max_workers=CHECK_WORKERS) as pool:
            future_to_domain = {pool.submit(_check_product_signals, d): d for d in unique_domains}
            for future in tqdm(as_completed(future_to_domain), total=len(unique_domains), desc="Product check"):
                domain = future_to_domain[future]
                domain_results[domain] = future.result()

        for domain, signals in domain_results.items():
            mask = df["found_root_domain"].eq(domain)
            df.loc[mask, "sells_anything"]  = signals["sells_anything"]
            df.loc[mask, "sells_shoes"]     = signals["sells_shoes"]
            df.loc[mask, "sells_twisted_x"] = signals["sells_twisted_x"]

    # ── Save ──────────────────────────────────────────────
    if is_csv:
        df.to_csv(output_file, index=False)
    else:
        df.to_excel(output_file, index=False)

    # Summary
    log.info(f"\n{'='*52}")
    log.info(f"Saved {len(df):,} rows → {output_file}")
    log.info(f"{'='*52}")
    if "enrichment_source" in df.columns:
        counts = df["enrichment_source"].fillna("(none)").value_counts()
        for status, count in counts.items():
            log.info(f"  {str(status):<28} {count:>6,}")
    log.info(f"{'='*52}")

    # ── SFTP upload + archive ──────────────────────────────
    if USE_SFTP:
        review_remote_path  = f"{SFTP_REVIEW_DIR.rstrip('/')}/{os.path.basename(output_file)}"
        archive_remote_path = f"{SFTP_ARCHIVE_DIR.rstrip('/')}/{os.path.basename(remote_input_path)}"
        log.info(f"Uploading enriched file to SFTP {review_remote_path}...")
        with sftp_session() as sftp:
            sftp.put(output_file, review_remote_path)
            log.info(f"Archiving input: {remote_input_path} → {archive_remote_path}")
            sftp.rename(remote_input_path, archive_remote_path)
        log.info("SFTP upload and archive complete.")


if __name__ == "__main__":
    main()
