"""
All environment variables, column mappings, and pipeline constants.

Every other enrichment module imports from here. Changing a constant in this
file propagates everywhere — no other file hardcodes configuration values.
"""
from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — env vars must be set in the shell

# ---------------------------------------------------------------------------
# Google Places API
# ---------------------------------------------------------------------------

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")
if not GOOGLE_PLACES_API_KEY:
    raise RuntimeError(
        "GOOGLE_PLACES_API_KEY is not set. Add it to .env (see .env.example) "
        "or export it in the shell before running the pipeline."
    )

PLACES_URL = "https://places.googleapis.com/v1/places:searchText"
FIELD_MASK = (
    "places.id,places.displayName,places.websiteUri,places.googleMapsUri,"
    "places.formattedAddress,places.businessStatus,places.primaryType,places.primaryTypeDisplayName,"
    "places.nationalPhoneNumber,places.rating,places.userRatingCount,places.regularOpeningHours,"
    "places.location"
)
PLACES_MAX_CANDIDATES = 20

# Max Places text-search results to request per company (API cap is 20)
PLACES_REQUEST_TIMEOUT = 10

# ---------------------------------------------------------------------------
# Address Validation API + Places Details API (used by /api/enrich endpoint)
# ---------------------------------------------------------------------------

# POST endpoint — validates a physical address and returns a placeId when found.
# Requires "Address Validation API" enabled in Google Cloud Console.
ADDRESS_VALIDATION_URL = "https://addressvalidation.googleapis.com/v1:validateAddress"

# GET endpoint — fetches full place details by placeId.
# Requires "Places API (New)" (places.googleapis.com) enabled — distinct from
# the legacy Places API.  Note: field mask has no "places." prefix (single
# object, not a list).
PLACES_DETAILS_URL = "https://places.googleapis.com/v1/places/{place_id}"
PLACES_DETAILS_FIELD_MASK = (
    "id,displayName,websiteUri,googleMapsUri,formattedAddress,"
    "businessStatus,primaryType,primaryTypeDisplayName,nationalPhoneNumber,"
    "rating,userRatingCount,regularOpeningHours,location"
)

# Per-request timeouts in seconds.  Both APIs are fast under normal conditions;
# 8 s gives headroom for a single retry on 429/503 without blocking the caller.
ADDRESS_VALIDATION_TIMEOUT = 8  # seconds
PLACES_DETAILS_TIMEOUT = 8  # seconds

# ---------------------------------------------------------------------------
# SFTP
# ---------------------------------------------------------------------------

SFTP_HOST        = os.getenv("SFTP_HOST", "")
SFTP_PORT        = int(os.getenv("SFTP_PORT", 22))
SFTP_USER        = os.getenv("SFTP_USER", "")

_SFTP_KEY_RAW  = (os.getenv("SFTP_KEY_PATH", "") or "").strip()
_SFTP_KEY_PATH = _SFTP_KEY_RAW.split("#", 1)[0].strip() if _SFTP_KEY_RAW else ""
if _SFTP_KEY_PATH.startswith("Users/") or _SFTP_KEY_PATH.startswith("home/"):
    _SFTP_KEY_PATH = "/" + _SFTP_KEY_PATH
SFTP_KEY_PATH    = os.path.expanduser(_SFTP_KEY_PATH)
SFTP_PASSWORD    = os.getenv("SFTP_PASSWORD", "")
SFTP_INBOUND_DIR = os.getenv("SFTP_INBOUND_DIR", "/inbound")
SFTP_REVIEW_DIR  = os.getenv("SFTP_REVIEW_DIR",  "/review")
SFTP_ARCHIVE_DIR = os.getenv("SFTP_ARCHIVE_DIR", "/archive")

# Set to True for the automated Celigo flow; False for local/manual runs.
USE_SFTP = os.getenv("USE_SFTP", "false").lower() == "true"

# Used only when USE_SFTP=False
INPUT_FILE  = os.getenv("INPUT_FILE",  "QueryResults_837.csv")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "QueryResults_837_Enriched.csv")

# ---------------------------------------------------------------------------
# NetSuite column names
# ---------------------------------------------------------------------------

COMPANY_COL     = "Company"
URL_COL         = "website url"
ADDRESS_COLS    = ["address", "city", "state", "zip code"]
NETSUITE_ID_COL = "Internal ID"

# Celigo / saved-search export labels → pipeline-internal names.
# Applied at load time so the rest of the pipeline uses consistent names.
COLUMN_MAP = {
    "internalid":               "Internal ID",
    "company":                  "Company",
    "Company Name":             "Company",
    "Web Address":              "website url",
    "Shipping Address 1":       "address",
    "Shipping City":            "city",
    "Shipping State/Province":  "state",
    "Shipping Zip":             "zip code",
    "Last Enrichment Date":     "last_enrichment_date",
    "name":                     "Company",
    "url":                      "website url",
    "zip":                      "zip code",
}

# ---------------------------------------------------------------------------
# Re-enrichment cadence
# ---------------------------------------------------------------------------

NETSUITE_LAST_ENRICHED_COL = "last_enrichment_date"
ENRICHMENT_TTL_DAYS        = int(os.getenv("ENRICHMENT_TTL_DAYS", 30))

# ---------------------------------------------------------------------------
# Pipeline behaviour flags
# ---------------------------------------------------------------------------

# Ping existing URLs before deciding whether to call Google Places.
PING_EXISTING_URLS = True

# Never overwrite the original NetSuite website URL — Celigo owns the write-back.
# Set True only if you want the pipeline to backfill blank URL cells.
FILL_BLANK_WEBSITE_WHEN_MATCHED = False

# Accept top Places candidate when no address data is available for verification.
ACCEPT_UNVERIFIED_MATCH = True

# Suffixes that identify a "channel" row (ecommerce/online) with no physical branch.
CHANNEL_KEYWORDS = {"ecommerce", "e-commerce", "ecom", "online", "web", "website"}

# ---------------------------------------------------------------------------
# URL / ping settings
# ---------------------------------------------------------------------------

CONCURRENT_CHECKS = 100
REQUEST_TIMEOUT   = 5

# Values that mean "no URL" (checked case-insensitively after strip)
URL_BLACKLIST = {"", "n/a", "na", "-", "tbd", "none", "no website", "null"}

WEBSITE_NOT_FOUND_LABEL = "website not found"

# ---------------------------------------------------------------------------
# Product check (optional — requires api_server running)
# ---------------------------------------------------------------------------

ENABLE_PRODUCT_CHECK = os.getenv("ENABLE_PRODUCT_CHECK", "").lower() in ("1", "true", "yes")
CHECK_API_URL        = os.getenv("CHECK_API_URL", "http://localhost:8000/api/check")
CHECK_API_TIMEOUT    = int(os.getenv("CHECK_API_TIMEOUT", 60))
CHECK_WORKERS        = int(os.getenv("CHECK_WORKERS", 3))

# ---------------------------------------------------------------------------
# Retail classification
# ---------------------------------------------------------------------------

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
