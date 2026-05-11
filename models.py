"""
Pydantic models for Twisted X Scraper API (Celigo rearchitecture)
"""
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field


# =============================================================================
# /api/scrape models
# =============================================================================

class ProductBlock(BaseModel):
    """Product block extracted from DOM (before LLM extraction)"""
    text: str = Field(..., description="Visible text of the product block")
    html_snippet: str = Field(..., description="Raw HTML snippet (truncated to ~3000 chars)")
    images: List[Dict[str, str]] = Field(default_factory=list, description="List of images: [{src, alt}]")
    links: List[Dict[str, str]] = Field(default_factory=list, description="List of links: [{href, text}]")


class ScrapeRequestNew(BaseModel):
    """Request for POST /api/scrape"""
    url: str = Field(..., description="Retailer URL to scrape")
    search_term: str = Field(default="Twisted X", description="Search term to use")
    max_pages: int = Field(default=15, description="Maximum pagination pages to process")
    timeout: int = Field(default=30000, description="Page load timeout in milliseconds")


class ScrapeResponse(BaseModel):
    """Response from POST /api/scrape"""
    url: str = Field(..., description="The URL that was scraped")
    retailer: str = Field(..., description="Retailer name extracted from URL")
    scraped_at: str = Field(..., description="ISO timestamp of when scraping occurred")
    method: str = Field(..., description="Extraction method: 'targeted' or 'fullpage_cleaned'")
    store_type: str = Field(..., description="Store type: ecommerce, company_store, brand_site, unknown")
    sells_online: bool = Field(..., description="Whether the site sells products online")
    online_confidence: str = Field(default="low", description="Confidence level: high, medium, low")
    online_indicators: List[str] = Field(default_factory=list, description="E-commerce indicators found")
    blockers: List[str] = Field(default_factory=list, description="In-store/offline blockers found")
    product_count: int = Field(default=0, description="Number of product blocks found")
    products: List[ProductBlock] = Field(default_factory=list, description="List of product blocks")
    errors: List[str] = Field(default_factory=list, description="List of errors encountered")


# =============================================================================
# /api/check models
# =============================================================================

class CheckRequest(BaseModel):
    """Request for POST /api/check"""
    url: str = Field(..., description="Retailer URL to check")


class CheckResponse(BaseModel):
    """Response from POST /api/check"""
    url: str = Field(..., description="The URL that was checked")
    retailer: str = Field(..., description="Retailer name extracted from URL")
    sells_twisted_x: Optional[bool] = Field(None, description="True/False if known; None when blocked (unknown, manual check required)")
    sells_footwear: Optional[bool] = Field(None, description="True/False if known; None when unknown")
    confidence: str = Field(default="low", description="Confidence level: high, medium, low")
    store_type: str = Field(..., description="Store type: ecommerce, company_store, brand_site, unknown")
    sells_online: bool = Field(..., description="Whether the site sells products online")
    proof: List[str] = Field(default_factory=list, description="Evidence explaining the determination")
    sample_products: List[Dict[str, str]] = Field(default_factory=list, description="Sample Twisted X products found (name, price, image)")
    page_url: Optional[str] = Field(default=None, description="URL of the page where products were found")
    checked_at: str = Field(..., description="ISO timestamp of when the check was performed")
    error: Optional[str] = Field(default=None, description="Error message if check failed")
    blocked: bool = Field(default=False, description="True if site appears to block automated access; verify manually")
    blocked_reasons: Optional[str] = Field(default=None, description="Why the site was marked as blocked (e.g. Cloudflare, bot detection)")


# =============================================================================
# /api/verify models
# =============================================================================

class VerifyRequest(BaseModel):
    """Request for POST /api/verify"""
    extracted_products: List[Dict[str, Any]] = Field(..., description="LLM-extracted products from Celigo")
    original_products: List[Dict[str, Any]] = Field(..., description="Original ProductBlocks from /api/scrape")


class VerifyResponse(BaseModel):
    """Response from POST /api/verify"""
    verified_products: List[Dict[str, Any]] = Field(default_factory=list, description="Products that passed verification")
    flagged_products: List[Dict[str, Any]] = Field(default_factory=list, description="Products with verification issues")
    verification_stats: Dict[str, Any] = Field(default_factory=dict, description="Summary statistics")


# =============================================================================
# /api/enrich models
# =============================================================================

class EnrichRequest(BaseModel):
    """Request for POST /api/enrich"""
    company:     str           = Field(..., min_length=1, max_length=200, description="Company name from NetSuite")
    address:     str           = Field(..., min_length=1, max_length=200, description="Street address line")
    city:        str           = Field(..., min_length=1, max_length=100, description="City name")
    state:       str           = Field(..., pattern=r'^[A-Za-z]{2}$',     description="2-letter US state code (e.g. AZ)")
    zip_code:    str           = Field(..., pattern=r'^\d{5}(-\d{4})?$',  description="5-digit ZIP or ZIP+4")
    current_url: Optional[str] = Field(default=None, max_length=500,      description="Existing website URL from NetSuite (informational, not used in lookup)")
    internal_id: Optional[str] = Field(default=None, max_length=50,       description="NetSuite internal ID (passed through, never logged)")
    # Pydantic returns 422 automatically on any validation failure — no Google API quota is burned


class EnrichResponse(BaseModel):
    """Response from POST /api/enrich"""
    # Google Places result fields
    found_url:                    Optional[str]   = Field(default=None,  description="Website URL from Google Places (may differ from current_url)")
    found_maps_url:               Optional[str]   = Field(default=None,  description="Google Maps URL for the matched location")
    matched_name:                 Optional[str]   = Field(default=None,  description="Business name as returned by Google Places")
    places_place_id:              Optional[str]   = Field(default=None,  description="Google Places place ID")
    places_formatted_address:     str             = Field(default="",    description="Standardised address from Google Places")
    places_national_phone:        str             = Field(default="",    description="Phone number from Google Places")
    places_rating:                Optional[float] = Field(default=None,  description="Google Places rating (1–5)")
    places_regular_opening_hours: str             = Field(default="",    description="Opening hours as a semicolon-separated string")
    places_latitude:              Optional[float] = Field(default=None,  description="Latitude from Google Places")
    places_longitude:             Optional[float] = Field(default=None,  description="Longitude from Google Places")
    places_business_status:       str             = Field(default="",    description="Google Places businessStatus (e.g. OPERATIONAL)")
    places_primary_type:          str             = Field(default="",    description="Google Places primaryType (e.g. shoe_store)")
    # Enrichment metadata
    match_confidence:             str             = Field(default="none", description="Address match confidence: high | medium | low | none")
    enrichment_source:            str             = Field(default="",     description="How the result was obtained: address_validation | text_search | not_found | enrichment_error")
    address_match:                bool            = Field(default=False,  description="True when match_confidence != none AND places_place_id is not null")


class EnrichPipelineResponse(BaseModel):
    """Response from POST /api/enrich/pipeline"""
    status:       str            = Field(...,        description="'completed' or 'error'")
    message:      str            = Field(default="", description="Human-readable summary of the pipeline run")
    started_at:   str            = Field(...,        description="ISO 8601 UTC timestamp when the pipeline started")
    completed_at: str            = Field(default="", description="ISO 8601 UTC timestamp when the pipeline completed")
    duration_sec: Optional[float] = Field(default=None, description="Total pipeline runtime in seconds")


# =============================================================================
# /api/enrich/ttl-check models
# =============================================================================

class TtlCheckItem(BaseModel):
    """Single record to check against the enrichment TTL."""
    internal_id:          str           = Field(...,          min_length=1, max_length=50,  description="NetSuite internal ID")
    last_enrichment_date: Optional[str] = Field(default=None, max_length=50,               description="ISO date of last enrichment — null/blank means never enriched")
    enrichment_source:    Optional[str] = Field(default=None, max_length=50,               description="Previous enrichment_source value; 'enrichment_error' or 'address_mismatch' forces re-enrich regardless of TTL")


class TtlCheckResponse(BaseModel):
    """Response from POST /api/enrich/ttl-check"""
    fresh:    List[str] = Field(default_factory=list, description="IDs enriched within TTL — safe to skip")
    stale:    List[str] = Field(default_factory=list, description="IDs that need re-enrichment (never enriched, past TTL, or previous error)")
    ttl_days: int       = Field(...,                  description="TTL window used for this check in days")


# =============================================================================
# /api/enrich/url-ping models
# =============================================================================

class UrlPingItem(BaseModel):
    """Single record to ping for URL liveness."""
    internal_id: str           = Field(...,          min_length=1, max_length=50,  description="NetSuite internal ID")
    url:         Optional[str] = Field(default=None, max_length=500,               description="URL to ping — null or blank is treated as missing")


class UrlPingDetail(BaseModel):
    """Per-record result from the URL ping."""
    internal_id: str           = Field(...,          description="NetSuite internal ID")
    status:      str           = Field(...,          description="active | redirected | blocked | dead | missing")
    http_code:   Optional[int] = Field(default=None, description="Final HTTP status code — null for network errors or missing URLs")
    final_url:   Optional[str] = Field(default=None, description="Resolved URL after redirects — null for dead/missing")


class UrlPingResponse(BaseModel):
    """Response from POST /api/enrich/url-ping"""
    alive:   List[str]           = Field(default_factory=list, description="IDs whose URLs are live (active / redirected / blocked by bot-detection)")
    dead:    List[str]           = Field(default_factory=list, description="IDs whose URLs are unreachable or returning errors")
    missing: List[str]           = Field(default_factory=list, description="IDs with no URL (null / blank / placeholder)")
    details: List[UrlPingDetail] = Field(default_factory=list, description="Per-ID status breakdown with HTTP code and final URL")


# =============================================================================
# /api/enrich/batch models
# =============================================================================

class BatchEnrichItem(BaseModel):
    """Single record result within a batch enrichment response."""
    internal_id: str          = Field(...,        description="Internal ID echoed from the request (empty string when not provided)")
    result:      EnrichResponse = Field(...,      description="Enrichment result — same shape as POST /api/enrich")


class BatchEnrichResponse(BaseModel):
    """Response from POST /api/enrich/batch"""
    results:     List[BatchEnrichItem] = Field(default_factory=list, description="Per-record enrichment results in the same order as the request")
    total:       int                   = Field(...,                  description="Number of records processed")
    duration_sec: float                = Field(...,                  description="Total wall-clock time for all concurrent enrichments")


# =============================================================================
# /api/enrich/online-status models
# =============================================================================

class OnlineStatusRequest(BaseModel):
    """Input signals for NetSuite online_sales_status computation."""
    found_url:       Optional[str] = Field(default=None, max_length=500, description="Website URL found by enrichment (null / blank = no website)")
    sells_twisted_x: Optional[str] = Field(default=None, max_length=10,  description="Product check result: 'yes' | 'no' | null")
    sells_anything:  Optional[str] = Field(default=None, max_length=10,  description="Product check result: 'yes' | 'no' | null")
    sells_shoes:     Optional[str] = Field(default=None, max_length=10,  description="Product check result: 'yes' | 'no' | null")


class OnlineStatusResponse(BaseModel):
    """Response from POST /api/enrich/online-status"""
    online_sales_status: str = Field(..., description=(
        "NetSuite dropdown value: "
        "'No Website' | 'Ecommerce Site : Sells Twisted X' | "
        "'Ecommerce Site : Opportunity' | 'Ecommerce Site : Does Not Sell Twisted X' | "
        "'No Ecommerce' | '' (insufficient data)"
    ))


# =============================================================================
# /api/enrich/address-validate models
# =============================================================================

class AddressValidateRequest(BaseModel):
    """Physical address to validate via Google Address Validation API."""
    address:  str = Field(..., min_length=1, max_length=200, description="Street address line")
    city:     str = Field(..., min_length=1, max_length=100, description="City name")
    state:    str = Field(..., pattern=r'^[A-Za-z]{2}$',    description="2-letter US state code")
    zip_code: str = Field(..., pattern=r'^\d{5}(-\d{4})?$', description="5-digit ZIP or ZIP+4")


class AddressValidateResponse(BaseModel):
    """Response from POST /api/enrich/address-validate"""
    geocoded:          bool           = Field(...,        description="True when Google resolved the address to coordinates")
    latitude:          Optional[float] = Field(default=None, description="Geocoded latitude — null when address could not be resolved")
    longitude:         Optional[float] = Field(default=None, description="Geocoded longitude — null when address could not be resolved")
    formatted_address: str            = Field(default="", description="Standardised address string returned by Google")
    place_id_present:  bool           = Field(...,        description="True when Google returned an address-level place_id (Ej... format)")
    is_business:       bool           = Field(...,        description="Google's is_business metadata flag — informational only, not used for routing")
    error:             Optional[str]  = Field(default=None, description="Error code if the API call failed: timeout | quota | upstream_5xx | parse_error")


# =============================================================================
# /api/enrich/classify-retail models
# =============================================================================

class ClassifyRetailRequest(BaseModel):
    """Inputs for retail type classification."""
    primary_type:      Optional[str] = Field(default=None,  max_length=100, description="Google Places primaryType value (e.g. 'shoe_store')")
    has_opening_hours: bool          = Field(default=False,                 description="True when the Places record has regularOpeningHours")
    is_channel_row:    bool          = Field(default=False,                 description="True when the company name indicates an ecommerce/online channel row")


class ClassifyRetailResponse(BaseModel):
    """Response from POST /api/enrich/classify-retail"""
    retail_type: str = Field(..., description="'retail' | 'not_retail' | 'unknown'")
