"""
Pydantic models for Twisted X Scraper API (Celigo rearchitecture)
"""
from typing import List, Dict, Optional, Any
from datetime import datetime
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
