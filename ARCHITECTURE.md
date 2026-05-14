# Twisted X Scraper — Complete Architecture & Developer Guide

---

## Table of Contents

1. [What This System Does](#1-what-this-system-does)
2. [Architecture Overview](#2-architecture-overview)
3. [Quick Start](#3-quick-start)
4. [File-by-File Reference](#4-file-by-file-reference)
   - [Entry Points](#41-entry-points)
   - [API Layer](#42-api-layer)
   - [checker/ — Retailer Detection Package](#43-checker--retailer-detection-package)
   - [enrichment/ — Customer Enrichment Package](#44-enrichment--customer-enrichment-package)
   - [Supporting Modules](#45-supporting-modules)
   - [celigo/ — Celigo Integration Assets](#46-celigo--celigo-integration-assets)
   - [config/ — Shared Configuration Files](#47-config--shared-configuration-files)
   - [data/ — Reference Data](#48-data--reference-data)
   - [Utility Scripts](#49-utility-scripts)
   - [scripts/ and tests/](#410-scripts-and-tests)
5. [Data Flows](#5-data-flows)
   - [Flow A: Celigo Retailer Sync (API)](#flow-a-celigo-retailer-sync-api)
   - [Flow B: Customer Enrichment Pipeline (CSV)](#flow-b-customer-enrichment-pipeline-csv)
   - [Flow C: URL Recovery (suggest_urls)](#flow-c-url-recovery-suggest_urls)
6. [Configuration Reference](#6-configuration-reference)
7. [Key Design Decisions](#7-key-design-decisions)
8. [Tech Stack](#8-tech-stack)
9. [Known Limitations & Roadmap](#9-known-limitations--roadmap)

---

## 1. What This System Does

Twisted X Global Brands maintains approximately 1,800 retail accounts in NetSuite. This system answers two business questions automatically:

**Question 1 (Retailer Detection):** Does this retailer's website actually sell Twisted X products online?
- Input: a retailer URL (e.g. `https://www.bootbarn.com`)
- Output: `yes/no`, confidence level, sample SKUs found, whether they sell online vs in-store only

**Question 2 (Customer Enrichment):** For each NetSuite customer account, what is their correct website, phone number, address, and retail classification?
- Input: NetSuite CSV export of customer records
- Output: Enriched CSV with verified URL, Google Places data, address match confidence, NetSuite `online_sales_status` dropdown value

Results from both flows feed back into NetSuite via Celigo (a cloud iPaaS the IT team already operates).

---

## 2. Architecture Overview

```
+------------------------------------------------------------------+
|                       Celigo Cloud Platform                       |
|                                                                    |
|   Flow A — Retailer Detection (scheduled):                        |
|     1. Pull retailer URLs from NetSuite                           |
|     2. POST /api/check   -> yes/no + SKUs                         |
|     3. POST /api/scrape  -> raw DOM product blocks                 |
|     4. LLM extraction via Anthropic connector (Claude)             |
|     5. POST /api/verify  -> anti-hallucination check              |
|     6. Write verified products back to NetSuite                    |
|                                                                    |
|   Flow B — Customer Enrichment (scheduled):                       |
|     1. Pull customer records from NetSuite                        |
|     2. Celigo date/source filter -> skip fresh records            |
|     3. POST /api/enrich/url-ping  -> flag dead/missing URLs       |
|     4. POST /api/enrich/batch     -> enrich stale records         |
|     5. POST /api/enrich/classify-retail -> retail type            |
|     6. POST /api/check (optional) -> product signals              |
|     7. Celigo native mapping -> compute online_sales_status       |
|     8. Write enriched fields back to NetSuite                     |
+-----------------------------+------------------------------------+
                              | Celigo Gateway (secure tunnel)
                              v
+------------------------------------------------------------------+
|              Scraper API  --  api_server.py  (FastAPI)            |
|                                                                    |
|   POST /api/check              ->  checker/  package              |
|   POST /api/scrape             ->  cleaning.py                    |
|   POST /api/verify             ->  verifier.py                    |
|                                                                    |
|   POST /api/enrich             ->  enrichment._enrich_single      |
|   POST /api/enrich/batch       ->  enrichment._enrich_single (N)  |
|   POST /api/enrich/url-ping    ->  enrichment._url.bulk_check_urls|
|   POST /api/enrich/address-validate -> enrichment._address_validation|
|   POST /api/enrich/classify-retail -> enrichment._retail          |
+-----------------------------+------------------------------------+
                              |
                  +-----------+-------------+
                  |                         |
                  v                         v
       +--------------------+    +-------------------------+
       |  Playwright        |    |  Google APIs            |
       |  (real browser)    |    |  - Address Validation   |
       |  headless Chromium |    |  - Places (New)         |
       +--------------------+    |  - Places Text Search   |
                                 +-------------------------+

The enrichment/ package is shared by both the Celigo API flow and the
fallback CLI script (url_enrichment_pipeline.py / POST /api/enrich/pipeline):

+------------------------------------------------------------------+
|   enrichment/ package                                             |
|         +-- _enrich_single.py  per-row: Address Validation →     |
|         |                      location-biased search → fallback  |
|         +-- _address_validation.py  Google Address Validation API |
|         +-- _url.py       async URL pinging (aiohttp)             |
|         +-- _places.py    Google Places Text Search               |
|         +-- _address.py   address normalisation + match           |
|         +-- _company.py   company key dedup + branch logic         |
|         +-- _retail.py    retail type classification               |
|         +-- _product.py   product signal -> NetSuite status        |
+------------------------------------------------------------------+
```

**One sentence summary of each layer:**

| Layer | File(s) | What it does |
|-------|---------|-------------|
| API server | `api_server.py` | FastAPI app — retailer detection + scrape/verify + 5 enrichment endpoints |
| Retailer detection | `checker/` | 4-layer check: HTTP → sitemap → SerpApi Google Search → Playwright browser |
| DOM extraction | `cleaning.py` | Strips noise from rendered pages, returns product card blocks |
| Anti-hallucination | `verifier.py` | Cross-checks LLM output against raw DOM blocks |
| URL validation | `url_validator/` | Playwright deep-dive: sells TX? sells online? sells footwear? |
| Single-record enrichment | `enrichment/_enrich_single.py` | Address Validation → location-biased Places search → Text Search fallback |
| Enrichment API wrappers | `enrichment/_address_validation.py` | Google Address Validation API + location-biased Text Search wrappers |
| Enrichment pipeline | `enrichment/` | Per-row enrichment logic — called by both the Celigo API endpoints and the fallback CLI script |
| Utilities | `suggest_urls_for_bad_rows.py`, `batch_check_excel.py`, `fill_phones.py` | One-off data repair scripts |

---

## 3. Quick Start

```bash
# 1. Clone and create virtual environment
python3 -m venv venv && source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt
playwright install chromium

# 3. Configure secrets
cp .env.example .env
# Edit .env -- at minimum, set GOOGLE_PLACES_API_KEY

# 4. Start the API server
uvicorn api_server:app --reload --port 8000

# 5. Smoke test
curl -X POST http://localhost:8000/api/check \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://www.atwoods.com/"}'

# Enrichment endpoints require the API key header:
curl -X POST http://localhost:8000/api/enrich \
  -H 'X-API-Key: your_enrich_api_key' \
  -H 'Content-Type: application/json' \
  -d '{"company": "Boot Barn", "address": "123 Main St", "city": "Scottsdale", "state": "AZ", "zip_code": "85260"}'
```

### What you need in `.env` for each flow

| Flow | Required variables |
|------|--------------------|
| Retailer detection (`/api/check`, `/api/scrape`, `/api/verify`) | None mandatory — server starts without any keys. Set `SERPAPI_KEY` to enable Layer 3. |
| Enrichment API (`/api/enrich/*`) | `GOOGLE_PLACES_API_KEY`, `ENRICH_API_KEY` |
| Fallback CLI pipeline (`url_enrichment_pipeline.py`) | `GOOGLE_PLACES_API_KEY`, `INPUT_FILE`, `OUTPUT_FILE` |
| SFTP mode (`USE_SFTP=true`) | All `SFTP_*` variables |

---

## 4. File-by-File Reference

### 4.1 Entry Points

#### `url_enrichment_pipeline.py` (5 lines)
The command-line entry point for the customer enrichment pipeline.

```python
from enrichment import run_pipeline
if __name__ == "__main__":
    run_pipeline()
```

All logic lives in the `enrichment/` package. This file exists so operators can run
`python3 url_enrichment_pipeline.py` without knowing the internal package structure.

---

#### `api_server.py`
FastAPI application entry point. Implements eleven endpoints across two groups.

**Auth:** the eight `/api/enrich/*` endpoints require an `X-API-Key` header matching the
`ENRICH_API_KEY` env var. The three retailer-detection endpoints need no auth.

---

**`POST /api/check`**
Quick yes/no: does this retailer URL sell Twisted X products?

```
Request:  {"url": "https://www.atwoods.com/"}

Response: {
  "url": "https://www.atwoods.com/",
  "sells_twisted_x": true,
  "sells_online": true,
  "sells_footwear": true,
  "confidence": "high",
  "store_type": "ecommerce",
  "proof": ["SKU MCA0070 found in page text"],
  "sample_products": [{"name": "Twisted X Men's...", "sku": "MCA0070", ...}],
  "blocked": false,
  "error": null
}
```

Delegates to `checker.run_check()`. Returns immediately if Layer 1 (HTTP) or Layer 2 (sitemap)
finds a definitive answer; only launches a browser for Layer 3.

**`POST /api/scrape`**
Full product block extraction. Used by Celigo to get raw DOM content for LLM extraction.

```
Request:  {"url": "https://www.atwoods.com/", "search_term": "Twisted X", "max_pages": 15}

Response: {
  "url": "...",
  "products": [
    {
      "text": "Twisted X Men's CellStretch...",
      "html_snippet": "<div class='product-card'>...</div>",
      "images": [{"src": "...", "alt": "..."}],
      "links": [{"href": "...", "text": "..."}]
    }
  ]
}
```

Scrape pipeline inside `_scrape_url_sync`:
1. Run `url_validator.check_url()` to detect store type and confirm TX presence.
2. Call `_navigate_to_best_tx_page()` — after validation the browser may be on a footwear
   detection page (`/boots`). This helper tries common brand/collection URL patterns
   (e.g. `/brands/twisted-x/`, `/collections/twisted-x`) and navigates to whichever
   page has the most Add-to-Cart buttons, ensuring extraction starts from the fullest catalog.
3. For up to `max_pages` pages (default **15**): scroll the full page to trigger lazy loading,
   extract product blocks with `cleaning.clean_and_extract()`, then click the next-page control.
   Pagination is verified by checking that URL or visible page content actually changed before
   advancing — a click with no content change stops the loop.

The LLM never runs here — Celigo sends these blocks to Anthropic.

**`POST /api/verify`**
Anti-hallucination check. Cross-checks LLM-extracted product fields against the raw
`ProductBlock` data returned by `/api/scrape`.

```
Request: {
  "extracted_products": [{"name": "...", "sku": "MCA0070", "price": "$149.99", ...}],
  "original_products": [{ /* ProductBlock from /api/scrape */ }]
}

Response: {
  "verified_products": [...],
  "flagged_products": [{"product": {...}, "issues": ["SKU not found in source"]}],
  "verification_stats": {"total_input": 5, "verified": 4, "flagged": 1}
}
```

Uses `verifier.py`. Pure deterministic logic — no LLM calls here.

**Other endpoints:**
- `GET /` — root info (project name, version, endpoint list)
- `GET /health` — returns `{"status": "healthy", "timestamp": "..."}`
- `GET /api/test` — smoke test; returns `{"message": "API is working"}`

---

**`POST /api/enrich`** _(requires `X-API-Key`)_
Enrich a single NetSuite customer record.

```
Request: {
  "company":     "Boot Barn",
  "address":     "15776 N Greenway Hayden Loop",
  "city":        "Scottsdale",
  "state":       "AZ",
  "zip_code":    "85260",
  "current_url": "https://www.bootbarn.com",    // optional — informational only
  "internal_id": "NS-12345"                      // optional — echoed back, never logged
}

Response: {
  "found_url":                    "https://www.bootbarn.com",
  "found_maps_url":               "https://maps.google.com/?cid=...",
  "matched_name":                 "Boot Barn",
  "places_place_id":              "ChIJ...",
  "places_formatted_address":     "15776 N Greenway Hayden Loop, Scottsdale, AZ 85260",
  "places_national_phone":        "(480) 951-2969",
  "places_rating":                4.4,
  "places_regular_opening_hours": "Monday: 9AM-9PM; ...",
  "places_latitude":              33.6231,
  "places_longitude":             -111.9086,
  "places_business_status":       "OPERATIONAL",
  "places_primary_type":          "clothing_store",
  "match_confidence":             "high",
  "enrichment_source":            "address_validation",
  "address_match":                true
}
```

**Lookup flow:** Google Address Validation API → location-biased Places Text Search (within 300m)
→ plain Text Search fallback. `enrichment_source` values:
- `address_validation` — primary path succeeded
- `text_search` — primary failed/skipped; text search found a candidate
- `not_found` — all paths completed, no candidate found
- `enrichment_error` — technical error (timeout/5xx) AND no candidate found

Google failures return HTTP **200** with `enrichment_source="enrichment_error"` — callers should
retry on HTTP 5xx only, not on 200.

---

**`POST /api/enrich/batch`** _(requires `X-API-Key`)_
Enrich up to 100 records in one call. Records are processed **concurrently** via a thread pool.

```
Request:  [ EnrichRequest, EnrichRequest, ... ]   // max 100 items
Headers:  X-Idempotency-Key: <uuid>               // optional — safe retries
Response: {
  "results": [
    { "internal_id": "NS-12345", "result": { ...EnrichResponse... } },
    ...
  ],
  "total":            20,
  "duration_sec":     3.42,
  "google_api_calls": 47,
  "quota_errors":     0
}
```

Results are returned in the **same order** as the request. Each item echoes `internal_id` so
Celigo can match without relying on array position. Exceeding 100 items → HTTP 422.

**Idempotency:** pass `X-Idempotency-Key: <uuid>` to make Celigo retries safe. The server caches
the full response for 30 minutes — a retry with the same key returns the cached result without
re-calling Google APIs.

**Quota visibility:** `google_api_calls` is the total Google API calls made across all records
(0-3 per record). `quota_errors` counts records where enrichment failed due to quota limits.

---

**`POST /api/enrich/url-ping`** _(requires `X-API-Key`)_
Concurrently ping URLs and bucket them as alive / dead / missing.

```
Request:  [ { "internal_id": "NS-1", "url": "https://www.bootbarn.com" }, ... ]
Response: {
  "alive":   ["NS-1"],
  "dead":    ["NS-2"],
  "missing": ["NS-3"],
  "details": [
    { "internal_id": "NS-1", "status": "active",   "http_code": 200, "final_url": "https://..." },
    { "internal_id": "NS-2", "status": "dead",     "http_code": 404, "final_url": null },
    { "internal_id": "NS-3", "status": "missing",  "http_code": null, "final_url": null }
  ]
}
```

Status → bucket: `active`/`redirected`/`blocked` → **alive**; `dead` → **dead**; `missing` → **missing**.
`blocked` (401/403/429/503) is grouped into alive — the server is reachable, URL is valid.

---

**`POST /api/enrich/address-validate`** _(requires `X-API-Key`)_
Debug tool: call Google Address Validation API for a single address (one API call only, no Places lookup).

```
Request:  { "address": "15776 N Greenway Hayden Loop", "city": "Scottsdale", "state": "AZ", "zip_code": "85260" }
Response: {
  "geocoded":          true,
  "latitude":          33.6231,
  "longitude":         -111.9086,
  "formatted_address": "15776 N Greenway Hayden Loop, Scottsdale, AZ 85260, USA",
  "place_id_present":  true,
  "is_business":       true,
  "error":             null
}
```

Diagnostic interpretation: `geocoded=false` → address too vague; `geocoded=true, place_id_present=false`
→ coordinates found but no business listing; both `true` → enrichment should work.

---

**`POST /api/enrich/classify-retail`** _(requires `X-API-Key`)_
Classify a business as `retail` / `not_retail` / `unknown`. Pure logic — no API calls.

```
Request:  { "primary_type": "shoe_store", "has_opening_hours": true, "is_channel_row": false }
Response: { "retail_type": "retail" }
```

`is_channel_row=true` (ecom suffix in name) → `not_retail`. Warehouse/storage/distribution
types → `not_retail`. Known store types (shoe_store, clothing_store, …) or has opening hours
with unrecognised type → `retail`. Otherwise → `unknown`.

---

### 4.2 API Layer

#### `models.py`
Pydantic request/response models for all API endpoints. If Celigo or another caller
expects a field, it is defined here.

Retailer detection models:
- `CheckRequest` / `CheckResponse` — for `/api/check`
- `ScrapeRequestNew` / `ScrapeResponse` / `ProductBlock` — for `/api/scrape`
  (`max_pages` defaults to **15**; `search_term` defaults to `"Twisted X"`)
- `VerifyRequest` / `VerifyResponse` — for `/api/verify`

**Important:** `ProductBlock` is both returned by `/api/scrape` and consumed as input by
`/api/verify`. The structure must match exactly for the round-trip to work.

Enrichment models:
- `EnrichRequest` / `EnrichResponse` — for `/api/enrich` (single record); `EnrichRequest` is
  also the element type for `/api/enrich/batch`. `EnrichResponse` includes `google_api_calls` (0-3).
- `UrlPingItem` / `UrlPingDetail` / `UrlPingResponse` — for `/api/enrich/url-ping`
- `BatchEnrichItem` / `BatchEnrichResponse` — for `/api/enrich/batch`.
  `BatchEnrichResponse` includes `google_api_calls` (total across batch) and `quota_errors` (count).
- `AddressValidateRequest` / `AddressValidateResponse` — for `/api/enrich/address-validate`
- `ClassifyRetailRequest` / `ClassifyRetailResponse` — for `/api/enrich/classify-retail`

---

#### `config.py` (~150 lines)
Module-level configuration loaded at import time.

Loads:
- **SKU database** from `data/twisted_x_skus_v107.xlsx` (primary) and `data/twisted_x_sku.csv`
  (fallback) — raises `RuntimeError` on startup if neither file loads or if fewer than 1,000
  style codes are found. This is intentional: a missing SKU database would silently return wrong results.
- **`TX_STYLE_CODES`** — a Python `set` of all 3,000+ Twisted X style code tokens
  (e.g. `{"MCA0070", "ICA0035", ...}`)
- **Playwright settings** — `HEADLESS`, `TIMEOUT_MS`
- **`get_retailer_name(url)`** — helper that extracts a human-readable name from a URL

**Do not import secrets from here.** Environment variables / secrets are loaded by
`enrichment/_config.py` for the pipeline and read directly from `os.environ` in `api_server.py`.

---

#### `brand_config.py` (26 lines)
Single source of truth for all Twisted X brand and product-line keywords.

Reads `config/brand_indicators.json` at import time and exports:
- `BRANDS` — `["twisted x", "twistedx", "black star", "cellsole", "hooey", ...]`
- `PRODUCT_LINES` — `["tech x", "feather x", "cellstretch", ...]`
- `ALL_INDICATORS` — combined + pre-lowercased list used for text matching
- `PRIMARY_BRAND_PAIR` — `["twisted x", "twistedx"]` — minimum required for a definitive brand match

**Adding a new sub-brand:** edit `config/brand_indicators.json` only. Every consumer
(`verifier.py`, `url_validator/`, `api_server.py`, `checker/_scanners.py`) imports from
`brand_config` and picks up the change on next restart.

---

### 4.3 `checker/` — Retailer Detection Package

The core detection engine. Called by `api_server.py` for every `/api/check` request.

**Entry point:** `from checker import run_check; result = run_check(url)`

#### Detection strategy — four layers in order

```
URL
 |
 +-> Layer 1: HTTP-first (_http.py)
 |     Plain HTTP GET, Chrome user-agent.
 |     Scans response HTML for TX SKU codes.
 |     sells_online derived from whether found products have a price or product URL.
 |     If SKU found -> definitive YES, stop here.
 |     Cost: ~1 HTTP request, no browser.
 |
 +-> Layer 2: Sitemap (_sitemap.py)
 |     Fetches robots.txt -> follows Sitemap: directives.
 |     Falls back to /sitemap.xml -> /sitemap_index.xml.
 |     Prioritises product/brand/category child sitemaps; skips blog/news/video.
 |     Scans all <loc> URLs for TX slugs (twisted-x, twistedx, tx-boots, tx-footwear...).
 |     No URL count cap — scans everything in fetched sitemaps (string matching is free).
 |     If TX slug found -> definitive YES, stop here.
 |     Cost: 2-10 HTTP requests, no browser.
 |
 +-> Layer 3: SerpApi (_serp.py)
 |     Searches Google for "Twisted X site:<domain>" via SerpApi JSON API.
 |     Bypasses Cloudflare / PerimeterX — Google has already indexed the retailer.
 |     No results is NOT definitive NO (small sites may not be indexed).
 |     If Google results found -> definitive YES, stop here.
 |     Cost: 1 SerpApi call, no browser.
 |
 +-> Layer 4: Playwright (_playwright.py)
       Launches real Chromium browser.
       Detects platform (Shopify / WooCommerce / NetSuite / generic).
       Runs platform-appropriate search strategy.
       Scans search results for SKUs and brand names.
       Cost: 15-60 seconds, full browser.
```

Layers 1-3 are cheap. Layer 4 is expensive. The design short-circuits so Playwright only
runs when all three cheap layers are inconclusive.

#### Module-by-module reference

**`checker/__init__.py`**
Wires the four layers together in `run_check(url)`. Handles URL normalisation, merges
sitemap/SerpApi context notes into Layer 4 results when Layer 4 found nothing on its own.

---

**`checker/_types.py`**
TypedDict definitions + result factory functions. Every function in the package returns one
of these typed dicts so callers never have to guess which keys exist.

Key types:
- `SampleProduct` — `{name, price, sku, image, product_url}`
- `ScanResult` — `{matched_codes: set, matched_in: list, sample_products: list}`
- `LayerResult` — returned by Layer 1 and Layer 2; includes `definitive`, `sells_twisted_x`, `confidence`, `proof`
- `SearchOutcome` — returned by `_search.py`; includes `found`, `method`, `proof`, `sample_products`

Factory functions:
- `new_check_result(url, retailer, error=None)` — builds a blank result dict with all required keys
- `empty_scan()` — blank `ScanResult`
- `empty_search()` — blank `SearchOutcome`

---

**`checker/_scanners.py`**
The two core detection primitives used by all three layers.

`scan_html_for_skus(html: str) -> ScanResult`
- Takes raw HTML (string)
- Tokenises the HTML into uppercase/alphanumeric tokens (e.g. `MCA0070`)
- Cross-references against `config.TX_STYLE_CODES` (set of 3,000+ codes)
- Returns matched style codes + provenance strings (e.g. `"MCA0070 in page text"`)
- Used by Layer 1 (HTTP) and Layer 2 (sitemap)

`scan_page_for_skus(page) -> ScanResult`
- Takes a live Playwright Page
- Calls `scan_html_for_skus` on the rendered page content
- Also extracts sample product data (name, price, image, URL) from matching elements
- Used by Layer 4 (Playwright)

`find_brand_in_product_context(page) -> str | None`
- Runs JavaScript inside the browser to scan product link/card text for brand names
- Returns the matched brand term, or None
- Fallback for sites where SKU fingerprinting finds no codes but brand name is visible in product cards

---

**`checker/_platform.py`**
Platform detection and bot-block detection.

`detect_platform(page, url) -> "shopify" | "woocommerce" | "netsuite" | "normal"`
- Reads page HTML for platform fingerprints (WP-JSON, `Shopify.shop`, SuiteCommerce markers)
- NetSuite is checked before WooCommerce because some NS stores also include WordPress elements

`detect_blocked(page) -> (bool, list[str])`
- Checks for Cloudflare "checking your browser", PerimeterX, DDoS-Guard challenge pages
- Returns `(is_blocked, list_of_reasons)`
- Called before any search attempt; a blocked page short-circuits to `blocked: true`

---

**`checker/_search.py`**
Platform-aware search strategies. Each function receives an already-open Playwright page
and navigates it to the best search URL for that platform.

`search_netsuite(page, base_url) -> SearchOutcome`
- Navigates to `/catalog/productsearch` (NetSuite's product catalog endpoint)
- Scans for TX SKUs and brand names

`search_shopify_or_woo(page, platform, base_url, url) -> SearchOutcome`
- Shopify: navigates to `/search?type=product&q=Twisted+X`
- WooCommerce: navigates to `/?s=Twisted+X&post_type=product`

`search_generic(page, base_url, url) -> SearchOutcome`
- Tries a ranked list of common search URL patterns
- Falls back to interacting with the site's search input field via `url_validator._search_on_site()`
- Used for unknown/custom platforms

All three scanners call `scan_page_for_skus` and `find_brand_in_product_context` on the results page.

---

**`checker/_http.py`**
Layer 1 implementation.

`http_first_check(url) -> LayerResult`
- Makes a plain `requests.get()` with Chrome UA and a short timeout
- Runs `scan_html_for_skus` on the response HTML
- Returns `definitive=True` only on a SKU match (brand name alone risks false positives)
- On connection error, returns `definitive=False` so Layer 2/3 can try

---

**`checker/_sitemap.py`**
Layer 2 implementation.

`sitemap_check(url) -> LayerResult`
- Fetches `robots.txt` and extracts `Sitemap:` directive URLs
- Falls back to `/sitemap.xml` and `/sitemap_index.xml`
- Handles gzipped sitemaps — detects via filename `.xml.gz` **and** `Content-Encoding: gzip` header
- Prioritises child sitemaps with `product/brand/categor/collection/catalog` in the name
- Skips child sitemaps with `blog/news/post/video/media` in the name (never contain TX products)
- Fetches up to 10 child sitemaps (in priority order); no URL count cap — scanning is free
- Scans all `<loc>` URLs with regex covering `twisted-x`, `twistedx`, `tx-boots`, `tx-footwear`, `tx-work`, `tx-western` etc.
- `sells_online` derived from whether the found URL contains product/shop/brand path segments
- A URL slug like `/brands/twisted-x/` is a definitive positive
- Absence is NOT treated as a negative (products may simply not be in the sitemap)

---

**`checker/_serp.py`**
Layer 3 implementation. Requires `SERPAPI_KEY` in `.env`.

`serp_check(url) -> dict`
- Searches Google for `"Twisted X site:<domain>"` via SerpApi JSON API
- Returns `definitive=True` only when Google results are found
- No results is NOT a definitive NO — falls through to Playwright
- Gracefully disabled (returns `definitive=False`) when `SERPAPI_KEY` is blank
- Key advantage: Google has already crawled bot-protected sites (Boot Barn, Cavenders) — Layer 3
  catches these without needing a browser or residential proxy

---

**`checker/_playwright.py`**
Layer 4 implementation. The only module in the package that opens a browser.

`playwright_check(url, normalized, retailer_name) -> dict`

Steps:
1. Launch headless Chromium with `playwright_stealth` to reduce bot detection
2. Navigate to the URL, wait for DOM content loaded
3. `detect_blocked(page)` — if challenged, return immediately with `blocked: true`
4. `detect_platform(page, url)` — identify Shopify / WooCommerce / NetSuite / normal
5. Run the appropriate search strategy
6. Run `url_validator.check_url()` for `sells_online`, `store_type`, `sells_footwear`
7. Assemble final result dict

---

### 4.4 `enrichment/` — Customer Enrichment Package

Bulk CSV enrichment pipeline. Called by `url_enrichment_pipeline.py`.

**Entry points:**
- `from enrichment import run_pipeline; run_pipeline()` — full CSV batch pipeline
- `from enrichment import enrich_single_customer; enrich_single_customer(...)` — single record

#### Pipeline steps (in order)

```
1. Resolve paths
   SFTP mode: pull oldest CSV from /inbound
   Local mode: read INPUT_FILE from .env

2. Load DataFrame
   Read CSV or Excel, apply COLUMN_MAP renames,
   strip invisible chars, validate required columns

3. Partition rows
   Skip rows enriched within ENRICHMENT_TTL_DAYS (default 30)
   Re-enrich rows with blank date, enrichment_error, or address_mismatch status

4. URL ping
   Async aiohttp HEAD requests, 50 concurrent
   Classifies each URL: active / redirected / dead / blocked / not_found

5. Per-row enrichment loop (_run_enrich_loop)
   For each stale row: enrich_single_customer(company, address, city, state, zip)
     -> Step A: Google Address Validation API -> lat/lng
     -> Step B: location-biased Text Search (within 300m of coordinates)
     -> Step C: plain Text Search fallback (if A/B failed or PO box)
     -> Step D: address_match_confidence on winning candidate
   Writes: business name, phone, address, lat/lng, place_id, hours,
           enrichment_source, match_confidence, address_match

6. Product check (if ENABLE_PRODUCT_CHECK=true)
   domain_signals() -> fast path for 34 known domains
   POST /api/check -> for unknown domains
   Writes: sells_anything, sells_shoes, sells_twisted_x

7. Compute status
   Maps signals -> NetSuite dropdown value
   e.g. sells_twisted_x=yes -> "Ecommerce Site : Sells Twisted X"

8. Save output
   Writes enriched CSV + companion JSON
   SFTP mode: uploads to /review, archives input to /archive
```

#### Module-by-module reference

**`enrichment/__init__.py`**
Exposes `run_pipeline` and `enrich_single_customer`. Everything else is package-internal.

---

**`enrichment/_enrich_single.py`** _(new)_
Single-record enrichment orchestration. Public entry point: `enrich_single_customer()`.

Lookup flow:
1. **Input normalisation** — strip suite/apt noise, uppercase state, strip ZIP+4, detect PO boxes
2. **Address Validation API** — resolves physical address to lat/lng; PO boxes skip this step
3. **Location-biased Text Search** — searches for company by name within 300m of the geocoded point;
   finds the specific branch at that address rather than any branch of the chain in a different city
4. **Text Search fallback** — `find_places_candidates()` + `pick_branch_candidate_for_row()` if primary path failed
5. **`address_match_confidence()`** — grades the winning candidate: `high` / `medium` / `low` / `none`

`enrichment_source` values: `address_validation` | `text_search` | `not_found` | `enrichment_error`.
Never raises — all errors are captured and reflected in `enrichment_source`.
PII-safe logging: logs city/state/enrichment_source/latency/error; never logs company, street, or internal_id.

---

**`enrichment/_address_validation.py`** _(new)_
HTTP wrappers for two Google APIs used by `_enrich_single`.

`validate_address(address, city, state, zip_code) -> (dict | None, error_code)`
- POST to Google Address Validation API
- Returns `{"place_id", "formatted_address", "is_business", "latitude", "longitude"}` on success
- `is_business` is logged only — never used to choose a lookup path
- Returns `(None, error_code)` on failure: `timeout` | `quota` | `upstream_5xx` | `parse_error`

`find_places_near_location(company, lat, lng, radius_meters=300) -> (list | None, error_code)`
- Location-biased Text Search POST to `PLACES_URL`
- Same `FIELD_MASK` and result shape as the existing text search
- Returns `(candidates_list, "")` on success — list may be empty if no match within radius
- Returns `(None, error_code)` on failure

Both functions retry once on 429/503. All exceptions are swallowed; callers never need `try/except`.

---

**`enrichment/_config.py`**
All environment variables, column mappings, and pipeline constants. Every other enrichment
module imports from here — nothing else hardcodes a configuration value.

Key values:
- `GOOGLE_PLACES_API_KEY` — raises `RuntimeError` at startup if missing
- `SFTP_*` — host, port, user, key path, password, inbound/review/archive dirs
- `USE_SFTP` — boolean, read from `os.environ` at call time (not cached at import)
- `COLUMN_MAP` — maps NetSuite export column names to pipeline-internal names
  (e.g. `"Web Address"` -> `"website url"`)
- `NETSUITE_ID_COL`, `URL_COL`, `ADDRESS_COLS` — canonical column name constants
- `ENRICHMENT_TTL_DAYS` — rows enriched more recently than this are skipped (default 90)
- `CHANNEL_KEYWORDS` — suffixes that identify channel rows (e.g. `"Boot Barn - ecommerce"`)
- `URL_BLACKLIST` — values treated as "no website" (`"n/a"`, `"-"`, etc.)
- `ENABLE_PRODUCT_CHECK` — flag to enable the optional `/api/check` step

---

**`enrichment/_pipeline.py`**
Orchestrator. `run_pipeline()` calls the other modules in the correct order.
Each step is a named function (~10-30 lines) so the flow reads like a checklist.

Internal step functions (not public API):
- `_resolve_paths()` — returns `(input_path, output_path, remote_input_path, tmp_obj)`
- `_init_output_columns(df)` — adds all output columns with NA defaults so column order is stable
- `_partition_by_freshness(df)` — calls `should_enrich()` per row, returns index sets
- `_tag_fresh_rows(df, fresh_idx)` — copies existing enrichment data to fresh rows
- `_run_url_ping(df, enrich_idx)` — returns `(ping_df, already_alive_idx, broken_idx)`
- `_run_enrich_loop(df, ...)` — **new** per-row loop; calls `enrich_single_customer()` for each row;
  supersedes the old per-company `_run_places_loop()` (kept as legacy, no longer called)
- `_maybe_backfill_url_new(df, idx, result)` — backfills `found_url` from enrichment result
  when `enrichment_source` is `address_validation` or `text_search`
- `_run_product_check(df)` — calls `/api/check` per live URL
- `_log_summary(df, output_path)` — prints final stats

---

**`enrichment/_url.py`**
URL health checking, classification, and root-domain extraction.

- `is_url_blank_or_invalid(val) -> bool` — True for None, empty, or blacklisted values
- `bulk_check_urls(urls, concurrency=50) -> dict` — async aiohttp batch pinger
  Status values: `"active"`, `"redirected"`, `"dead"`, `"blocked"`, `"not_found"`
- `classify_url(url) -> str` — returns `"website"`, `"facebook"`, `"instagram"`, `"not_found"`, etc.
- `extract_root_domain(url) -> str` — returns just `"bootbarn.com"` with no subdomains or path

---

**`enrichment/_address.py`**
Address normalisation and matching.

- `normalize_zip(zip_val) -> str` — strips leading zeros, handles 9-digit ZIPs
- `normalize_address_for_match(addr) -> str` — expands abbreviations, strips punctuation for fuzzy compare
- `parse_places_address(formatted_address) -> dict` — parses Google Places address string into components
- `address_matches(row_addr, places_addr, threshold=0.7) -> (bool, float)` — compares NetSuite vs Places
  address, returns `(matches, confidence_score)` where confidence is 0.0-1.0

---

**`enrichment/_places.py`**
Google Places API integration.

- `find_places_candidates(company_name, city, state, zip_code) -> list | None` — calls Places Text Search API
- `find_on_google_places(row) -> dict` — wraps candidates with retry/fallback query variants

Result dict keys: `google_business_name`, `google_place_id`, `places_primary_type`,
`places_formatted_address`, `places_national_phone`, `places_rating`, `places_latitude`,
`places_longitude`, `places_regular_opening_hours`, `places_business_status`

---

**`enrichment/_company.py`**
Company key normalisation and branch/channel row handling.

- `normalize_company_key(row, company_col) -> str` — strips branch suffixes (`"- HQ"`, `"#2"`) for deduplication
- `is_channel_row(raw_company) -> bool` — True if name ends with a channel suffix (`"- ecommerce"`)
  Prevents channel rows from overwriting HQ row's Places data
- `build_branch_norms(df) -> dict` — single-pass O(n) scan, groups all branches of the same company
  Used so Google Places is called once per company, not once per row
- `pick_places_result_for_company(candidates) -> dict` — picks best candidate using address confidence

---

**`enrichment/_retail.py`**
Retail type classification and known-domain lookup.

- `classify_retail_type(row_is_channel, primary_type, has_opening_hours) -> str`
  Maps Google Places `primary_type` -> `"retail"`, `"online_only"`, `"wholesale"`, `"unknown"`

- `KNOWN_DOMAIN_SIGNALS` — dict mapping 34 known retailer domains directly to product signal dicts,
  bypassing the `/api/check` call for high-confidence known accounts

- `domain_signals(url) -> dict | None` — fast path; returns pre-computed signals or None

---

**`enrichment/_product.py`**
Product signal checking and NetSuite status computation.

`check_product_signals(url) -> dict`
- First checks `domain_signals()` (no HTTP call for known domains)
- Falls back to `POST /api/check` on the running API server
- Returns `{sells_anything: "yes"|"no"|"unknown", sells_shoes: ..., sells_twisted_x: ...}`

`compute_online_sales_status(row) -> str`
Maps signals to the NetSuite `online_sales_status` dropdown value:

| Condition | NetSuite value |
|-----------|---------------|
| No website | `"No Website"` |
| `sells_twisted_x = yes` | `"Ecommerce Site : Sells Twisted X"` |
| `sells_anything = yes` and `sells_shoes = yes` | `"Ecommerce Site : Opportunity"` |
| `sells_anything = yes` and `sells_shoes = no` | `"Ecommerce Site : Does Not Sell Twisted X"` |
| `sells_anything = no` | `"No Ecommerce"` |
| insufficient data | `""` (blank — Celigo does not overwrite) |

---

**`enrichment/_io.py`**
File I/O and SFTP helpers.

- `sftp_session()` — context manager that opens/closes a Paramiko SFTP connection
- `resolve_input_file(sftp) -> str` — finds oldest CSV in SFTP inbound dir (FIFO processing)
- `derive_output_filename(remote_path) -> str` — derives `customers_20260401_Enriched.csv` from input
- `load_dataframe(path) -> (df, is_csv)` — loads CSV or Excel, applies COLUMN_MAP, strips invisible chars
- `save_output(df, output_path, is_csv) -> json_path` — saves enriched CSV + companion JSON
- `upload_results(output_path, json_path, remote_input_path)` — uploads to SFTP, archives input
- `should_enrich(row) -> bool` — decides whether a row needs re-enrichment this run

---

### 4.5 Supporting Modules

#### `url_validator/` — Playwright Deep Validator Package

The Playwright-based deep URL validator. Used by `checker/_playwright.py` and as a
standalone batch runner (`python -m url_validator [input.csv [output.csv]]`).

**Do not call this directly from new code.** Use `checker.run_check()` for detection and
`enrichment/_url.py` for URL pinging. `url_validator` is the implementation those packages
delegate to for Playwright-based checks.

All public symbols are re-exported from `url_validator/__init__.py` so existing import
patterns (`from url_validator import check_url`, `import url_validator; url_validator._search_on_site(...)`)
continue to work unchanged.

**Sub-modules:**

`url_validator/_constants.py`
- Pure data — no Playwright imports. All selector lists and configuration values in one place.
- Key constants: `TIMEOUT_MS=20000`, `VALIDATION_TIMEOUT=18000`, `SEARCH_GROWTH_RATIO=1.2`, `_RATE_LIMIT_S=0.5`
- Key lists: `_POPUP_CLOSE_SELECTORS`, `_SEARCH_INPUT_SELECTORS`, `_PURCHASE_BUTTON_SELECTORS`,
  `_CART_SELECTORS`, `_ONLINE_BLOCKER_PHRASES`, `_PHYSICAL_STORE_PHRASES`, `_NO_RESULTS_PHRASES`,
  `_PRODUCT_TITLE_SELECTORS`, `_URL_COLUMN_CANDIDATES`

`url_validator/_brand.py`
- `normalize_url(url) -> str | None` — fixes malformed URLs (`http://http:/`, `http://ww.`,
  missing protocol, leading quotes). Returns None for clearly invalid inputs.
- `_check_brand_in_content(text, html) -> bool` — checks brand indicators from `brand_config`
- `_check_product_links(page) -> bool` — scans product link/card text for brand names
- `_is_netsuite_site(url) -> bool` — detects NetSuite SuiteCommerce stores
- `_classify_brand_site(final_url, page_text, original_url) -> (bool, bool)` — returns
  `(is_official_brand, is_brand_site)` to handle twistedx.com redirect overrides

`url_validator/_browser.py`
- `_close_popups(page)` — dismisses cookie banners and modal overlays
- `_search_on_site(page, search_term) -> bool` — finds the search bar (40+ CSS selectors
  covering WooCommerce, Shopify, BigCommerce, generic themes); falls back to URL-based search
- `_try_category_pages(page, url) -> bool` — navigates `/boots`, `/footwear`, `/shoes` as fallback
- `_try_fill_search_input(page, search_term) -> bool` — extracted helper shared by two search paths

`url_validator/_detect.py`
- `detect_twisted_x(page, url) -> dict`
  - Method 1: Homepage text + HTML scan
  - Method 2: Product link scan
  - Method 3: Site search (`"Twisted X"` and `"twistedx"` variants, with lazy-load scrolling)
  - Method 4: Category page navigation
- `detect_online_sales_capability(page) -> dict`
  - Looks for functional purchase buttons (Add to Cart, Buy Now), not just text mentions
  - Checks for shopping cart link/icon
  - Strong blockers: `"in-store only"`, `"no online ordering"`, `"call for availability"`
  - Returns `{sells_online, confidence, indicators, blockers}`
- `detect_footwear(page, base_url) -> dict`
  - Homepage scan → category page navigation → site search for `"boots"`

`url_validator/_check.py`
- `check_url(url, page, retries=2) -> dict`
  - Orchestrates `detect_twisted_x` + `detect_online_sales_capability` + `detect_footwear`
  - Tracks redirects; applies brand-site overrides so twistedx.com itself is not credited as
    an online retailer unless it shows real product listings with working purchase buttons
  - Returns full verdict: `has_twisted_x`, `sells_online`, `sells_footwear`, `combined_status`,
    `final_url`, `redirected`, `error`
  - `combined_status` values: `has_products_sells_online`, `has_products_in_store_only`,
    `ecommerce_no_twisted_x`, `no_products_no_online`, `error`

`url_validator/_batch.py`
- `validate_urls(input_csv, output_csv) -> dict`
  - Batch runner — reads any CSV with a recognised URL column, runs `check_url` on each
  - Supported column names: `"website url"`, `"Web Address"`, `"url"`, `"URL"`, `"Website"`
  - Creates a fresh browser page per URL to prevent cross-site cookie/auth bleed
  - Rate-limits at 0.5 s between URLs to avoid WAF IP blocks
  - Writes two output files: full results CSV + `*_filtered_online_only.csv`

`url_validator/__main__.py`
- CLI entry point: `python -m url_validator [input.csv [output.csv]]`
- Defaults to `data/CustomCustomerSearchResults990.csv` if no argument given

---

#### `cleaning.py` (~420 lines)
DOM cleanup and product block extraction for `/api/scrape`.

Before extraction, `clean_and_extract` scrolls the full page in steps so that
lazy-loaded product cards are rendered into the DOM. This is critical for sites
like atwoods.com where only the visible viewport is initially populated.

Three extraction strategies, tried in priority order:

1. **Targeted** — CSS selectors for product cards. Returns one `ProductBlock` per card.
   Deduplicates by first 80 chars of card text.
2. **Segmented** — Splits full visible text on price/action boundary patterns (`$`, `Add to Cart`).
   Uses a 7-line lookback from each boundary to capture brand + name + price + action.
3. **Fullpage** — Last resort. Returns entire page text as a single block (truncated).

Payload limits applied to all strategies:
- Max **300** products per scrape call
- Max 500 chars text per block
- Max 500 chars HTML per block
- Max 2 images per block
- Max 3 links per block

---

#### `verifier.py` (~250 lines)
Anti-hallucination verification for LLM-extracted products.

Checks each extracted product field (name, SKU, price, URL) against the original
`ProductBlock[]` from `/api/scrape`. Uses fuzzy string matching (`difflib.SequenceMatcher`,
threshold 0.3) for name fields.

Confidence scoring:
- Brand name present in source: **+3 points**
- SKU matches `TX_STYLE_CODES`: **+2 points**
- Price found in source: **+1 point**
- >= 6 points -> `"high"` | >= 3 points -> `"medium"` | < 3 points -> `"low"` (flagged)

---

#### `sftp_connect.py` (~85 lines)
Paramiko SFTP helper with multi-auth fallback.

Auth order:
1. Public-key (`Ed25519Key`, `RSAKey`, `ECDSAKey` tried in sequence)
2. Password via `auth_password`
3. Keyboard-interactive (required by SFTPGo — standard password auth fails on some configs)

---

### 4.6 `celigo/` — Celigo Integration Assets

> **Note for code reviewers:** `celigo/` is listed in `.gitignore` and is **not tracked in this
> repository.** The files are deployed directly to Celigo's platform. They are documented here
> for context only — you will not find them on disk.

The `celigo/` directory (when present locally) holds:

| File | What it is | Who uses it |
|------|-----------|------------|
| `extraction_system_prompt.txt` | System prompt for Claude product extraction | Celigo Anthropic connector |
| `extraction_user_prompt_template.txt` | Per-request user prompt (`{{product_blocks}}` placeholder) | Celigo Anthropic connector |
| `classification_rules.json` | Gender / product type / work feature classification rules | Celigo (post-verify step, **not loaded by this Python code**) |
| `API_CONTRACT.md` | Endpoint contract for Celigo engineers | Celigo IT team |

**Classification is Celigo's responsibility.** The Python `/api/verify` endpoint returns
`verified_products`, `flagged_products`, and `verification_stats` only. It does **not** add
`gender`, `product_type`, `sub_brand`, or `work_features` — those fields are appended by
Celigo after verification using its own classification rules.

---

### 4.7 `config/` — Shared Configuration Files

| File | What it is |
|------|-----------|
| `config/brand_indicators.json` | Single source of truth for brand keywords. Edit this to add a new sub-brand. |

All Python consumers load this via `brand_config.py`.

---

### 4.8 `data/` — Reference Data

| File | What it is | Used by |
|------|-----------|---------|
| `data/twisted_x_skus_v107.xlsx` | Primary SKU database (3,000+ style codes) | `config.py` at startup |
| `data/twisted_x_sku.csv` | Fallback SKU database (CSV version) | `config.py` if xlsx fails |
| `data/CustomCustomerSearchResults990.csv` | NetSuite customer export (990 rows) | `python -m url_validator` standalone batch run |
| `data/url_validation_full_updated.csv` | Full URL validation results dataset | Reference / analysis |
| `data/error_urls_to_recheck.csv` | URLs with previous errors — re-run queue | `suggest_urls_for_bad_rows.py` |
| `data/missing_urls_exact_match.csv` | Customers with no URL, exact match found | `suggest_urls_for_bad_rows.py` |
| `data/missing_urls_from_custom_customer_search.csv` | Customers needing web search | `suggest_urls_for_bad_rows.py` |
| `data/twisted_x_global_brands_online_sellers.csv` | Known online sellers list | Reference |

**The active pipeline input is `QueryResults_837.csv`** (in repo root, set in `.env` as `INPUT_FILE`).

---

### 4.9 Utility Scripts

These run standalone from the command line. None are imported by `api_server.py` or the pipeline.

#### `suggest_urls_for_bad_rows.py` (~870 lines)
Finds correct URLs for retailer rows with no website or a broken URL.

Search strategy order:
1. Validate existing URL if present
2. Gemini with Google Search grounding (requires `GEMINI_API_KEY`)
3. Startpage web search
4. DuckDuckGo fallback

Each candidate is scored (company name match, domain relevance, reachability). Best match
above `MIN_SCORE_THRESHOLD` (default 20) is written back.

```bash
python3 suggest_urls_for_bad_rows.py \
  --input  data/missing_urls_from_custom_customer_search.csv \
  --output data/missing_urls_fixed.csv \
  [--no-recheck]   # skip /api/check after fixing
```

---

#### `batch_check_excel.py` (~200 lines)
Reads an Excel file of retailer URLs, calls `/api/check` for each, writes
`Sell Anything`, `Sell Footwear`, `Sell Twisted X` columns back into the file.

Requires `api_server.py` running on port 8000.

```bash
python3 batch_check_excel.py --input retailers.xlsx --sheet Sheet1 --url-col Website
```

---

#### `check_suggested_urls.py` (~180 lines)
Narrower version of `batch_check_excel.py`. Reads a `"Suggested URL"` column and writes
back `"Sell Twisted X on suggested"`. Used to validate output of `suggest_urls_for_bad_rows.py`.

---

#### `fill_phones.py` (~166 lines)
Fills blank `places_national_phone` cells in an enriched CSV using the Google Places API.
Only targets rows where phone is blank AND city/state is a real store location.

```bash
python3 fill_phones.py \
  --input  QueryResults_837_Enriched.csv \
  --output QueryResults_837_Enriched_WithPhones.csv
```

---

### 4.10 `scripts/` and `tests/`

**`scripts/debug/debug_batch_repro.py`**
Diagnostic script for reproducing batch failures on a specific URL. Not for production use.

**`tests/manual/`** — manual diagnostic scripts (not pytest), run against a live API server.

| Script | What it tests |
|--------|--------------|
| `test_dillards.py` | Dillard's URL detection (Playwright heavy) |
| `test_root_domain.py` | Root domain extraction edge cases |
| `test_sftp_connection.py` | SFTP connectivity check |

```bash
source venv/bin/activate && python3 tests/manual/test_sftp_connection.py
```

---

## 5. Data Flows

### Flow A: Celigo Retailer Sync (API)

```
Celigo scheduler triggers (e.g. nightly)
  |
  +- Pull batch of retailer URLs from NetSuite
  |
  +- For each URL:
  |   |
  |   +- POST /api/check {"url": "https://www.bootbarn.com"}
  |   |   +- checker.run_check()
  |   |       +- Layer 1: HTTP GET -> scan for TX SKUs -> definitive? stop
  |   |       +- Layer 2: sitemap scan -> TX slug? stop
  |   |       +- Layer 3: SerpApi -> Google Search -> definitive? stop
  |   |       +- Layer 4: Playwright -> platform detect -> search -> scan
  |   |   -> {sells_twisted_x: true, confidence: "high", sample_products: [...]}
  |   |
  |   +- POST /api/scrape {"url": "...", "search_term": "Twisted X"}
  |   |   +- cleaning.py: targeted -> segmented -> fullpage
  |   |   -> {products: [ProductBlock, ProductBlock, ...]}
  |   |
  |   +- Celigo sends ProductBlocks to Anthropic (Claude)
  |   |   using celigo/extraction_system_prompt.txt
  |   |   -> [{name, sku, price, image_url, product_url}, ...]
  |   |
  |   +- POST /api/verify {"extracted_products": [...], "original_products": [...]}
  |   |   +- verifier.py: fuzzy match each field against source blocks
  |   |   -> {verified_products: [...], flagged_products: [...],
  |   |        verification_stats: {total_input, verified, flagged}}
  |   |
  |   +- Celigo applies its own classification rules (not in this repo)
  |       e.g. gender, product_type, work_features (Celigo-side only)
  |
  +- Write verified product records to NetSuite
```

---

### Flow B: Customer Enrichment (Celigo)

The enrichment flow is fully Celigo-orchestrated via the API endpoints.
There is no CSV file hand-off — Celigo reads from NetSuite, calls the API, and writes back directly.

```
Celigo scheduler triggers (e.g. nightly)
  |
  +- Pull batch of customer records from NetSuite
  |   Fields: internal_id, company, address, city, state, zip_code,
  |           website_url, last_enrichment_date, enrichment_source
  |
  +- Celigo native filter (no API call)
  |   Skip records where last_enrichment_date is within TTL days
  |   AND enrichment_source is not "text_search" or "enrichment_error"
  |   (last_enrichment_date and enrichment_source stored in NetSuite)
  |
  +- POST /api/enrich/url-ping  (stale records only)
  |   [{internal_id, url}, ...]
  |   -> {alive: [...], dead: [...], missing: [...], details: [...]}
  |   Celigo notes which URLs are dead/missing (needs new URL from enrichment)
  |
  +- POST /api/enrich/batch  (up to 100 stale records per call)
  |   [{company, address, city, state, zip_code, internal_id, current_url}, ...]
  |   -> {results: [{internal_id, result: EnrichResponse}, ...], duration_sec}
  |   Each result: found_url, phone, address, lat/lng, place_id, hours,
  |                match_confidence, enrichment_source, address_match
  |
  +- POST /api/enrich/classify-retail  (per record, using Places primary_type)
  |   {primary_type, has_opening_hours, is_channel_row}
  |   -> {retail_type: "retail" | "not_retail" | "unknown"}
  |
  +- POST /api/check  (optional — only for records with a live ecommerce URL)
  |   {url: found_url}
  |   -> {sells_twisted_x, sells_online, sells_footwear, confidence, ...}
  |
  +- Celigo native field mapping (no API call needed)
  |   maps sells_twisted_x / sells_anything / sells_shoes / found_url
  |   -> NetSuite online_sales_status dropdown value
  |
  +- Write enriched fields back to NetSuite per record
     found_url, phone, address, lat/lng, place_id, hours,
     match_confidence, enrichment_source, retail_type,
     sells_twisted_x, online_sales_status
```

**Fallback (manual / no Celigo):** `POST /api/enrich/pipeline` or
`python3 url_enrichment_pipeline.py` — reads `INPUT_FILE`, writes `OUTPUT_FILE`.
Use this for one-off bulk runs or if Celigo is unavailable.

---

### Flow C: URL Recovery (suggest_urls)

```
Trigger: python3 suggest_urls_for_bad_rows.py --input missing_urls.csv
  |
  +- For each row with no URL or broken URL:
  |   +- Try Gemini + Google Search grounding (if API key available)
  |   +- Try Startpage web search
  |   +- Try DuckDuckGo
  |
  +- Score each candidate URL:
  |   - Company name similarity
  |   - Domain relevance keywords (western, boots, ranch, etc.)
  |   - Reachability (HEAD request)
  |
  +- Pick best candidate above MIN_SCORE_THRESHOLD (20)
  |
  +- (Optional) POST /api/check on the winning URL
     -> validate it actually sells Twisted X
     -> write results back to Excel
```

---

## 6. Configuration Reference

All configuration via environment variables. Copy `.env.example` -> `.env` and fill in real values.

| Variable | Required for | Default | Description |
|----------|-------------|---------|-------------|
| `GOOGLE_PLACES_API_KEY` | Enrichment pipeline + all `/api/enrich/*` | — | Google Places + Address Validation API key |
| `ENRICH_API_KEY` | All `/api/enrich/*` endpoints | — | Shared secret; pass as `X-API-Key` header. Server refuses to start if unset. |
| `SERPAPI_KEY` | Layer 3 retailer detection | — | SerpApi Google Search key — bypasses bot protection. Free tier: 100/month. Leave blank to skip Layer 3. |
| `USE_SFTP` | Enrichment pipeline | `false` | `true` = SFTP mode (Celigo automated flow) |
| `INPUT_FILE` | Enrichment pipeline | `QueryResults_837.csv` | Local input CSV (ignored when USE_SFTP=true) |
| `OUTPUT_FILE` | Enrichment pipeline | `QueryResults_837_Enriched.csv` | Local output CSV |
| `SFTP_HOST` | SFTP mode | — | SFTP server hostname |
| `SFTP_PORT` | SFTP mode | `22` | SFTP port |
| `SFTP_USER` | SFTP mode | — | SFTP username |
| `SFTP_KEY_PATH` | SFTP mode | — | Path to SSH private key (blank = use password) |
| `SFTP_PASSWORD` | SFTP mode | — | SFTP password |
| `SFTP_INBOUND_DIR` | SFTP mode | `/inbound` | Dir where Celigo drops CSVs |
| `SFTP_REVIEW_DIR` | SFTP mode | `/review` | Dir to upload enriched output |
| `SFTP_ARCHIVE_DIR` | SFTP mode | `/archive` | Dir to move processed input |
| `ENABLE_PRODUCT_CHECK` | Enrichment pipeline | `false` | Call `/api/check` per URL |
| `ENRICHMENT_TTL_DAYS` | Enrichment pipeline | `30` | Skip re-enrichment within this many days (Celigo handles this filter natively) |
| `SKU_XLSX_FILENAME` | API server | `twisted_x_skus_v107.xlsx` | Override SKU database filename |
| `SKU_CSV_FILENAME` | API server | `twisted_x_sku.csv` | Override fallback SKU CSV |
| `MIN_EXPECTED_STYLE_CODES` | API server | `1000` | Server refuses to start if fewer codes loaded |
| `PRODUCTSEARCH_URL` | Image matching | — | On-prem marqo image similarity API |
| `PRODUCTSEARCH_USER` | Image matching | — | Image API username |
| `PRODUCTSEARCH_PASS` | Image matching | — | Image API password |

---

## 7. Key Design Decisions

### Why Playwright instead of `requests`?
Most western wear retailer sites render product listings via JavaScript. A plain HTTP request
gets an empty product grid. Playwright runs real Chromium so JS executes and product data loads.

### Why three detection layers, not just Playwright?
Playwright is slow (15-60 seconds per site) and detectable by bot-protection systems. HTTP
checks and sitemap scans take under a second with no browser fingerprint. For roughly 30% of
retailers, the sitemap alone confirms TX products — no browser needed.

### Why is the LLM in Celigo, not here?
Twisted X IT already runs Celigo for NetSuite integrations. Centralising the LLM call there
means AI governance, billing, and retry logic are managed in one place. This Python service
is intentionally "dumb" — it fetches DOM and verifies output, but never generates it.

### Why SKU fingerprinting and not just brand name matching?
Brand name matching produces false positives: "Twisted X" can appear in a blog post, a
navigation link, or a "brands we carry" sidebar. SKU codes like `MCA0070` are globally unique
to specific Twisted X products. A SKU match is near-certain proof of an actual product listing.

### Why does `/api/verify` not return gender, product type, or work features?
The classification step (gender / product type / work features) is Celigo's responsibility,
applied after `/api/verify` returns. This Python service is intentionally scoped to
anti-hallucination verification only — it checks that LLM-extracted fields actually exist
in the source DOM blocks. Classification logic lives in Celigo's platform (not in this repo)
because Celigo already owns the NetSuite write-back step where those fields are needed.

### Why deduplicate by company before calling Google Places?
A single retailer like "Boot Barn" may have 50+ rows in NetSuite (one per branch, plus channel
rows like "Boot Barn - ecommerce"). Calling Google Places once per row wastes quota and
produces inconsistent results. `build_branch_norms()` groups all rows and makes one API call
per company.

---

## 8. Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Language | Python | 3.11+ | All server and pipeline code |
| API framework | FastAPI | 0.104+ | `/api/check`, `/api/scrape`, `/api/verify` |
| ASGI server | Uvicorn | 0.24+ | FastAPI production server |
| Browser automation | Playwright (Chromium) | 1.49 | JS-rendered page scraping |
| Bot detection evasion | playwright-stealth | 1.0.6+ | Reduces Cloudflare/PerimeterX detection |
| Data validation | Pydantic v2 | 2.x | Request/response models |
| Async HTTP | aiohttp | 3.8+ | Bulk URL pinging (enrichment pipeline) |
| Sync HTTP | requests | 2.31+ | Layer 1/2 checks, Places API |
| DataFrame | pandas | 1.5+ | CSV loading/manipulation |
| Excel | openpyxl | 3.10+ | Reading/writing `.xlsx` files |
| SFTP | paramiko | 3.0+ | File transfer to/from Celigo file server |
| Progress bars | tqdm | 4.65+ | Pipeline progress display |
| Env vars | python-dotenv | 1.0+ | `.env` file loading |
| Integration | Celigo (cloud) | — | Orchestration, LLM calls, NetSuite sync |
| LLM | Anthropic Claude | — | Product extraction (called by Celigo, not this service) |
| Places lookup | Google Places API (New) | — | Retailer address/phone/URL enrichment via Text Search |
| Address lookup | Google Address Validation API | — | Geocode physical address → lat/lng → location-biased Places search |
| URL search | Google Custom Search Engine | — | Fallback URL discovery |

---

## 9. Known Limitations & Roadmap

### Current Limitations

| Issue | Impact | Status |
|-------|--------|--------|
| Cloudflare / PerimeterX blocks major retailers | Layer 3 (SerpApi) catches these via Google index — Playwright only needed for unindexed/unknown sites | Largely mitigated by SerpApi Layer 3 |
| No pytest for Playwright layers | Browser-dependent layers need integration harness | Planned — pure-Python layers have 265 unit tests |
| `suggest_urls_for_bad_rows.py` is 870 lines | Hard to maintain | Low priority — runs rarely |
| Sync Playwright (blocking) | One scrape blocks the API worker | Acceptable at current volume |
| Product classification (gender/type) not in Python API | Classification runs in Celigo after `/api/verify` | Deliberate — see design decision above |

### Completed Improvements (2026-05-14)

**checker/ — 4-layer detection**
- **Layer 3 (SerpApi)** added: `checker/_serp.py` — searches Google for `"Twisted X site:<domain>"` via SerpApi JSON API. Catches bot-protected retailers (Boot Barn, Cavenders) that Playwright would struggle with. Gracefully disabled when `SERPAPI_KEY` is blank. `sells_online` derived from ecommerce path signals in result URLs (`/product`, `/shop`, `/cart`, `/collections`, etc.) — not hardcoded.
- **Layer 1 fix**: `sells_online` and `store_type` are now derived from whether found products have a price or `product_url`, rather than hardcoded `True`/`"ecommerce"`.
- **Layer 2 improvements**:
  - TX regex expanded to also catch `tx-boots`, `tx-footwear`, `tx-work`, `tx-western` URL slugs
  - Child sitemaps prioritised — `product/brand/categor/collection/catalog` fetched first; `blog/news/video/media` skipped entirely
  - URL scan cap removed (string matching is free; HTTP fetches are the bottleneck)
  - Child sitemap fetch limit raised 3 → 10 (ordered by relevance, so the right ones come first)
  - Gzip detection now checks `Content-Encoding: gzip` header in addition to `.gz` filename suffix
  - `sells_online` derived from found page URL path segments, not hardcoded

**api_server.py**
- `X-Idempotency-Key` header support on `/api/enrich/batch` — 30-minute server-side cache prevents re-burning Google quota on Celigo retries
- `asyncio.Semaphore(3)` added to `/api/check` — caps concurrent Playwright browsers to prevent OOM
- `google_api_calls` and `quota_errors` fields added to `BatchEnrichResponse` for quota visibility
- Removed `POST /api/enrich/ttl-check` — staleness logic moved to Celigo natively (requires `last_enrichment_date` and `enrichment_source` stored in NetSuite)
- Removed `POST /api/enrich/online-status` — NetSuite field mapping handled by Celigo natively

**enrichment/_enrich_single.py**
- `google_api_calls` counter added — tracks 0-3 calls per record and surfaces in `EnrichResponse`

**config.py**
- `SERPAPI_KEY` env var added

### Completed Refactors (2026-05-11) — Enrichment API

- **`enrichment/_enrich_single.py`** (new): `enrich_single_customer()` orchestrates the new
  Address Validation → location-biased Text Search → fallback flow per record.
  - PO box detection skips directly to text search fallback.
  - `is_business` flag from Address Validation is logged only — never used for routing.
  - Never raises — all Google API failures reflected in `enrichment_source`.
  - PII-safe: company name, street address, and `internal_id` never logged.
- **`enrichment/_address_validation.py`** (new): `validate_address()` and
  `find_places_near_location()` wrappers with retry, error classification, and full error
  propagation via `(result, error_code)` tuple return convention.
- **`enrichment/_pipeline.py`**: `_run_places_loop()` (per-company) replaced by
  `_run_enrich_loop()` (per-row) which calls `enrich_single_customer()` per stale row.
  Old loop kept as `_run_places_loop_legacy()` for reference.
- **`enrichment/__init__.py`**: exports `enrich_single_customer` in addition to `run_pipeline`.
- **`api_server.py`**: 8 new endpoints under `/api/enrich/*`:
  `POST /api/enrich`, `/api/enrich/batch`, `/api/enrich/pipeline`,
  `/api/enrich/ttl-check`, `/api/enrich/url-ping`, `/api/enrich/online-status`,
  `/api/enrich/address-validate`, `/api/enrich/classify-retail`.
  All protected by `ENRICH_API_KEY` via `X-API-Key` header (HMAC constant-time compare).
  Server refuses to start if `ENRICH_API_KEY` is unset.
- **`models.py`**: 14 new Pydantic models for all 8 enrichment endpoints.

### Completed Refactors (2026-05-05 → 2026-05-06)

- `api_server.py`: 1,339 → ~400 lines (dead code removed, logic moved to `checker/`)
- `url_enrichment_pipeline.py`: 1,520 → 5 lines (all logic moved to `enrichment/` package)
- `checker/` package created with no circular imports back to `api_server`
- `url_validator.py` → `url_validator/` package: split into 7 focused sub-modules
  (`_constants`, `_brand`, `_browser`, `_detect`, `_check`, `_batch`, `__main__`).
  All public and private symbols re-exported from `__init__.py` for zero breaking changes.
- `url_validator`: 9 bugs fixed (cross-site state bleed, null JSHandle, wrong column name,
  missing rate limiting, false-positive blockers, redundant reloads, hooey false positives,
  duplicate brand lists, search URL false positives)
- All hardcoded brand keyword lists replaced with `brand_config.py` → `config/brand_indicators.json`
- `cleaning.py`: `MAX_PRODUCTS` raised 100 → 300; scroll-before-extract added to load lazy products
- `api_server.py` `_scrape_url_sync`: added `_navigate_to_best_tx_page()` so extraction always
  starts from the richest TX catalog page, not the last footwear-detection URL
- `_click_next_page()`: fixed false-positive returns — now requires URL or content change before
  reporting pagination success, eliminating duplicate product extraction
- `ScrapeRequestNew.max_pages` default raised 10 → 15
- ~15 MB of scratch CSVs and old Excel files removed from repo

### Planned

- Automated pytest suite for `checker/` detection logic
- MAP price compliance checking (blocked on this cleanup landing first)
- DuckDB integration for local product image matching
- Async Playwright for higher API throughput
