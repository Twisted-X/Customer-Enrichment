# twisted-x-scraper

Twisted X Global Brands retailer detection, product scraping, and customer
enrichment system. Two main flows:

1. **Retailer Detection API** — FastAPI service that Celigo calls to determine
   whether a retailer website sells Twisted X products online.
2. **Customer Enrichment Pipeline** — CLI that reads a NetSuite customer CSV, Excel, or JSON
   file, looks up each retailer on Google Places, pings their URLs, and writes back
   verified contact data and an `online_sales_status` value ready for NetSuite.

For full detail on every file, data flow, and design decision, see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## What's in the repo

```
api_server.py               FastAPI service — /api/check, /api/scrape, /api/verify
url_enrichment_pipeline.py  Entry point for the CSV enrichment pipeline

checker/                    4-layer retailer detection package
  __init__.py               run_check(url) — public entry point
  _http.py                  Layer 1: plain HTTP scan (no browser)
  _sitemap.py               Layer 2: sitemap URL slug scan (no browser)
  _serp.py                  Layer 3: SerpApi Google Search (no browser — bypasses bot protection)
  _playwright.py            Layer 4: full Playwright browser check
  _platform.py              Shopify / WooCommerce / NetSuite / generic detection
  _search.py                Platform-aware search strategies
  _scanners.py              SKU fingerprint + brand-context DOM scanning
  _types.py                 TypedDict definitions + result factories

enrichment/                 Customer enrichment package
  __init__.py               run_pipeline(), enrich_single_customer() — public entry points
  _pipeline.py              Orchestrator — 8 named steps (per-row enrichment via _enrich_single)
  _enrich_single.py         Single-record orchestration: Address Validation → Places → Text Search
  _address_validation.py    Google Address Validation API + location-biased Text Search wrappers
  _config.py                All env vars and constants
  _url.py                   Async URL pinging (aiohttp)
  _places.py                Google Places Text Search API integration
  _address.py               Address normalisation + matching
  _company.py               Company deduplication + branch logic
  _retail.py                Retail type classification + known domain lookup
  _product.py               Product signals -> NetSuite online_sales_status
  _io.py                    CSV/Excel load, save, SFTP upload

url_validator/              Playwright deep validator (used by checker Layer 4)
  __init__.py               Public re-exports — all existing import patterns preserved
  __main__.py               CLI: python -m url_validator [input.csv [output.csv]]
  _constants.py             All selector lists and config values (pure data, no Playwright)
  _brand.py                 normalize_url(), brand/site classification helpers
  _browser.py               _close_popups(), _search_on_site(), category navigation
  _detect.py                detect_twisted_x(), detect_online_sales_capability(), detect_footwear()
  _check.py                 check_url() orchestrator + brand-site override logic
  _batch.py                 validate_urls() batch processor + CSV I/O

cleaning.py                 DOM product block extraction (used by /api/scrape)
                            Scrolls page before extraction to load lazy content; max 300 products
verifier.py                 Anti-hallucination verification (used by /api/verify)
models.py                   Pydantic request/response schemas (scrape max_pages default: 15)
config.py                   SKU database + Playwright settings + SerpApi key
brand_config.py             Loader for config/brand_indicators.json
sftp_connect.py             SFTP multi-auth helper

celigo/                     Celigo-side assets — gitignored, not in this repo
                            (prompts, classification rules, API contract)
                            Classification (gender/product type) runs in Celigo,
                            not in this Python service.

config/
  brand_indicators.json     Single source of truth for brand keywords

data/
  twisted_x_skus_v107.xlsx  Primary SKU database (3,000+ style codes)
  twisted_x_sku.csv         Fallback SKU database
  QueryResults_837.csv      Active NetSuite input (current enrichment run)
  ...                       Reference datasets and URL work queues

batch_check_excel.py           Bulk check Excel file via /api/check
check_suggested_urls.py        Validate suggested URLs via /api/check
fill_phones.py                 Fill missing phone numbers via Google Places

scripts/debug/              Diagnostic repro scripts (not production)
tests/manual/               Manual smoke-test scripts (not pytest)
```

---

## How the detection works

`checker.run_check(url)` runs four layers in order and short-circuits as soon
as it has a definitive answer:

```
Layer 1 — HTTP scan (no browser, ~1 request)
  Plain GET with Chrome UA. Scans HTML for TX style codes (MCA0070, ICA0035...).
  sells_online derived from whether found products have a price or product URL.
  If a SKU matches -> definitive YES, stop.

Layer 2 — Sitemap scan (no browser, ~2-10 requests)
  Fetches robots.txt + sitemap.xml. Prioritises product/brand/category child
  sitemaps; skips blog/news/video sitemaps entirely. Scans all <loc> URLs for
  "twisted-x", "twistedx", "tx-boots", "tx-footwear" etc. slugs.
  If TX slug found -> definitive YES, stop.

Layer 3 — SerpApi Google Search (no browser, ~1 API call)
  Searches Google for "Twisted X site:<domain>" via SerpApi JSON API.
  Bypasses Cloudflare / PerimeterX — Google has already indexed the site.
  If Google results found -> definitive YES, stop.
  No results is NOT a definitive NO (small sites may not be indexed).

Layer 4 — Playwright (real browser, 15-60 seconds)
  Detects platform (Shopify / WooCommerce / NetSuite / custom).
  Runs platform-appropriate search for "Twisted X".
  Scans results for SKUs and brand names.
  Delegates online-sales + footwear detection to the url_validator/ package.
```

Layers 1-3 are cheap. Layer 4 is expensive. The design short-circuits so
Playwright only runs when the three cheap layers are all inconclusive.

---

## Setup

```bash
# Clone and create virtual environment
python3 -m venv venv && source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt

# Install Playwright and Patchright browsers
playwright install chromium
patchright install chromium

# Configure secrets
cp .env.example .env
# Edit .env — fill in GOOGLE_PLACES_API_KEY, ENRICH_API_KEY, SERPAPI_KEY
```

**Key dependencies:**

| Package | Purpose |
|---------|---------|
| `patchright` | Stealth browser (anti-bot detection) — Layer 4 |
| `playwright` | Browser automation base |
| `curl_cffi` | HTTP client with TLS fingerprint spoofing — Layers 1 & 2 |
| `serpapi` / `requests` | SerpApi Google Search — Layer 3 |
| `fastapi` + `uvicorn` | REST API server |
| `pandas` + `openpyxl` | CSV / Excel / JSON input-output |
| `paramiko` | SFTP for automated Celigo pipeline |
| `httpx` | Required by FastAPI `TestClient` |
| `typing_extensions` | Backported type hints (`TypedDict`) |

---

## Running the API server

```bash
source venv/bin/activate
uvicorn api_server:app --reload --port 8000
```

The server loads the SKU database at startup. If `data/twisted_x_skus_v107.xlsx`
is missing or loads fewer than 1,000 style codes, startup fails immediately
(a missing SKU database would silently return wrong results).

**Smoke test:**
```bash
curl -X POST http://localhost:8000/api/check \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://www.atwoods.com/"}'
```

**Endpoints:**

| Endpoint | What it does |
|----------|-------------|
| `POST /api/check` | Does this retailer sell Twisted X? Returns yes/no + SKUs found + confidence |
| `POST /api/scrape` | Extract raw DOM product blocks (Celigo sends these to Claude) |
| `POST /api/verify` | Anti-hallucination: cross-check LLM output against source blocks |
| `GET /health` | Returns `{"status": "healthy", "timestamp": "..."}` |
| `GET /api/test` | Smoke test — confirms SKU database is loaded |

**Enrichment endpoints** (all require `X-API-Key` header matching `ENRICH_API_KEY` env var):

| Endpoint | What it does |
|----------|-------------|
| `POST /api/enrich` | Enrich one customer record: Address Validation → Places Details → Text Search fallback |
| `POST /api/enrich/batch` | Enrich up to 100 records concurrently. Supports `X-Idempotency-Key` header (30 min cache). Returns `google_api_calls` + `quota_errors` totals. |
| `POST /api/enrich/url-ping` | Are these URLs still alive? Returns alive/dead/missing buckets with HTTP codes |
| `POST /api/enrich/address-validate` | Debug: geocode a single address (Address Validation API only, no Places lookup) |
| `POST /api/enrich/classify-retail` | Classify a business as `retail` / `not_retail` / `unknown` (pure logic, no API calls) |

---

## Running the enrichment pipeline

```bash
source venv/bin/activate

# Local mode (reads INPUT_FILE from .env, writes OUTPUT_FILE)
python3 url_enrichment_pipeline.py

# SFTP mode (pulls from /inbound, pushes to /review, archives to /archive)
USE_SFTP=true python3 url_enrichment_pipeline.py
```

`GOOGLE_PLACES_API_KEY` must be set in `.env` — the pipeline raises an error
at startup if it is missing.

**What the pipeline outputs (columns added to the CSV):**

| Column | Description |
|--------|-------------|
| `url_check_status` | `active` / `redirected` / `dead` / `blocked` / `not_found` |
| `found_url` | Canonical URL after following redirects |
| `google_business_name` | Name from Google Places |
| `places_primary_type` | e.g. `clothing_store`, `sporting_goods_store` |
| `places_national_phone` | Formatted phone number |
| `places_formatted_address` | Full address from Places |
| `address_match` | `True/False` — does Places address match NetSuite address? |
| `match_confidence` | `high` / `medium` / `low` |
| `places_latitude` / `places_longitude` | Coordinates |
| `retail_type` | `retail` / `online_only` / `wholesale` / `unknown` |
| `sells_anything` | `yes` / `no` / `unknown` (requires ENABLE_PRODUCT_CHECK=true) |
| `sells_shoes` | `yes` / `no` / `unknown` |
| `sells_twisted_x` | `yes` / `no` / `unknown` |
| `online_sales_status` | NetSuite dropdown value (see below) |
| `enrichment_source` | `address_validation` / `text_search` / `not_found` / `enrichment_error` |
| `last_enrichment_date` | ISO date of this run |

**NetSuite `online_sales_status` mapping:**

| Condition | Value written to NetSuite |
|-----------|--------------------------|
| No website | `No Website` |
| Sells Twisted X online | `Ecommerce Site : Sells Twisted X` |
| Sells shoes online (not TX) | `Ecommerce Site : Opportunity` |
| Sells online (no shoes) | `Ecommerce Site : Does Not Sell Twisted X` |
| No online sales detected | `No Ecommerce` |
| Not enough data | _(blank — Celigo does not overwrite)_ |

---

## Utility scripts

All require the API server running on port 8000 unless noted.

```bash
# Bulk check an Excel file of retailer URLs
python3 batch_check_excel.py --input retailers.xlsx --url-col "website url"

# Validate a "Suggested URL" column in an Excel file
python3 check_suggested_urls.py --input retailers.xlsx

# Fill missing phone numbers in an enriched CSV (no API server needed)
python3 fill_phones.py \
  --input  QueryResults_837_Enriched.csv \
  --output QueryResults_837_Enriched_WithPhones.csv
```

---

## Configuration

All configuration is via environment variables. See `.env.example` for the
full list with descriptions. Key variables:

| Variable | Required for | What it does |
|----------|-------------|-------------|
| `GOOGLE_PLACES_API_KEY` | Enrichment pipeline + enrich API | Google Places Text Search + Address Validation |
| `ENRICH_API_KEY` | All `/api/enrich/*` endpoints | Shared secret — pass as `X-API-Key` header |
| `SERPAPI_KEY` | Layer 3 retailer detection | SerpApi Google Search — bypasses bot protection on major retailers. Free tier: 100/month. Leave blank to skip Layer 3. |
| `USE_SFTP` | Enrichment pipeline | `true` = SFTP mode, `false` = local files |
| `INPUT_FILE` | Local mode | CSV to read (default: `QueryResults_837.csv`) |
| `OUTPUT_FILE` | Local mode | CSV to write (default: `QueryResults_837_Enriched.csv`) |
| `SFTP_HOST/USER/PASSWORD` | SFTP mode | File server credentials |
| `ENABLE_PRODUCT_CHECK` | Enrichment pipeline | `true` = call `/api/check` per URL |

---

## Adding a new brand or product line

Edit **`config/brand_indicators.json`** only:

```json
{
  "brands": ["twisted x", "twistedx", "black star", "cellsole", "hooey"],
  "product_lines": ["tech x", "feather x", "cellstretch", "...your new line..."]
}
```

Every consumer (`verifier.py`, `url_validator/`, `api_server.py`,
`checker/_scanners.py`) imports from `brand_config.py` which reads this file
at startup. No other files need to change.

---

## Code conventions

- **No hardcoded paths** — everything resolves relative to repo root or via env vars
- **No bare `except:`** — use `except Exception:` or a specific type so `KeyboardInterrupt` propagates
- **No global SSL bypass** — use `SSL_INSECURE_DOMAINS` env var for per-domain exceptions
- **No hardcoded brand strings** — edit `config/brand_indicators.json` only
- **No mock data** — if data is unavailable, raise an error; never return fake results
- **Manual scripts** belong under `scripts/debug/` or `tests/manual/`, not in repo root

---

## Documentation

| File | What's in it |
|------|-------------|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Full system architecture, every file documented, all data flows, design decisions, configuration reference |
| [`tests/manual/README.md`](tests/manual/README.md) | How to run manual diagnostic scripts |
| [`scripts/debug/README.md`](scripts/debug/README.md) | Debug repro script usage |

> **Note:** `celigo/` is gitignored and not in this repo. API contract docs for the Celigo
> integration live in Celigo's platform. Endpoint shapes are documented in `ARCHITECTURE.md` §4.1.
