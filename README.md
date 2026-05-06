# twisted-x-scraper

Twisted X Global Brands retailer detection, product scraping, and customer
enrichment system. Two main flows:

1. **Retailer Detection API** — FastAPI service that Celigo calls to determine
   whether a retailer website sells Twisted X products online.
2. **Customer Enrichment Pipeline** — CLI that reads a NetSuite customer CSV,
   looks up each retailer on Google Places, pings their URLs, and writes back
   verified contact data and a `online_sales_status` value ready for NetSuite.

For full detail on every file, data flow, and design decision, see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## What's in the repo

```
api_server.py               FastAPI service — /api/check, /api/scrape, /api/verify
url_enrichment_pipeline.py  Entry point for the CSV enrichment pipeline

checker/                    3-layer retailer detection package
  __init__.py               run_check(url) — public entry point
  _http.py                  Layer 1: plain HTTP scan (no browser)
  _sitemap.py               Layer 2: sitemap URL slug scan (no browser)
  _playwright.py            Layer 3: full Playwright browser check
  _platform.py              Shopify / WooCommerce / NetSuite / generic detection
  _search.py                Platform-aware search strategies
  _scanners.py              SKU fingerprint + brand-context DOM scanning
  _types.py                 TypedDict definitions + result factories

enrichment/                 Customer enrichment package
  __init__.py               run_pipeline() — public entry point
  _pipeline.py              Orchestrator — 8 named steps
  _config.py                All env vars and constants
  _url.py                   Async URL pinging (aiohttp)
  _places.py                Google Places API integration
  _address.py               Address normalisation + matching
  _company.py               Company deduplication + branch logic
  _retail.py                Retail type classification + known domain lookup
  _product.py               Product signals -> NetSuite online_sales_status
  _io.py                    CSV/Excel load, save, SFTP upload

url_validator/              Playwright deep validator (used by checker Layer 3)
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
config.py                   SKU database + Playwright settings
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

suggest_urls_for_bad_rows.py   Find correct URLs for missing/broken rows
batch_check_excel.py           Bulk check Excel file via /api/check
check_suggested_urls.py        Validate suggested URLs via /api/check
fill_phones.py                 Fill missing phone numbers via Google Places

scripts/debug/              Diagnostic repro scripts (not production)
tests/manual/               Manual smoke-test scripts (not pytest)
```

---

## How the detection works

`checker.run_check(url)` runs three layers in order and short-circuits as soon
as it has a definitive answer:

```
Layer 1 — HTTP scan (no browser, ~1 request)
  Plain GET with Chrome UA. Scans HTML for TX style codes (MCA0070, ICA0035...).
  If a SKU matches -> definitive YES, stop.

Layer 2 — Sitemap scan (no browser, ~2-5 requests)
  Fetches robots.txt + sitemap.xml. Scans <loc> URLs for "twisted-x" slugs.
  If TX slug found -> definitive YES, stop.

Layer 3 — Playwright (real browser, 15-60 seconds)
  Detects platform (Shopify / WooCommerce / NetSuite / custom).
  Runs platform-appropriate search for "Twisted X".
  Scans results for SKUs and brand names.
  Delegates online-sales + footwear detection to the url_validator/ package.
```

Roughly 30% of retailers are resolved by Layers 1 or 2 with no browser needed.

---

## Setup

```bash
# Clone and create virtual environment
python3 -m venv venv && source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt
playwright install chromium

# Configure secrets
cp .env.example .env
# Edit .env -- fill in GOOGLE_PLACES_API_KEY and any SFTP credentials
```

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
| `enrichment_source` | `hybrid_full` / `google_places` / `url_only` / `enrichment_error` |
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
# Find correct URLs for retailers with no/broken website
python3 suggest_urls_for_bad_rows.py \
  --input  data/missing_urls_from_custom_customer_search.csv \
  --output data/missing_urls_fixed.csv

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
| `GOOGLE_PLACES_API_KEY` | Enrichment pipeline | Google Places Text Search |
| `GOOGLE_CSE_API_KEY` + `GOOGLE_CSE_CX` | URL recovery script | Fallback URL search |
| `USE_SFTP` | Enrichment pipeline | `true` = SFTP mode, `false` = local files |
| `INPUT_FILE` | Local mode | CSV to read (default: `QueryResults_837.csv`) |
| `OUTPUT_FILE` | Local mode | CSV to write (default: `QueryResults_837_Enriched.csv`) |
| `SFTP_HOST/USER/PASSWORD` | SFTP mode | File server credentials |
| `ENABLE_PRODUCT_CHECK` | Enrichment pipeline | `true` = call `/api/check` per URL |
| `ENRICHMENT_TTL_DAYS` | Enrichment pipeline | Skip rows enriched within N days (default 90) |

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
