"""
Customer URL Enrichment Pipeline — entry point.

All logic lives in the `enrichment/` package. See enrichment/__init__.py
for the full pipeline description and enrichment/_pipeline.py for the
step-by-step orchestration.

Usage:
    python url_enrichment_pipeline.py

    Environment variables (set in .env or shell):
        GOOGLE_PLACES_API_KEY  — required
        INPUT_FILE             — default: QueryResults_837.csv
        OUTPUT_FILE            — default: QueryResults_837_Enriched.csv
        USE_SFTP               — set to 'true' for automated Celigo flow
        ENABLE_PRODUCT_CHECK   — set to 'true' to call /api/check per domain
"""
from enrichment import run_pipeline

if __name__ == "__main__":
    run_pipeline()
