"""
enrichment — Customer URL enrichment pipeline for Twisted X.

Public API:
    from enrichment import run_pipeline
    run_pipeline()

Pipeline flow (see _pipeline.py for full detail):
  1. Resolve input/output paths (SFTP or local file)
  2. Load + normalise DataFrame (column renames, whitespace cleanup)
  3. Partition rows: fresh (skip) vs stale (enrich)
  4. Ping URLs for health (async, concurrent)
  5. Google Places lookup — one API call per unique company
  6. Optional product check via POST /api/check
  7. Compute NetSuite online_sales_status dropdown values
  8. Save CSV + JSON; upload to SFTP if configured
"""
from ._pipeline import run_pipeline

__all__ = ["run_pipeline"]
