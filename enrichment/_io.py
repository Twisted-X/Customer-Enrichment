"""
File I/O and SFTP helpers.

Public API:
    sftp_session()                        -> context manager yielding sftp client
    resolve_input_file(sftp)              -> str  (remote path)
    derive_output_filename(remote_path)   -> str  (filename only)
    load_dataframe(path)                  -> tuple[pd.DataFrame, bool]  (df, is_csv)
    save_output(df, output_path, is_csv)  -> str  json_path
    should_enrich(row)                    -> bool
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager

import pandas as pd

from . import _config
from ._config import (
    SFTP_HOST, SFTP_PORT, SFTP_USER,
    SFTP_KEY_PATH, SFTP_PASSWORD,
    SFTP_INBOUND_DIR, SFTP_REVIEW_DIR, SFTP_ARCHIVE_DIR,
    COLUMN_MAP, NETSUITE_ID_COL, URL_COL, NETSUITE_LAST_ENRICHED_COL, ENRICHMENT_TTL_DAYS,
)

log = logging.getLogger(__name__)

# Invisible / whitespace characters stripped from string columns at load time
_INVISIBLE = '​ ﻿\r\n\t'


@contextmanager
def sftp_session():
    """
    Open a paramiko SFTP connection and yield the client.
    Tries public-key auth first, then password with keyboard-interactive
    fallback for SFTPGo and similar servers.
    """
    try:
        from sftp_connect import close_sftp, open_sftp
    except ImportError:
        raise ImportError("sftp_connect.py is required for SFTP (ships with this repo).")

    sftp = None
    try:
        sftp = open_sftp(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_KEY_PATH, SFTP_PASSWORD)
        yield sftp
    finally:
        if sftp is not None:
            close_sftp(sftp)


def resolve_input_file(sftp) -> str:
    """
    Find the oldest CSV in SFTP_INBOUND_DIR (FIFO processing order).
    Raises FileNotFoundError with a clear message if the folder is empty.
    """
    try:
        entries = sftp.listdir_attr(SFTP_INBOUND_DIR)
    except FileNotFoundError:
        raise FileNotFoundError(f"SFTP inbound directory not found: {SFTP_INBOUND_DIR}")

    csvs = [e for e in entries if e.filename.lower().endswith(".csv")]
    if not csvs:
        raise FileNotFoundError(
            f"No CSV files found in SFTP {SFTP_INBOUND_DIR} — nothing to process."
        )
    oldest = sorted(csvs, key=lambda e: e.st_mtime)[0]
    return f"{SFTP_INBOUND_DIR.rstrip('/')}/{oldest.filename}"


def derive_output_filename(input_remote_path: str) -> str:
    """
    Derive the enriched output filename from the input path.
    e.g. /inbound/customers_20260401.csv → customers_20260401_Enriched.csv
    """
    stem, ext = os.path.splitext(os.path.basename(input_remote_path))
    return f"{stem}_Enriched{ext}"


def load_dataframe(path: str) -> tuple:
    """
    Load a CSV or Excel file into a DataFrame.

    Applies COLUMN_MAP renames, strips invisible characters from URL and
    address columns, and replaces literal 'nan' strings (dtype=str artefact)
    with empty/NA.

    Returns (df, is_csv).
    """
    is_csv  = path.lower().endswith(".csv")
    is_json = path.lower().endswith(".json")
    if is_json:
        df = pd.read_json(path, dtype=str)
        df = df.astype(str)
    elif is_csv:
        df = pd.read_csv(path, dtype=str)
    else:
        df = pd.read_excel(path, dtype=str)
    log.info("Loaded %d records from %s", len(df), path)

    # Rename Celigo / export column labels to pipeline-internal names
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    if rename:
        df.rename(columns=rename, inplace=True)

    # Validate NetSuite Internal ID column when running via SFTP
    if _config.USE_SFTP and NETSUITE_ID_COL not in df.columns:
        raise ValueError(
            f"Column '{NETSUITE_ID_COL}' not found in input. "
            f"Add 'customer.id as \"{NETSUITE_ID_COL}\"' to the saved search. "
            f"Columns present: {list(df.columns[:10])}..."
        )

    # Validate URL column exists (try case-insensitive fallback)
    url_col = _resolve_column(df, URL_COL, required=True)

    # Strip invisible chars and normalise blank cells
    clean_cols = [url_col] + [c for c in ["address", "city", "state", "zip code"] if c in df.columns]
    for col in clean_cols:
        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.strip(_INVISIBLE)
            .replace({"nan": "", "NaN": ""})
            .replace("", pd.NA)
        )

    return df, is_json or is_csv  # treat JSON same as CSV for output routing


def save_output(df: pd.DataFrame, output_path: str, is_csv: bool) -> str:
    """
    Save the enriched DataFrame to CSV/JSON/Excel and a companion JSON file.
    Returns the JSON output path.
    """
    is_json_out = output_path.lower().endswith(".json")
    if is_json_out:
        records = df.where(df.notna(), other=None).to_dict(orient="records")
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=2, ensure_ascii=False, default=str)
    elif is_csv:
        df.to_csv(output_path, index=False)
    else:
        df.to_excel(output_path, index=False)

    json_path = os.path.splitext(output_path)[0] + ".json"
    records   = df.where(df.notna(), other=None).to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2, ensure_ascii=False, default=str)

    log.info("Saved %d rows → %s", len(df), output_path)
    log.info("Saved JSON   → %s", json_path)
    return json_path


def upload_results(output_path: str, json_path: str, remote_input_path: str) -> None:
    """Upload enriched CSV + JSON to SFTP review dir; archive the input file."""
    review_csv  = f"{SFTP_REVIEW_DIR.rstrip('/')}/{os.path.basename(output_path)}"
    review_json = f"{SFTP_REVIEW_DIR.rstrip('/')}/{os.path.basename(json_path)}"
    archive     = f"{SFTP_ARCHIVE_DIR.rstrip('/')}/{os.path.basename(remote_input_path)}"

    with sftp_session() as sftp:
        sftp.put(output_path, review_csv)
        log.info("Uploaded → %s", review_csv)
        sftp.put(json_path, review_json)
        log.info("Uploaded → %s", review_json)
        sftp.rename(remote_input_path, archive)
        log.info("Archived input: %s → %s", remote_input_path, archive)


def should_enrich(row) -> bool:
    """
    Return True if this row should be (re-)enriched this run.

    Always re-enriches:
      - last_enrichment_date is blank/missing
      - previous enrichment_source is 'enrichment_error' or 'address_mismatch'

    Skips re-enrichment (returns False):
      - last_enrichment_date is present and within ENRICHMENT_TTL_DAYS days
    """
    prev_src = str(row.get("enrichment_source", "") or "").strip()
    if prev_src in ("enrichment_error", "address_mismatch"):
        return True

    if NETSUITE_LAST_ENRICHED_COL not in row:
        return True

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
            "Could not parse %s value %r — defaulting to re-enrich",
            NETSUITE_LAST_ENRICHED_COL, val_str,
        )
        return True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_column(df: pd.DataFrame, col_name: str, required: bool = False) -> str:
    """Return the actual column name, trying case-insensitive match as fallback."""
    if col_name in df.columns:
        return col_name
    for c in df.columns:
        if c and str(c).strip().lower() == col_name.lower():
            return str(c).strip()
    if required:
        raise ValueError(
            f"Column '{col_name}' not found. Columns: {list(df.columns[:15])}..."
        )
    log.warning("Column '%s' not found; related features may fail.", col_name)
    return col_name
