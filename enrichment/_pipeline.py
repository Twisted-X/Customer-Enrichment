"""
Pipeline orchestration — the only module that calls all others in sequence.

run_pipeline() replaces the 495-line main() from url_enrichment_pipeline.py.
Each step is a named function call so the flow reads like a checklist.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Optional

import pandas as pd
from tqdm import tqdm

from ._config import (
    USE_SFTP, INPUT_FILE, OUTPUT_FILE,
    COMPANY_COL, URL_COL, ADDRESS_COLS,
    NETSUITE_LAST_ENRICHED_COL, ENRICHMENT_TTL_DAYS,
    PING_EXISTING_URLS, FILL_BLANK_WEBSITE_WHEN_MATCHED,
    ACCEPT_UNVERIFIED_MATCH, ENABLE_PRODUCT_CHECK, CHECK_WORKERS,
    PLACES_MAX_CANDIDATES, WEBSITE_NOT_FOUND_LABEL,
)
from ._url import is_url_blank_or_invalid, bulk_check_urls, classify_url, extract_root_domain
from ._places import find_places_candidates
from ._company import (
    normalize_company_key, is_channel_row,
    pick_branch_candidate_for_row, pick_places_result_for_company,
    build_branch_norms,
)
from ._retail import classify_retail_type
from ._product import check_product_signals, compute_online_sales_status
from ._io import (
    sftp_session, resolve_input_file, derive_output_filename,
    load_dataframe, save_output, upload_results, should_enrich,
)

log = logging.getLogger(__name__)

# Output columns cleared when a branch candidate is not found
_BRANCH_COLS_CLEAR = [
    "places_formatted_address", "places_national_phone", "found_maps_url",
    "places_regular_opening_hours", "places_rating", "places_user_rating_count",
    "matched_name", "places_business_status", "places_primary_type",
    "places_primary_type_display_name", "places_latitude", "places_longitude",
    "places_place_id", "match_confidence",
]

# All output columns — ensured present before the Places loop writes to them
_ALL_OUTPUT_COLS = [
    "url_check_status", "url_http_code", "found_url", "found_url_type", "found_root_domain",
    "found_maps_url", "enrichment_source", "address_match", "match_confidence", "matched_name",
    "places_business_status", "places_primary_type", "places_primary_type_display_name",
    "places_formatted_address", "places_national_phone",
    "places_rating", "places_user_rating_count", "places_regular_opening_hours",
    "places_latitude", "places_longitude", "places_place_id", "url_was_backfilled",
    "enrichment_run_date", "retail_type",
    "sells_anything", "sells_shoes", "sells_twisted_x", "online_sales_status",
]


def run_pipeline() -> None:
    """
    Full enrichment pipeline:
      1. Resolve input/output paths (SFTP or local)
      2. Load + normalise DataFrame
      3. Mark rows as fresh (skip) or stale (enrich)
      4. Ping URLs for health check
      5. Google Places lookup (company-level deduplication)
      6. Optional product check via /api/check
      7. Compute NetSuite online_sales_status
      8. Save CSV + JSON, upload to SFTP
    """
    input_path, output_path, remote_input_path, tmp_obj = _resolve_paths()

    df, is_csv = load_dataframe(input_path)
    today_iso  = date.today().isoformat()

    _init_output_columns(df)

    fresh_idx, enrich_idx = _partition_by_freshness(df)
    _tag_fresh_rows(df, fresh_idx)

    ping_df, already_alive_idx, broken_idx = _run_url_ping(df, enrich_idx)

    _log_ping_summary(df, enrich_idx, already_alive_idx, broken_idx)
    _write_alive_rows(df, ping_df, already_alive_idx, enrich_idx, today_iso)

    branch_norms = build_branch_norms(df)
    _run_places_loop(df, ping_df, broken_idx, already_alive_idx, branch_norms, today_iso)

    if ENABLE_PRODUCT_CHECK:
        _run_product_check(df)

    df["online_sales_status"] = df.apply(compute_online_sales_status, axis=1)

    _log_summary(df, output_path)
    json_path = save_output(df, output_path, is_csv)

    if USE_SFTP and remote_input_path:
        upload_results(output_path, json_path, remote_input_path)


# ---------------------------------------------------------------------------
# Step 1 — Resolve paths
# ---------------------------------------------------------------------------

def _resolve_paths() -> tuple:
    """
    Determine local input/output paths.
    In SFTP mode: downloads the oldest inbound CSV to a temp directory.
    In local mode: uses INPUT_FILE / OUTPUT_FILE env vars.

    Returns (input_path, output_path, remote_input_path | None, tmp_dir_obj | None).
    The tmp_dir_obj must stay alive for the duration of main() — it cleans up on GC.
    """
    if not USE_SFTP:
        return INPUT_FILE, OUTPUT_FILE, None, None

    log.info("Connecting to SFTP to resolve input file...")
    with sftp_session() as sftp:
        try:
            remote_input_path = resolve_input_file(sftp)
        except FileNotFoundError as exc:
            log.info(str(exc))
            sys.exit(0)  # empty inbound — exit cleanly

        remote_output_filename = derive_output_filename(remote_input_path)
        tmp_obj       = tempfile.TemporaryDirectory(prefix="twx_enrich_")
        tmp_dir       = tmp_obj.name
        local_input   = os.path.join(tmp_dir, os.path.basename(remote_input_path))
        local_output  = os.path.join(tmp_dir, remote_output_filename)

        log.info("Downloading %s from SFTP...", remote_input_path)
        sftp.get(remote_input_path, local_input)

    return local_input, local_output, remote_input_path, tmp_obj


# ---------------------------------------------------------------------------
# Step 2 — Initialise output columns
# ---------------------------------------------------------------------------

def _init_output_columns(df: pd.DataFrame) -> None:
    for col in _ALL_OUTPUT_COLS:
        if col not in df.columns:
            df[col] = None
    df["url_was_backfilled"] = False
    df["enrichment_run_date"] = ""


# ---------------------------------------------------------------------------
# Step 3 — Freshness partition
# ---------------------------------------------------------------------------

def _partition_by_freshness(df: pd.DataFrame) -> tuple:
    """Split row indices into fresh (skip) and stale (enrich)."""
    if NETSUITE_LAST_ENRICHED_COL in df.columns:
        fresh_mask = df.apply(lambda r: not should_enrich(r), axis=1)
        fresh_idx  = set(df.index[fresh_mask].tolist())
        enrich_idx = set(df.index[~fresh_mask].tolist())
        log.info("Rows skipped (fresh ≤ %d days): %d", ENRICHMENT_TTL_DAYS, len(fresh_idx))
        log.info("Rows to enrich (blank/stale):   %d", len(enrich_idx))
    else:
        fresh_idx  = set()
        enrich_idx = set(df.index.tolist())
        log.info("'%s' column absent — enriching all rows.", NETSUITE_LAST_ENRICHED_COL)
    return fresh_idx, enrich_idx


def _tag_fresh_rows(df: pd.DataFrame, fresh_idx: set) -> None:
    if fresh_idx:
        df.loc[list(fresh_idx), "enrichment_source"] = "skipped_fresh"


# ---------------------------------------------------------------------------
# Step 4 — URL ping
# ---------------------------------------------------------------------------

def _run_url_ping(df: pd.DataFrame, enrich_idx: set) -> tuple:
    """
    Ping URLs for all rows due for enrichment.
    Returns (ping_df, already_alive_idx, broken_idx).

    ping_df is a copy of df with _url_status / _final_url / _http_code columns.
    already_alive_idx: rows with a live URL (active/redirected/blocked).
    broken_idx:        rows with dead/missing URLs that need Places lookup.
    """
    enrich_list = sorted(enrich_idx)
    ping_df     = df.copy()

    if not PING_EXISTING_URLS:
        ping_df["_url_status"] = "missing"
        ping_df["_final_url"]  = None
        ping_df["_http_code"]  = None
        is_blank = df.apply(lambda r: is_url_blank_or_invalid(r.get(URL_COL)), axis=1)
        broken_idx = [i for i in df.index[is_blank] if i in enrich_idx]
        return ping_df, [], broken_idx

    log.info("Step 1: Pinging %d URLs...", len(enrich_list))
    urls_to_ping  = df.loc[enrich_list, URL_COL].fillna("").astype(str).tolist()
    check_results = asyncio.run(bulk_check_urls(urls_to_ping))

    for i, idx in enumerate(enrich_list):
        ping_df.at[idx, "_url_status"] = check_results[i]["status"]
        ping_df.at[idx, "_final_url"]  = check_results[i]["final_url"]
        ping_df.at[idx, "_http_code"]  = check_results[i]["http_code"]

    alive_mask        = ping_df["_url_status"].isin(["active", "redirected", "blocked"])
    already_alive_idx = [i for i in ping_df.index[alive_mask] if i in enrich_idx]
    broken_idx        = [i for i in enrich_list if i not in already_alive_idx]
    return ping_df, already_alive_idx, broken_idx


def _log_ping_summary(df, enrich_idx, already_alive_idx, broken_idx) -> None:
    enrich_series = df.index.isin(enrich_idx)
    is_blank      = df.apply(lambda r: is_url_blank_or_invalid(r.get(URL_COL)), axis=1)
    n_blank       = int((is_blank & enrich_series).sum())
    log.info("Blank/invalid URLs:    %d", n_blank)
    log.info("Dead/broken URLs:      %d", len(broken_idx) - n_blank)
    log.info("Alive URLs:            %d", len(already_alive_idx))
    log.info("Total rows → Places:   %d", len(broken_idx) + len(already_alive_idx))


def _write_alive_rows(df, ping_df, already_alive_idx, enrich_idx, today_iso) -> None:
    """Write ping results for rows whose URLs are alive."""
    had_valid_url = {
        i for i in enrich_idx
        if not is_url_blank_or_invalid(df.at[i, URL_COL] if URL_COL in df.columns else "")
    }
    for idx in already_alive_idx:
        row       = ping_df.loc[idx]
        status    = row.get("_url_status", "active")
        final_url = row.get("_final_url") or row.get(URL_COL)
        http_code = row.get("_http_code")

        df.at[idx, "url_check_status"]  = status
        df.at[idx, "url_http_code"]     = str(http_code) if http_code is not None else ""
        df.at[idx, "found_url"]         = final_url
        df.at[idx, "found_url_type"]    = classify_url(final_url)
        df.at[idx, "found_root_domain"] = extract_root_domain(final_url)
        df.at[idx, "enrichment_source"] = "url_alive"
        if idx not in had_valid_url:
            df.at[idx, "enrichment_run_date"] = today_iso


# ---------------------------------------------------------------------------
# Step 5 — Google Places loop
# ---------------------------------------------------------------------------

def _run_places_loop(
    df: pd.DataFrame,
    ping_df: pd.DataFrame,
    broken_idx: list,
    already_alive_idx: list,
    branch_norms: dict,
    today_iso: str,
) -> None:
    """
    For each unique company, call Google Places once, then write results to
    every row belonging to that company.
    """
    already_alive_set = set(already_alive_idx)

    # Group rows by company key (one Places call per company)
    company_to_indices: dict = {}
    for idx in broken_idx + already_alive_idx:
        if idx not in df.index:
            continue
        r  = df.loc[idx]
        ck = normalize_company_key(r, COMPANY_COL)
        if not ck:
            _write_no_company_row(df, ping_df, idx)
            continue
        company_to_indices.setdefault(ck, []).append(idx)

    unique_companies = list(company_to_indices.keys())
    log.info("Companies needing Places lookup: %d", len(unique_companies))

    def _coerce(val):
        return val if val is not None else ""

    for ck in tqdm(unique_companies, desc="  Google Places lookup"):
        indices         = company_to_indices[ck]
        first_idx       = indices[0]
        company_display = df.loc[first_idx].get(COMPANY_COL, "")
        co_branch_norms = branch_norms.get(ck, [])

        hint_city, hint_state = _pick_location_hint(df, indices)

        candidates = find_places_candidates(
            company_display, hint_city, hint_state, "",
            max_result_count=PLACES_MAX_CANDIDATES,
        )

        # Global fallback: if location hint caused Google to return a result
        # with no website, search again without the hint to find the main site.
        local_website = _best_website(candidates) if candidates else ""
        if not local_website and (hint_city or hint_state):
            global_cands  = find_places_candidates(
                company_display, "", "", "",
                max_result_count=PLACES_MAX_CANDIDATES,
            )
            global_website = _best_website(global_cands) if global_cands else ""
        else:
            global_website = ""
        resolved_website = local_website or global_website

        company_status, company_pick, found_url_value = _resolve_company(
            candidates, co_branch_norms, resolved_website,
        )

        for idx in indices:
            row          = df.loc[idx]
            row_channel  = is_channel_row(str(row.get(COMPANY_COL, "")))
            ping_row     = ping_df.loc[idx] if idx in ping_df.index else {}

            _write_url_check_cols(df, idx, ping_row, idx in already_alive_set, found_url_value)
            branch, match_conf, enrichment_src = _resolve_row(
                row, idx, row_channel, company_status,
                candidates, found_url_value,
            )

            df.at[idx, "enrichment_source"]   = enrichment_src
            df.at[idx, "address_match"]       = branch is not None
            df.at[idx, "match_confidence"]    = match_conf
            df.at[idx, "enrichment_run_date"] = today_iso

            if branch:
                _write_branch_cols(df, idx, branch, _coerce)
            else:
                for col in _BRANCH_COLS_CLEAR:
                    df.at[idx, col] = ""

            df.at[idx, "retail_type"] = classify_retail_type(
                row_is_channel=row_channel,
                primary_type=df.at[idx, "places_primary_type"] or None,
                has_opening_hours=bool(df.at[idx, "places_regular_opening_hours"]),
            )

            _maybe_backfill_url(df, idx, URL_COL, enrichment_src, company_pick)

        time.sleep(0.05)  # gentle rate limit between companies


def _pick_location_hint(df: pd.DataFrame, indices: list) -> tuple:
    """Return (city, state) from the first row that has both."""
    for hi in indices:
        r = df.loc[hi]
        c = str(r.get("city",  "") or "").strip()
        s = str(r.get("state", "") or "").strip()
        if c and c.lower() not in ("nan", "") and s and s.lower() not in ("nan", ""):
            return c, s
        if c and c.lower() not in ("nan", ""):
            return c, ""
    return "", ""


def _best_website(candidates: list) -> str:
    """Root domain of the first candidate that has a website URL."""
    for cand in (candidates or []):
        w = (cand.get("website_url") or "").strip()
        if w:
            root = extract_root_domain(w)
            return root if root else w
    return ""


def _resolve_company(candidates, branch_norms: list, resolved_website: str) -> tuple:
    """
    Determine company-level resolution status, winning candidate, and found_url.
    Returns (status, company_pick, found_url_value).
    """
    if candidates is None:
        return "enrichment_error", None, WEBSITE_NOT_FOUND_LABEL
    if not candidates:
        return "not_found", None, WEBSITE_NOT_FOUND_LABEL
    if not branch_norms:
        if ACCEPT_UNVERIFIED_MATCH:
            found = resolved_website or WEBSITE_NOT_FOUND_LABEL
            return "unverified_match", candidates[0], found
        return "address_mismatch", None, WEBSITE_NOT_FOUND_LABEL

    company_pick = pick_places_result_for_company(candidates, branch_norms)
    if company_pick is None:
        return "address_mismatch", None, WEBSITE_NOT_FOUND_LABEL
    found = resolved_website or WEBSITE_NOT_FOUND_LABEL
    return "resolved", company_pick, found


def _resolve_row(row, idx, row_channel, company_status, candidates, found_url_value) -> tuple:
    """Determine per-row branch candidate, match confidence, and enrichment_source."""
    if company_status in ("not_found", "enrichment_error"):
        return None, "none", company_status

    if company_status == "address_mismatch" and not row_channel:
        return None, "none", "address_mismatch"

    # Channel row or unverified/resolved company — pick branch candidate
    if row_channel:
        branch = candidates[0] if candidates else None
        return branch, "none", "unverified_match"

    if company_status == "unverified_match":
        branch = candidates[0] if candidates else None
        return branch, "none", "unverified_match"

    # Normal verified path
    branch, match_conf = pick_branch_candidate_for_row(
        candidates,
        row.get("city", ""),
        row.get("state", ""),
        row.get("zip code", ""),
    )
    enrichment_src = "hybrid_full" if branch else "hybrid_website_only"
    return branch, match_conf, enrichment_src


def _write_url_check_cols(df, idx, ping_row, is_alive, found_url_value) -> None:
    http_code = ping_row.get("_http_code")
    df.at[idx, "url_check_status"] = ping_row.get("_url_status", "missing")
    df.at[idx, "url_http_code"]    = str(http_code) if http_code is not None else ""
    if not is_alive:
        df.at[idx, "found_url"]         = found_url_value
        df.at[idx, "found_url_type"]    = classify_url(found_url_value)
        df.at[idx, "found_root_domain"] = extract_root_domain(found_url_value)


def _write_branch_cols(df, idx, branch, coerce) -> None:
    df.at[idx, "places_formatted_address"]         = coerce(branch.get("formatted_address"))
    df.at[idx, "places_national_phone"]            = coerce(branch.get("national_phone_number"))
    df.at[idx, "found_maps_url"]                   = coerce(branch.get("maps_url"))
    df.at[idx, "places_regular_opening_hours"]     = coerce(branch.get("regular_opening_hours"))
    df.at[idx, "places_rating"]                    = coerce(branch.get("rating"))
    df.at[idx, "places_user_rating_count"]         = coerce(branch.get("user_rating_count"))
    df.at[idx, "matched_name"]                     = coerce(branch.get("matched_name"))
    df.at[idx, "places_business_status"]           = coerce(branch.get("business_status"))
    df.at[idx, "places_primary_type"]              = coerce(branch.get("primary_type"))
    df.at[idx, "places_primary_type_display_name"] = coerce(branch.get("primary_type_display_name"))
    df.at[idx, "places_latitude"]                  = coerce(branch.get("latitude"))
    df.at[idx, "places_longitude"]                 = coerce(branch.get("longitude"))
    df.at[idx, "places_place_id"]                  = coerce(branch.get("place_id"))


def _write_no_company_row(df, ping_df, idx) -> None:
    """Write a consistent failure row for rows with no company name."""
    ping_row = ping_df.loc[idx] if idx in ping_df.index else {}
    http_code = ping_row.get("_http_code")
    df.at[idx, "enrichment_source"]  = "not_found"
    df.at[idx, "found_url"]          = WEBSITE_NOT_FOUND_LABEL
    df.at[idx, "found_url_type"]     = "not_found"
    df.at[idx, "found_root_domain"]  = ""
    df.at[idx, "address_match"]      = False
    df.at[idx, "url_was_backfilled"] = False
    df.at[idx, "url_check_status"]   = ping_row.get("_url_status", "missing")
    df.at[idx, "url_http_code"]      = str(http_code) if http_code is not None else ""
    for col in _BRANCH_COLS_CLEAR:
        df.at[idx, col] = ""


def _maybe_backfill_url(df, idx, url_col, enrichment_src, company_pick) -> None:
    """Optionally write the found website URL back into the original URL column."""
    if not FILL_BLANK_WEBSITE_WHEN_MATCHED:
        return
    if enrichment_src not in ("hybrid_full", "hybrid_website_only", "unverified_match"):
        return
    if not (company_pick and company_pick.get("website_url")):
        return
    if not is_url_blank_or_invalid(df.at[idx, url_col]):
        return
    new_url = company_pick["website_url"]
    df.at[idx, url_col]             = new_url
    df.at[idx, "url_was_backfilled"] = True
    df.at[idx, "found_url_type"]    = classify_url(new_url)
    df.at[idx, "found_root_domain"] = extract_root_domain(new_url)


# ---------------------------------------------------------------------------
# Step 6 — Product check (optional)
# ---------------------------------------------------------------------------

def _run_product_check(df: pd.DataFrame) -> None:
    checkable = (
        df["found_url_type"].eq("website") &
        df["found_root_domain"].notna() &
        df["found_root_domain"].ne("")
    )
    unique_domains = df.loc[checkable, "found_root_domain"].unique()
    log.info("Product check: %d unique domains", len(unique_domains))

    domain_results: dict = {}
    with ThreadPoolExecutor(max_workers=CHECK_WORKERS) as pool:
        future_to_domain = {pool.submit(check_product_signals, d): d for d in unique_domains}
        for future in tqdm(as_completed(future_to_domain), total=len(unique_domains), desc="Product check"):
            domain_results[future_to_domain[future]] = future.result()

    for domain, signals in domain_results.items():
        mask = df["found_root_domain"].eq(domain)
        df.loc[mask, "sells_anything"]  = signals["sells_anything"]
        df.loc[mask, "sells_shoes"]     = signals["sells_shoes"]
        df.loc[mask, "sells_twisted_x"] = signals["sells_twisted_x"]


# ---------------------------------------------------------------------------
# Step 7 — Summary log
# ---------------------------------------------------------------------------

def _log_summary(df: pd.DataFrame, output_path: str) -> None:
    log.info("=" * 52)
    log.info("Saved %d rows → %s", len(df), output_path)
    log.info("=" * 52)
    if "enrichment_source" in df.columns:
        for status, count in df["enrichment_source"].fillna("(none)").value_counts().items():
            log.info("  %-28s %6d", status, count)
    log.info("=" * 52)
