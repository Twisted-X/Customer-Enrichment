"""
Company key normalisation and Places candidate selection.

Public API:
    normalize_company_key(row, company_col)                            -> str
    is_channel_row(raw_company)                                        -> bool
    pick_branch_candidate_for_row(candidates, city, state, zip_code)   -> tuple[dict|None, str]
    pick_places_result_for_company(candidates, branch_norms)           -> dict | None
    build_branch_norms(df)                                             -> dict[str, list]
"""
from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from ._config import (
    COMPANY_COL, CHANNEL_KEYWORDS,
    ACCEPT_UNVERIFIED_MATCH,
)
from ._address import normalize_address_for_match, address_match_confidence, address_matches
from ._places import clean_company_name

# Legal-entity suffixes stripped from the grouping key only (NOT from the
# display name used as the Places query). Prevents 'Academy' and 'Academy LTD'
# being treated as different companies while keeping the full name for Google.
_LEGAL_SUFFIX_RE = re.compile(
    r'[\s,]+(llc|ltd|l\.l\.c\.|inc|incorporated|corp|corporation|co\.|company)\s*$',
    re.IGNORECASE,
)


def normalize_company_key(row, company_col: str) -> str:
    """
    Build a stable deduplication key for grouping rows by company.

    - Strips branch/location suffixes via clean_company_name
    - Strips legal suffixes (LLC, LTD, Inc …)
    - Normalises '&' → 'and'
    - Collapses whitespace
    - Treats missing/NaN as blank (returns "")
    """
    v = row.get(company_col, "")
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    key = clean_company_name(str(v).strip()).lower()
    if key in ("nan", ""):
        return ""
    key = key.replace("&", "and")
    key = _LEGAL_SUFFIX_RE.sub("", key).strip().rstrip(",").strip()
    return " ".join(key.split())


def is_channel_row(raw_company: str) -> bool:
    """
    Return True if the company name ends with a channel suffix (e.g. '- ecommerce',
    '- online', '- web').

    Channel rows have no physical branch address — their address in the sheet
    is the HQ address, so normal address verification always fails. They skip
    verification and accept the top Places candidate directly.
    """
    parts = re.split(r'-\s+', str(raw_company), maxsplit=1)
    if len(parts) < 2:
        return False
    return parts[1].strip().lower() in CHANNEL_KEYWORDS


def pick_branch_candidate_for_row(
    candidates: list,
    city: str,
    state: str,
    zip_code: str,
) -> tuple:
    """
    Return (candidate, confidence) for the first candidate whose formattedAddress
    matches this row's city/state/zip, or (None, 'none') if no match.

    confidence: 'high' | 'medium' | 'low' | 'none'
    """
    city_n, state_n, zip_n = normalize_address_for_match(city, state, zip_code)
    for cand in candidates:
        fa   = cand.get("formatted_address") or ""
        conf = address_match_confidence(city_n, state_n, zip_n, fa)
        if conf != "none":
            return cand, conf
    return None, "none"


def pick_places_result_for_company(
    candidates: list,
    branch_norms: list,
) -> Optional[dict]:
    """
    Return the first Places candidate whose formatted_address matches any
    known branch norm for this company.

    Returns None when:
      - candidates is empty
      - branch_norms is empty (cannot verify without address data)
      - no candidate matches any known branch
    """
    if not candidates or not branch_norms:
        return None
    for cand in candidates:
        fa = cand.get("formatted_address") or ""
        if not fa:
            continue
        for c_city, c_state, c_zip in branch_norms:
            if address_matches(c_city, c_state, c_zip, fa):
                return cand
    return None


def build_branch_norms(df: pd.DataFrame) -> dict:
    """
    Build a dict of company_key → list of (city_norm, state_norm, zip5) tuples
    in a single O(n) pass over the DataFrame.

    Used to verify whether any Places candidate matches a known branch before
    accepting it as the company's resolved location.
    """
    norms: dict = {}
    for idx in df.index:
        r  = df.loc[idx]
        ck = normalize_company_key(r, COMPANY_COL)
        if not ck:
            continue
        city  = r.get("city",     "") if "city"     in df.columns else ""
        state = r.get("state",    "") if "state"    in df.columns else ""
        z     = r.get("zip code", "") if "zip code" in df.columns else ""
        trip  = normalize_address_for_match(city, state, z)
        if not (trip[0] or trip[1] or trip[2]):
            continue
        row_norms = norms.setdefault(ck, [])
        if trip not in row_norms:
            row_norms.append(trip)
    return norms
