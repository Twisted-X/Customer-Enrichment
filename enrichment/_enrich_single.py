"""
Single-record enrichment orchestration.

Public API:
    enrich_single_customer(company, address, city, state, zip_code,
                           current_url=None, internal_id=None) -> dict

Lookup flow:
  Step 1 — Google Address Validation API → placeId
  Step 2 — Places Details API (primary path, only when placeId found)
  Step 3 — Text Search fallback (existing pipeline code)
  Step 4 — address_match_confidence on winning candidate

The returned dict maps directly to all fields of EnrichResponse in models.py.

Logging contract (PII-safe):
  - Logs: enrichment_source, match_confidence, latency_ms, city, state, error (when set)
  - Never logs: company name, street address, internal_id
  - Error codes: timeout | quota | upstream_5xx | parse_error
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

from ._address import address_match_confidence, normalize_address_for_match
from ._address_validation import find_places_near_location, validate_address
from ._company import pick_branch_candidate_for_row
from ._places import find_places_candidates

log = logging.getLogger(__name__)

# Regex for PO box detection (case-insensitive).
_PO_BOX_RE = re.compile(r'^\s*p\.?\s*o\.?\s*box\b', re.IGNORECASE)

# Regex for suite/unit/apt noise that confuses address validation.
_SUITE_NOISE_RE = re.compile(
    r',?\s*(?:suite|ste|unit|apt|apartment|#)\s*[\w-]+\s*$',
    re.IGNORECASE,
)


def _normalize_inputs(
    address: str, city: str, state: str, zip_code: str
) -> tuple[str, str, str, str]:
    """Strip ZIP+4, uppercase state, strip suite/apt noise from address."""
    addr_clean = _SUITE_NOISE_RE.sub("", address.strip()).strip()
    city_clean  = city.strip()
    state_clean = state.strip().upper()
    zip_clean   = re.sub(r'\D', '', zip_code)[:5]
    return addr_clean, city_clean, state_clean, zip_clean


def _classify_error(exc: Exception) -> str:
    """
    Map an unexpected escaped exception to one of the four standardised error codes.

    Used only for exceptions that escape the API wrappers (should be rare since
    validate_address / find_places_near_location catch internally). The wrappers
    themselves now return the error code alongside None, so this function is a
    last-resort guard rather than the primary classification path.
    """
    import requests
    msg = str(exc).lower()
    if isinstance(exc, requests.Timeout):
        return "timeout"
    if isinstance(exc, requests.HTTPError):
        code = getattr(exc.response, "status_code", 0)
        return "quota" if code == 429 else "upstream_5xx"
    if "parse" in msg or "json" in msg or "decode" in msg:
        return "parse_error"
    return "upstream_5xx"


def _empty_result() -> dict:
    return {
        "found_url":                    None,
        "found_maps_url":               None,
        "matched_name":                 None,
        "places_place_id":              None,
        "places_formatted_address":     "",
        "places_national_phone":        "",
        "places_rating":                None,
        "places_regular_opening_hours": "",
        "places_latitude":              None,
        "places_longitude":             None,
        "places_business_status":       "",
        "places_primary_type":          "",
        "match_confidence":             "none",
        "enrichment_source":            "",
        "address_match":                False,
    }


def _candidate_to_result(candidate: dict, source: str, confidence: str) -> dict:
    """Build an EnrichResponse-compatible dict from a _place_api_dict_to_result candidate."""
    place_id = candidate.get("place_id")
    address_match = confidence != "none" and place_id is not None
    return {
        "found_url":                    candidate.get("website_url"),
        "found_maps_url":               candidate.get("maps_url"),
        "matched_name":                 candidate.get("matched_name"),
        "places_place_id":              place_id,
        "places_formatted_address":     candidate.get("formatted_address") or "",
        "places_national_phone":        candidate.get("national_phone_number") or "",
        "places_rating":                candidate.get("rating"),
        "places_regular_opening_hours": candidate.get("regular_opening_hours") or "",
        "places_latitude":              candidate.get("latitude"),
        "places_longitude":             candidate.get("longitude"),
        "places_business_status":       candidate.get("business_status") or "",
        "places_primary_type":          candidate.get("primary_type") or "",
        "match_confidence":             confidence,
        "enrichment_source":            source,
        "address_match":                address_match,
    }


def enrich_single_customer(
    company: str,
    address: str,
    city: str,
    state: str,
    zip_code: str,
    current_url: Optional[str] = None,
    internal_id: Optional[str] = None,
) -> dict:
    """
    Enrich a single customer record via Google Address Validation → Places Details
    (primary path) or Text Search (fallback).

    Returns a flat dict matching all fields of EnrichResponse.
    Never raises — errors are captured and reflected in enrichment_source.

    Parameters
    ----------
    company:     Company name (used only in Text Search fallback, never logged).
    address:     Street address line (e.g. "15776 N Greenway Hayden Loop").
    city:        City name.
    state:       2-letter state code (e.g. "AZ").
    zip_code:    5-digit zip (ZIP+4 accepted and stripped automatically).
    current_url: Existing website URL from NetSuite (passed through, unused here).
    internal_id: NetSuite internal ID (never logged).
    """
    t0 = time.monotonic()
    addr_clean, city_clean, state_clean, zip_clean = _normalize_inputs(
        address, city, state, zip_code
    )

    upstream_error: bool = False
    upstream_error_code: str = ""
    candidate: dict | None = None
    source: str = ""

    # ── Step 2: skip Address Validation for PO boxes ──────────────────────
    is_po_box = bool(_PO_BOX_RE.match(addr_clean))

    if not is_po_box:
        # ── Step 1: Address Validation API → lat/lng ──────────────────────
        # validate_address returns (dict, "") on success or (None, error_code).
        # is_business is logged inside the wrapper — NEVER used for routing.
        try:
            av_result, av_err = validate_address(addr_clean, city_clean, state_clean, zip_clean)
        except Exception as exc:
            av_result, av_err = None, _classify_error(exc)
            log.warning(
                "enrichment validate_address unexpected escape: %s city=%s state=%s",
                av_err, city_clean, state_clean,
            )

        if av_result is None:
            upstream_error = True
            upstream_error_code = av_err

        if av_result is not None:
            lat = av_result.get("latitude")
            lng = av_result.get("longitude")

            # ── Step 2: location-biased Text Search ───────────────────────
            # Address Validation gives us precise coordinates for the input
            # address. We use those to search for the business by name near
            # that exact point. This returns a ChIJ... business listing
            # (with website, phone, hours) for the specific branch — not a
            # random branch of the same chain in a different city.
            if lat and lng:
                try:
                    near_candidates, near_err = find_places_near_location(
                        company, lat, lng, radius_meters=300
                    )
                except Exception as exc:
                    near_candidates, near_err = None, _classify_error(exc)
                    log.warning(
                        "enrichment find_places_near_location unexpected escape: %s city=%s state=%s",
                        near_err, city_clean, state_clean,
                    )

                if near_candidates is None:
                    upstream_error = True
                    upstream_error_code = near_err
                elif near_candidates:
                    # Pick the closest/best matching candidate
                    near_pick, _ = pick_branch_candidate_for_row(
                        near_candidates, city_clean, state_clean, zip_clean
                    )
                    candidate = near_pick or near_candidates[0]
                    source = "address_validation"
                # near_candidates == [] means no business found near those
                # coordinates — fall through to Text Search fallback below

    # ── Step 3: Text Search fallback ──────────────────────────────────────
    if candidate is None:
        # find_places_candidates (from _places.py) swallows its own exceptions
        # and returns bare None — we can't recover the error code from it.
        # Unexpected escapes are caught below; None return is treated as
        # upstream_5xx since we have no further information.
        try:
            candidates = find_places_candidates(company, city_clean, state_clean, zip_clean)
        except Exception as exc:
            candidates = None
            ts_err = _classify_error(exc)
            log.warning(
                "enrichment find_places_candidates unexpected escape: %s city=%s state=%s",
                ts_err, city_clean, state_clean,
            )
            if not upstream_error:
                upstream_error = True
                upstream_error_code = ts_err

        if candidates is None:
            # None = API/network error swallowed inside find_places_candidates
            if not upstream_error:
                upstream_error = True
                upstream_error_code = "upstream_5xx"
        elif candidates:
            branch_candidate, _ = pick_branch_candidate_for_row(
                candidates, city_clean, state_clean, zip_clean
            )
            if branch_candidate:
                candidate = branch_candidate
                source = "text_search"
            else:
                # No address match but results exist; accept top result
                # (consistent with ACCEPT_UNVERIFIED_MATCH in pipeline).
                candidate = candidates[0]
                source = "text_search"

        if candidate is None:
            source = "enrichment_error" if upstream_error else "not_found"

    # ── Step 4: address_match_confidence ──────────────────────────────────
    if candidate is not None:
        city_n, state_n, zip_n = normalize_address_for_match(city_clean, state_clean, zip_clean)
        confidence = address_match_confidence(
            city_n, state_n, zip_n,
            candidate.get("formatted_address") or "",
        )
    else:
        confidence = "none"

    # ── Step 5: structured log (PII-safe) ─────────────────────────────────
    latency_ms = int((time.monotonic() - t0) * 1000)
    log_extra = dict(
        enrichment_source=source,
        match_confidence=confidence,
        latency_ms=latency_ms,
        city=city_clean.upper(),
        state=state_clean,
    )
    if upstream_error_code:
        log_extra["error"] = upstream_error_code
    log.info("enrichment %s", " ".join(f"{k}={v}" for k, v in log_extra.items()))

    # ── Build result ───────────────────────────────────────────────────────
    if candidate is not None:
        return _candidate_to_result(candidate, source, confidence)

    result = _empty_result()
    result["enrichment_source"] = source
    return result
