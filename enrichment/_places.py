"""
Google Places API integration.

Public API:
    find_places_candidates(company_name, city, state, zip_code, max_result_count) -> list | None
    find_on_google_places(company_name, city, state, zip_code)                    -> dict

    find_places_candidates returns:
      None  — API / network error (distinct from zero results)
      []    — success, no results found
      [...]  — list of normalised place dicts

    find_on_google_places is a single-result convenience wrapper used for
    direct lookups (e.g. one-off scripts). The pipeline calls
    find_places_candidates directly so it can score all candidates.
"""
from __future__ import annotations

import logging
import re

import pandas as pd
import requests

from ._config import (
    GOOGLE_PLACES_API_KEY,
    PLACES_URL, FIELD_MASK,
    PLACES_REQUEST_TIMEOUT,
    WEBSITE_NOT_FOUND_LABEL,
)

log = logging.getLogger(__name__)


def clean_company_name(name: str) -> str:
    """
    Strip branch / location suffixes for a cleaner Places search query.

    Splits on the FIRST '-' followed by whitespace:
      '601 Sports - Brookhaven'  → '601 Sports'
      'AG 1 Farmers Coop- HQ'   → 'AG 1 Farmers Coop'
      '4-D Western - HQ'         → '4-D Western'
    """
    if not name:
        return name
    return re.split(r'-\s+', name, maxsplit=1)[0].strip()


def _format_opening_hours(hours) -> str:
    """Convert regularOpeningHours payload to a readable string."""
    if not hours:
        return ""
    if isinstance(hours, dict) and "weekdayDescriptions" in hours:
        return "; ".join(hours.get("weekdayDescriptions", []))
    if isinstance(hours, list):
        return "; ".join(str(h) for h in hours[:7])
    return str(hours)


def _place_api_dict_to_result(place: dict) -> dict:
    """Map one Places API place object to the enrichment dict shape."""
    display_name = place.get("displayName", {})
    matched_name = (
        display_name.get("text")
        if isinstance(display_name, dict)
        else str(display_name)
    )

    primary_type_display = place.get("primaryTypeDisplayName", "")
    if isinstance(primary_type_display, dict):
        primary_type_display = primary_type_display.get("text", "") or ""

    website_url = place.get("websiteUri")
    maps_url    = place.get("googleMapsUri")
    source      = "website" if website_url else ("maps" if maps_url else "not_found")

    loc = place.get("location", {}) or {}

    return {
        "place_id":                    place.get("id"),
        "website_url":                 website_url,
        "maps_url":                    maps_url,
        "source":                      source,
        "matched_name":                matched_name,
        "formatted_address":           place.get("formattedAddress") or "",
        "business_status":             place.get("businessStatus", ""),
        "primary_type":                place.get("primaryType", ""),
        "primary_type_display_name":   primary_type_display,
        "national_phone_number":       place.get("nationalPhoneNumber") or "",
        "rating":                      place.get("rating"),
        "user_rating_count":           place.get("userRatingCount"),
        "regular_opening_hours":       _format_opening_hours(place.get("regularOpeningHours")),
        "latitude":                    loc.get("latitude"),
        "longitude":                   loc.get("longitude"),
    }


def find_places_candidates(
    company_name: str,
    city: str = "",
    state: str = "",
    zip_code: str = "",
    max_result_count: int = 20,
) -> list | None:
    """
    Text search Google Places. Returns up to max_result_count normalised place dicts.

    Returns None on API/network error so callers can distinguish from zero results.
    Company-level lookups pass no location so candidates can match any branch.
    """
    if not company_name or pd.isna(company_name):
        return []

    clean_name = clean_company_name(str(company_name))
    if city or state or zip_code:
        parts      = [clean_name, str(city).strip(), str(state).strip(), str(zip_code).strip()]
        text_query = ", ".join(p for p in parts if p)
    else:
        text_query = clean_name

    try:
        resp = requests.post(
            PLACES_URL,
            headers={
                "Content-Type":   "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": FIELD_MASK,
            },
            json={
                "textQuery":      text_query,
                "maxResultCount": min(max(1, max_result_count), 20),
                "languageCode":   "en",
            },
            timeout=PLACES_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise ValueError(f"Places API error body: {data['error']}")
        places = data.get("places", []) or []
        return [_place_api_dict_to_result(p) for p in places]

    except Exception as exc:
        log.warning("Google Places failed for '%s': %s", company_name, exc)
        return None  # None = API error; [] = success with zero results


def find_on_google_places(
    company_name: str,
    city: str = "",
    state: str = "",
    zip_code: str = "",
) -> dict:
    """
    Single-result convenience wrapper around find_places_candidates.

    Returns a normalised place dict. source="enrichment_error" when the API
    call failed; source="not_found" when it succeeded but returned no results.
    """
    cands = find_places_candidates(company_name, city, state, zip_code, max_result_count=1)
    if cands is None:
        out = _empty_places_result()
        out["source"] = "enrichment_error"
        return out
    if not cands:
        out = _empty_places_result()
        out["source"] = "not_found"
        return out
    return cands[0]


def _empty_places_result() -> dict:
    """Return a blank result dict for when Places finds nothing."""
    return {
        "place_id":                  None,
        "website_url":               None,
        "maps_url":                  None,
        "source":                    "not_found",
        "matched_name":              None,
        "formatted_address":         "",
        "business_status":           "",
        "primary_type":              "",
        "primary_type_display_name": "",
        "national_phone_number":     "",
        "rating":                    None,
        "user_rating_count":         None,
        "regular_opening_hours":     "",
        "latitude":                  None,
        "longitude":                 None,
    }
