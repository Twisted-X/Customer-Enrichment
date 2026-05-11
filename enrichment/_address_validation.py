"""
Google Address Validation API + Places Details API wrappers.

Used by _enrich_single.py as the primary enrichment path:
  1. validate_address()  → place_id (if address resolves to a known place)
  2. fetch_place_details() → full place dict (same shape as Text Search results)

Return convention (both functions):
  (result_dict, "")          — success; caller inspects result_dict["place_id"]
  (None, error_code)         — API/network failure; error_code is one of:
                               "timeout" | "quota" | "upstream_5xx" | "parse_error"

Returning the error code in the tuple (rather than just None) means callers
get exact observability even though exceptions are swallowed here.

GCP prerequisites (must be enabled before these calls work):
  - Address Validation API  (addressvalidation.googleapis.com)
  - Places API (New)        (places.googleapis.com)
"""
from __future__ import annotations

import logging
import time

import requests

from ._config import (
    GOOGLE_PLACES_API_KEY,
    ADDRESS_VALIDATION_URL,
    ADDRESS_VALIDATION_TIMEOUT,
    PLACES_DETAILS_URL,
    PLACES_DETAILS_FIELD_MASK,
    PLACES_DETAILS_TIMEOUT,
    PLACES_URL,
    FIELD_MASK,
    PLACES_REQUEST_TIMEOUT,
)
from ._places import _place_api_dict_to_result

log = logging.getLogger(__name__)

# Retry once on rate-limit or transient server errors.
_RETRYABLE_STATUS = {429, 503}


def _http_error_code(exc: requests.HTTPError) -> str:
    """Map an HTTPError to a standardised error code."""
    status = getattr(exc.response, "status_code", 0)
    return "quota" if status == 429 else "upstream_5xx"


def validate_address(
    address: str,
    city: str,
    state: str,
    zip_code: str,
) -> tuple[dict | None, str]:
    """
    POST to the Google Address Validation API.

    Returns (result_dict, "") on success:
        result_dict = {
            "place_id":          str | None,   # non-null when address resolves to a known place
            "formatted_address": str,
            "is_business":       bool,          # logged only — NEVER used for routing decisions
        }
    Returns (None, error_code) on any network / API failure.

    Note: a valid address that Google cannot resolve to a specific place returns
    ({"place_id": None, ...}, "") — success with no place_id, not an error.
    """
    payload = {
        "address": {
            "addressLines":       [address],
            "locality":           city,
            "administrativeArea": state,
            "postalCode":         zip_code,
            "regionCode":         "US",
        }
    }
    headers = {
        "Content-Type":   "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
    }

    error_code = "upstream_5xx"  # default; overwritten on specific exceptions

    for attempt in range(2):
        try:
            resp = requests.post(
                ADDRESS_VALIDATION_URL,
                json=payload,
                headers=headers,
                timeout=ADDRESS_VALIDATION_TIMEOUT,
            )
            if resp.status_code in _RETRYABLE_STATUS and attempt == 0:
                log.debug("Address Validation API %s — retrying in 1s", resp.status_code)
                time.sleep(1)
                continue
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                log.warning("Address Validation API error body: %s", data["error"])
                return (None, "upstream_5xx")

            result   = data.get("result", {})
            geocode  = result.get("geocode", {}) or {}
            metadata = result.get("metadata", {}) or {}

            place_id    = geocode.get("placeId")
            formatted   = result.get("address", {}).get("formattedAddress", "") or ""
            is_business = bool(metadata.get("business", False))

            # Extract coordinates from geocode.location so callers can use
            # them for location-biased search without a separate API call.
            location    = geocode.get("location") or {}
            latitude    = location.get("latitude")
            longitude   = location.get("longitude")

            log.debug(
                "Address Validation: place_id=%s is_business=%s lat=%s lng=%s city=%s state=%s",
                "present" if place_id else "absent",
                is_business,
                f"{latitude:.4f}" if latitude else "none",
                f"{longitude:.4f}" if longitude else "none",
                city.upper(),
                state.upper(),
            )
            return ({
                "place_id":          place_id,
                "formatted_address": formatted,
                "is_business":       is_business,
                "latitude":          latitude,
                "longitude":         longitude,
            }, "")

        except requests.Timeout:
            error_code = "timeout"
            log.warning("Address Validation API timeout (attempt %d) city=%s state=%s", attempt + 1, city, state)
            if attempt == 0:
                continue
            break
        except requests.HTTPError as exc:
            error_code = _http_error_code(exc)
            log.warning("Address Validation API HTTP error %s city=%s state=%s", error_code, city, state)
            break
        except (ValueError, requests.exceptions.JSONDecodeError):
            error_code = "parse_error"
            log.warning("Address Validation API parse error city=%s state=%s", city, state)
            break
        except requests.RequestException as exc:
            error_code = "upstream_5xx"
            log.warning("Address Validation API request error: %s", exc)
            break

    return (None, error_code)


def fetch_place_details(place_id: str) -> tuple[dict | None, str]:
    """
    GET place details from the Places API (New) by place_id.

    Passes the raw API response directly to _place_api_dict_to_result() so the
    returned dict has the exact same shape as Text Search candidates.

    Returns (result_dict, "") on success, (None, error_code) on failure.
    """
    url = PLACES_DETAILS_URL.format(place_id=place_id)
    headers = {
        "X-Goog-Api-Key":   GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": PLACES_DETAILS_FIELD_MASK,
    }

    error_code = "upstream_5xx"

    for attempt in range(2):
        try:
            resp = requests.get(
                url,
                headers=headers,
                timeout=PLACES_DETAILS_TIMEOUT,
            )
            if resp.status_code in _RETRYABLE_STATUS and attempt == 0:
                log.debug("Places Details API %s — retrying in 1s", resp.status_code)
                time.sleep(1)
                continue
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                log.warning("Places Details API error body: %s", data["error"])
                return (None, "upstream_5xx")
            return (_place_api_dict_to_result(data), "")

        except requests.Timeout:
            error_code = "timeout"
            log.warning("Places Details API timeout (attempt %d) place_id=%.16s", attempt + 1, place_id)
            if attempt == 0:
                continue
            break
        except requests.HTTPError as exc:
            error_code = _http_error_code(exc)
            log.warning("Places Details API HTTP error %s place_id=%.16s", error_code, place_id)
            break
        except (ValueError, requests.exceptions.JSONDecodeError):
            error_code = "parse_error"
            log.warning("Places Details API parse error place_id=%.16s", place_id)
            break
        except requests.RequestException as exc:
            error_code = "upstream_5xx"
            log.warning("Places Details API request error: %s", exc)
            break

    return (None, error_code)


def find_places_near_location(
    company: str,
    latitude: float,
    longitude: float,
    radius_meters: float = 300,
) -> tuple[list | None, str]:
    """
    Text Search biased toward a verified lat/lng from Address Validation.

    Uses the same PLACES_URL and FIELD_MASK as the existing text search path,
    but adds a locationBias circle so results are anchored to the exact address
    rather than the city/state string. This finds the specific branch at that
    address rather than any branch of the same chain.

    radius_meters: how tight to constrain the search. 300m ≈ one city block;
    increase to 500m for campuses or large-footprint stores.

    Returns (candidates_list, "") on success — list may be empty if no match.
    Returns (None, error_code) on network/API failure.
    """
    headers = {
        "Content-Type":     "application/json",
        "X-Goog-Api-Key":   GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,   # includes "places." prefix — Text Search format
    }
    payload = {
        "textQuery":      company,
        "maxResultCount": 5,
        "locationBias": {
            "circle": {
                "center": {"latitude": latitude, "longitude": longitude},
                "radius": radius_meters,
            }
        },
    }

    error_code = "upstream_5xx"

    try:
        resp = requests.post(
            PLACES_URL,
            json=payload,
            headers=headers,
            timeout=PLACES_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            log.warning("find_places_near_location API error body: %s", data["error"])
            return (None, "upstream_5xx")
        places = data.get("places", []) or []
        return ([_place_api_dict_to_result(p) for p in places], "")

    except requests.Timeout:
        error_code = "timeout"
        log.warning("find_places_near_location timeout lat=%.4f lng=%.4f", latitude, longitude)
    except requests.HTTPError as exc:
        error_code = _http_error_code(exc)
        log.warning("find_places_near_location HTTP error %s", error_code)
    except (ValueError, requests.exceptions.JSONDecodeError):
        error_code = "parse_error"
        log.warning("find_places_near_location parse error")
    except requests.RequestException as exc:
        error_code = "upstream_5xx"
        log.warning("find_places_near_location request error: %s", exc)

    return (None, error_code)
