"""
Address normalisation and matching for Google Places candidate verification.

Public API:
    normalize_zip(zip_val)                                          -> str
    normalize_address_for_match(city, state, zip_val)               -> tuple[str,str,str]
    parse_places_address(formatted_address)                         -> tuple[str,str,str]
    address_match_confidence(our_city, our_state, our_zip, places_formatted_address) -> str
    address_matches(our_city, our_state, our_zip, places_formatted_address)          -> bool
"""
from __future__ import annotations

import re

import pandas as pd


def normalize_zip(zip_val) -> str:
    """Extract first 5 digits from any zip code format."""
    if zip_val is None or (isinstance(zip_val, float) and pd.isna(zip_val)):
        return ""
    s = re.sub(r"\D", "", str(zip_val))
    return s[:5] if s else ""


def normalize_address_for_match(city, state, zip_val) -> tuple:
    """Return (city_norm, state_norm, zip5) uppercased for comparison."""
    def _n(s):
        if s is None or (isinstance(s, float) and pd.isna(s)):
            return ""
        return " ".join(str(s).strip().upper().split())

    return (_n(city), _n(state), normalize_zip(zip_val))


def parse_places_address(formatted_address: str) -> tuple:
    """
    Parse a Google Places formattedAddress into (city, state, zip5).

    Handles typical US formats:
      "Street, City, STATE ZIP"
      "Street, City, STATE ZIP, USA"
    Returns (city_norm, state_norm, zip5) — all uppercase strings.
    """
    if not formatted_address or not isinstance(formatted_address, str):
        return ("", "", "")
    parts = [p.strip() for p in formatted_address.split(",")]
    if len(parts) < 2:
        return ("", "", "")

    # Strip trailing country suffix so both 3-part and 4-part formats work the
    # same way: parts[-1] = "STATE ZIP", parts[-2] = "City".
    if parts[-1].strip().upper() in ("USA", "UNITED STATES", "US"):
        parts = parts[:-1]
    if len(parts) < 2:
        return ("", "", "")

    zip5  = ""
    state = ""
    city  = ""

    state_zip_part = parts[-1].strip().upper()
    if len(parts) >= 2:
        city = " ".join(parts[-2].strip().upper().split())

    for token in state_zip_part.split():
        digits = re.sub(r"\D", "", token)
        if len(digits) >= 5:
            zip5 = digits[:5]
        elif len(token) == 2 and token.isalpha():
            state = token

    if not zip5:
        for p in parts:
            for token in p.split():
                digits = re.sub(r"\D", "", token)
                if len(digits) >= 5:
                    zip5 = digits[:5]
                    break

    return (" ".join(city.split()), state, zip5)


def address_match_confidence(
    our_city: str, our_state: str, our_zip: str,
    places_formatted_address: str,
) -> str:
    """
    Return match confidence for a candidate address:
      'high'   — exact 5-digit zip match
      'medium' — same city, zips differ or one side absent
      'low'    — same state only (city absent/differs on one side)
      'none'   — no match, or no address data to compare

    State mismatch is always disqualifying.
    Handles HQ-vs-store case: 77449 HQ and 77450 store in same city get
    'medium' rather than failing on zip inequality.
    """
    if not our_city and not our_state and not our_zip:
        return "none"

    p_city, p_state, p_zip = parse_places_address(places_formatted_address)

    if our_state and p_state and our_state != p_state:
        return "none"

    if our_zip and p_zip and our_zip == p_zip:
        return "high"

    if our_city and p_city:
        return "medium" if our_city == p_city else "none"

    if our_state and p_state and our_state == p_state:
        return "low"

    return "none"


def address_matches(
    our_city: str, our_state: str, our_zip: str,
    places_formatted_address: str,
) -> bool:
    """True if address_match_confidence is not 'none'."""
    return address_match_confidence(
        our_city, our_state, our_zip, places_formatted_address
    ) != "none"
