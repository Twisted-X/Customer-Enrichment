"""
fill_phones.py
==============
Standalone script to fill missing phone numbers in the Locally.com enriched CSV.

Uses Google Places Text Search API (same key as the main pipeline).
Only fills rows where places_national_phone is blank AND the city/state
is a real store location (not a HQ placeholder address).

Usage:
    source venv/bin/activate
    python3 fill_phones.py \
        --input  "/path/to/Locallycom_Enriched.csv" \
        --output "/path/to/Locallycom_Enriched_WithPhones.csv"
"""

import argparse
import os
import re
import time
import logging

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")

# Addresses that indicate a HQ / corporate placeholder — not a real store location
HQ_ADDRESS_PATTERNS = [
    "11251 BEECH",   # Boot Barn corporate HQ, Fontana CA
]

PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"


def is_hq_address(address: str) -> bool:
    """Return True if the address looks like a corporate HQ placeholder."""
    addr = str(address or "").upper()
    return any(pat in addr for pat in HQ_ADDRESS_PATTERNS)


def strip_branch_suffix(name: str) -> str:
    """
    'Boot Barn Inc - 0460' → 'Boot Barn'
    'Bomgaars - 997'       → 'Bomgaars'
    'Alex Boots - Gateway' → 'Alex Boots'
    Keeps non-numeric suffixes only if they look like location names.
    """
    # Remove " - NNNN" (store number) patterns
    name = re.sub(r"\s*-\s*\d+\s*$", "", name).strip()
    # Remove " Inc", " LLC", " Ltd" etc.
    name = re.sub(r"\s+(Inc|LLC|Ltd|Co|Corp|HQ)\.?\s*$", "", name, flags=re.IGNORECASE).strip()
    return name


def places_text_search(query: str) -> dict | None:
    """
    Call Google Places Text Search API v1.
    Returns the first result dict or None.
    """
    if not PLACES_API_KEY:
        log.error("GOOGLE_PLACES_API_KEY not set in .env")
        return None

    resp = requests.post(
        PLACES_TEXT_SEARCH_URL,
        headers={
            "X-Goog-Api-Key": PLACES_API_KEY,
            "X-Goog-FieldMask": "places.displayName,places.nationalPhoneNumber,places.formattedAddress,places.rating",
            "Content-Type": "application/json",
        },
        json={"textQuery": query, "maxResultCount": 1},
        timeout=10,
    )

    if resp.status_code != 200:
        log.warning(f"Places API error {resp.status_code} for query '{query}': {resp.text[:200]}")
        return None

    data = resp.json()
    places = data.get("places", [])
    return places[0] if places else None


def clean_company_name(raw_name: str) -> str:
    """Get a clean searchable company name from the raw NetSuite name."""
    return strip_branch_suffix(str(raw_name or "").strip())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True, help="Path to enriched CSV")
    parser.add_argument("--output", required=True, help="Path to save updated CSV")
    args = parser.parse_args()

    if not PLACES_API_KEY:
        log.error("GOOGLE_PLACES_API_KEY not found in .env — aborting.")
        return

    df = pd.read_csv(args.input, dtype=str)
    log.info(f"Loaded {len(df):,} rows from {args.input}")

    # Rows missing phone
    missing_mask = df["places_national_phone"].isna() | (df["places_national_phone"].str.strip() == "")
    missing_idx  = df.index[missing_mask].tolist()
    log.info(f"Rows missing phone: {len(missing_idx)}")

    filled   = 0
    skipped  = 0
    not_found = 0

    for idx in tqdm(missing_idx, desc="Filling phones"):
        row     = df.loc[idx]
        address = str(row.get("address", "") or "")
        city    = str(row.get("city",    "") or "").strip()
        state   = str(row.get("state",   "") or "").strip()
        raw_name = str(row.get("Company", "") or "").strip()

        # Skip rows with HQ placeholder address — city/state is also wrong
        if is_hq_address(address):
            log.info(f"Skipping HQ-address row: {raw_name} ({city}, {state})")
            df.at[idx, "places_national_phone"] = "HQ address - store location unknown"
            skipped += 1
            continue

        if not city or not state:
            log.info(f"Skipping row with no city/state: {raw_name}")
            skipped += 1
            continue

        # Build search query: clean name + city + state
        clean_name = clean_company_name(raw_name)
        query      = f"{clean_name} {city} {state}"

        result = places_text_search(query)
        time.sleep(0.1)  # stay well within rate limits

        if result and result.get("nationalPhoneNumber"):
            phone = result["nationalPhoneNumber"]
            found_name = result.get("displayName", {}).get("text", "")
            log.info(f"✓ {raw_name} ({city},{state}) → {phone} [{found_name}]")
            df.at[idx, "places_national_phone"] = phone
            filled += 1
        else:
            log.info(f"✗ Not found: {query}")
            not_found += 1

    log.info(f"\n{'='*50}")
    log.info(f"Filled:     {filled}")
    log.info(f"HQ skipped: {skipped}")
    log.info(f"Not found:  {not_found}")
    log.info(f"{'='*50}")

    df.to_csv(args.output, index=False)
    log.info(f"Saved → {args.output}")


if __name__ == "__main__":
    main()
