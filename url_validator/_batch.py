"""
Batch URL processing: read URLs from CSV, validate each, write results.

Functions
---------
_read_urls_from_csv(input_csv)          -> List[str]
_write_results(results, output_csv)     -> str   (returns filtered CSV path)
validate_urls(input_csv, output_csv)    -> dict
"""
import csv
import logging
import time
from typing import Dict, List

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from config import HEADLESS

from ._constants import _URL_COLUMN_CANDIDATES, _RATE_LIMIT_S, _BROWSER_UA
from ._brand import normalize_url
from ._check import check_url

log = logging.getLogger(__name__)


def _read_urls_from_csv(input_csv: str) -> List[str]:
    """Read non-empty URLs from the first recognised URL column in a CSV."""
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        url_col = next(
            (c for c in _URL_COLUMN_CANDIDATES if c in (reader.fieldnames or [])),
            None,
        )
        if url_col is None:
            raise ValueError(
                f"No URL column found in {input_csv}. "
                f"Expected one of: {_URL_COLUMN_CANDIDATES}. "
                f"Got: {reader.fieldnames}"
            )
        return [
            (row.get(url_col) or '').strip()
            for row in reader
            if (row.get(url_col) or '').strip()
        ]


def _write_results(results: List[Dict], output_csv: str) -> str:
    """
    Write full results to output_csv and a filtered CSV (online sellers only).
    Returns the path of the filtered CSV.
    """
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'original_url', 'normalized_url', 'final_url', 'redirected', 'status',
            'has_twisted_x', 'sells_online', 'twisted_x_method',
            'online_confidence', 'online_indicators', 'blockers', 'error',
        ])
        writer.writeheader()
        writer.writerows(results)

    filtered_csv = output_csv.replace('.csv', '_filtered_online_only.csv')
    with open(filtered_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Web Address'])
        for r in results:
            if r['status'] == 'has_products_sells_online' and r['normalized_url']:
                writer.writerow([r['normalized_url']])

    return filtered_csv


def validate_urls(input_csv: str, output_csv: str) -> Dict:
    """
    Validate all URLs in a CSV and write full + filtered result CSVs.

    Reads URLs from any recognised URL column, runs check_url on each via a
    reused Playwright browser context (stealth mode), and writes two files:
    - output_csv                 — full results for every URL
    - *_filtered_online_only.csv — only URLs confirmed to sell TX online

    Returns a summary dict with per-status counts and the full results list.
    """
    urls = _read_urls_from_csv(input_csv)
    log.info("Enhanced URL Validator — checking %d URLs for Twisted X + Online Sales", len(urls))

    results = []
    stats = {
        'total': len(urls),
        'invalid': 0,
        'has_products_sells_online': 0,
        'has_products_in_store_only': 0,
        'ecommerce_no_twisted_x': 0,
        'no_products_no_online': 0,
        'errors': 0,
    }

    # A fresh page is created per URL so cookies, localStorage, and auth
    # state from site A never bleed into site B. The context is reused for
    # efficiency; stealth is applied at the context level.
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=_BROWSER_UA,
        )
        Stealth().apply_stealth_sync(context)

        for i, url in enumerate(urls, 1):
            normalized = normalize_url(url)

            if not normalized:
                stats['invalid'] += 1
                results.append({
                    'original_url': url, 'normalized_url': None,
                    'status': 'invalid', 'has_twisted_x': False,
                    'sells_online': False, 'twisted_x_method': None,
                    'online_confidence': None, 'online_indicators': '',
                    'blockers': '', 'error': 'Invalid URL format',
                })
                log.warning("[%d/%d] Invalid URL: %s", i, stats['total'], url[:50])
                continue

            log.info("[%d/%d] Checking: %s", i, stats['total'], normalized[:60])

            page = context.new_page()
            try:
                check = check_url(normalized, page)
            finally:
                try:
                    page.close()
                except Exception:
                    pass

            status = check['combined_status']
            if status in stats:
                stats[status] += 1
            else:
                stats['errors'] += 1
            if check['error']:
                stats['errors'] += 1

            results.append({
                'original_url': url,
                'normalized_url': normalized,
                'final_url': check.get('final_url', normalized),
                'redirected': check.get('redirected', False),
                'status': status,
                'has_twisted_x': check['has_twisted_x'],
                'sells_online': check['sells_online'],
                'twisted_x_method': check['twisted_x_method'],
                'online_confidence': check['online_sales'].get('confidence'),
                'online_indicators': ', '.join(check['online_sales'].get('indicators', [])[:3]),
                'blockers': ', '.join(check['online_sales'].get('blockers', [])[:2]),
                'error': check['error'],
            })

            redirect_info = f" (-> {check.get('final_url', '')[:40]})" if check.get('redirected') else ""
            if status == 'has_products_sells_online':
                log.info("  -> Twisted X + Online Sales%s", redirect_info)
            elif status == 'has_products_in_store_only':
                log.info("  -> Twisted X (In-Store Only/Brand Site)%s", redirect_info)
            elif status == 'ecommerce_no_twisted_x':
                log.info("  -> E-commerce (No Twisted X)%s", redirect_info)
            elif check['error']:
                log.warning("  -> Error: %s", check['error'][:100])
            else:
                log.info("  -> No products%s", redirect_info)

            time.sleep(_RATE_LIMIT_S)

        browser.close()

    filtered_csv = _write_results(results, output_csv)

    log.info(
        "VALIDATION SUMMARY — total=%d invalid=%d online_sellers=%d in_store=%d "
        "ecomm_no_tx=%d no_products=%d errors=%d",
        stats['total'], stats['invalid'], stats['has_products_sells_online'],
        stats['has_products_in_store_only'], stats['ecommerce_no_twisted_x'],
        stats['no_products_no_online'], stats['errors'],
    )
    log.info("Full results:  %s", output_csv)
    log.info("Filtered list: %s  (only online sellers)", filtered_csv)

    return {**stats, 'results': results}
