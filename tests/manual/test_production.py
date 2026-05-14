"""
Production flow test — runs /api/check against real NetSuite customer URLs
drawn from url_validation_full_updated.csv (ground-truth labels).

Measures: accuracy, layer distribution, false positives, false negatives.

Run:
    cd /Users/yasasvi/Documents/twisted-x-scraper
    source venv/bin/activate
    python3 tests/manual/test_production.py

Requires API server running on localhost:8000.
"""
import csv
import time
import random
import requests

API_URL  = "http://localhost:8000/api/check"
DATA_CSV = "data/url_validation_full_updated.csv"

# ── Load ground-truth URLs from production CSV ────────────────────────────────
def load_urls(n_sellers=12, n_non_sellers=8, seed=42):
    sellers, non_sellers = [], []
    with open(DATA_CSV) as f:
        for row in csv.DictReader(f):
            url = row.get("normalized_url", "").strip()
            if not url.startswith("http"):
                continue
            if row.get("has_twisted_x") == "True" and row.get("sells_online") == "True":
                sellers.append(url)
            elif row.get("has_twisted_x") == "False":
                non_sellers.append(url)
    random.seed(seed)
    random.shuffle(sellers)
    random.shuffle(non_sellers)
    combined = (
        [(u, True)  for u in sellers[:n_sellers]] +
        [(u, False) for u in non_sellers[:n_non_sellers]]
    )
    random.shuffle(combined)
    return combined

_LAYER_LABELS = {
    "layer1_http":       "Layer 1 (HTTP+SKU)",
    "layer2_sitemap":    "Layer 2 (Sitemap)",
    "layer3_serp":       "Layer 3 (SerpApi)",
    "layer4_playwright": "Layer 4 (Playwright)",
}

def fmt_bool(val):
    if val is True:
        return "YES"
    if val is False:
        return "NO"
    return "unknown"


def run():
    urls = load_urls(n_sellers=12, n_non_sellers=8)
    total = len(urls)

    layer_counts   = {v: 0 for v in _LAYER_LABELS.values()}
    correct = wrong_fp = wrong_fn = errors = timeouts = 0
    rows = []

    print(f"\n{'='*90}")
    print(f"  PRODUCTION FLOW TEST  —  {total} real NetSuite URLs  ({12} sellers + {8} non-sellers)")
    print(f"{'='*90}\n")

    for i, (url, expected) in enumerate(urls, 1):
        exp_label = "YES" if expected else "NO "
        print(f"[{i:02d}/{total}] {url}")
        print(f"         Expected: {exp_label}")

        try:
            t0 = time.time()
            resp = requests.post(API_URL, json={"url": url}, timeout=120)
            elapsed = time.time() - t0
            data = resp.json()
        except requests.exceptions.Timeout:
            print("         TIMEOUT (>120s)\n")
            timeouts += 1
            errors += 1
            rows.append({"url": url, "expected": exp_label, "got": "TIMEOUT",
                         "layer": "—", "elapsed": 120.0, "result": "TIMEOUT"})
            continue
        except Exception as exc:
            print(f"         ERROR: {exc}\n")
            errors += 1
            rows.append({"url": url, "expected": exp_label, "got": "ERROR",
                         "layer": "—", "elapsed": 0, "result": "ERROR"})
            continue

        raw_layer  = data.get("detection_layer", "layer4_playwright")
        layer      = _LAYER_LABELS.get(raw_layer, raw_layer)
        sells      = data.get("sells_twisted_x")
        sells_fmt  = fmt_bool(sells)
        confidence = data.get("confidence", "—")
        blocked    = data.get("blocked", False)
        products   = data.get("sample_products", [])

        # Accuracy
        if sells is None:
            outcome = "UNKNOWN"
        elif sells == expected:
            outcome = "✓ CORRECT"
            correct += 1
        elif sells and not expected:
            outcome = "✗ FALSE POSITIVE"
            wrong_fp += 1
        else:
            outcome = "✗ FALSE NEGATIVE"
            wrong_fn += 1

        print(f"         Got:      {sells_fmt}  |  Layer: {layer}  |  Confidence: {confidence}  |  Time: {elapsed:.1f}s")
        print(f"         Outcome:  {outcome}")
        if blocked:
            print("         Blocked:  YES — manual check required")
        if products:
            print(f"         Products: {len(products)} found — {', '.join(p.get('sku','?') for p in products[:3])}")
        if data.get("proof"):
            for line in data["proof"][:3]:
                print(f"                   {line}")
        print()

        layer_counts[layer] = layer_counts.get(layer, 0) + 1
        rows.append({"url": url, "expected": exp_label, "got": sells_fmt,
                     "layer": layer, "elapsed": round(elapsed, 1), "result": outcome})

    # ── Summary ───────────────────────────────────────────────────────────────
    checked = total - errors
    accuracy = round(correct / checked * 100, 1) if checked else 0

    print(f"\n{'='*90}")
    print("  ACCURACY SUMMARY")
    print(f"{'='*90}")
    print(f"  Checked:         {checked}/{total}  ({timeouts} timeouts, {errors - timeouts} other errors)")
    print(f"  Correct:         {correct}  ({accuracy}%)")
    print(f"  False positives: {wrong_fp}  (said YES, actually NO)")
    print(f"  False negatives: {wrong_fn}  (said NO, actually YES)")

    print(f"\n{'='*90}")
    print("  LAYER DISTRIBUTION")
    print(f"{'='*90}")
    for label, count in layer_counts.items():
        bar = "█" * count
        print(f"  {label:<24} {bar} {count}")

    print(f"\n{'='*90}")
    print("  RESULTS TABLE")
    print(f"{'='*90}")
    header = f"{'URL':<45} {'Exp':>3} {'Got':>3} {'Layer':<24} {'Time':>6}s  Result"
    print(header)
    print("-" * len(header))
    for r in rows:
        short = r["url"].replace("https://","").replace("http://","").replace("www.","").rstrip("/")[:44]
        print(f"{short:<45} {r['expected']:>3} {r['got']:>3} {r['layer']:<24} {r['elapsed']:>6.1f}  {r['result']}")

    if wrong_fp:
        print("\n⚠  FALSE POSITIVES (investigate):")
        for r in rows:
            if "FALSE POSITIVE" in r["result"]:
                print(f"   {r['url']}")
    if wrong_fn:
        print("\n⚠  FALSE NEGATIVES (missed sellers):")
        for r in rows:
            if "FALSE NEGATIVE" in r["result"]:
                print(f"   {r['url']}")


if __name__ == "__main__":
    run()
