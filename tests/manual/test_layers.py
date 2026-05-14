"""
Manual layer coverage test — calls /api/check for 20 URLs and reports
which layer determined the result, with full proof.

Run:
    cd /Users/yasasvi/Documents/twisted-x-scraper
    source venv/bin/activate
    python3 tests/manual/test_layers.py

Requires API server running on localhost:8000.
"""
import time
import requests

API_URL = "http://localhost:8000/api/check"

URLS = [
    # --- Likely Layer 1 (SKU in HTML) ---
    "https://www.bootcountryonline.com/",
    "https://www.bomgaars.com/",
    "https://www.buchheits.com/",

    # --- Likely Layer 2 (sitemap slug) ---
    "https://www.sheplers.com/",
    "https://www.countryoutfitter.com/",
    "https://www.bootbay.com/",

    # --- Likely Layer 3 (SerpApi — bot-protected) ---
    "https://www.bootbarn.com/",
    "https://www.cavenders.com/",
    "https://www.pard.com/",

    # --- Likely Layer 4 (Playwright needed) ---
    "https://www.runningboardbootcamp.com/",
    "https://www.thecowboycollection.com/",
    "https://amigo-workwear.myshopify.com/",

    # --- Mix / uncertain ---
    "https://www.atwoods.com/",
    "https://www.bootjack.com/",
    "https://www.calranch.com/",
    "https://www.murdochs.com/",

    # --- Non-TX (expect NO) ---
    "https://www.nike.com/",
    "https://www.zara.com/",
    "https://www.target.com/",
    "https://www.homedepot.com/",
]

_LAYER_LABELS = {
    "layer1_http":      "Layer 1 (HTTP+SKU)",
    "layer2_sitemap":   "Layer 2 (Sitemap)",
    "layer3_serp":      "Layer 3 (SerpApi)",
    "layer4_playwright":"Layer 4 (Playwright)",
}


def fmt_bool(val) -> str:
    if val is True:
        return "YES"
    if val is False:
        return "NO"
    return "unknown"


def run():
    results = []
    layer_counts = {label: [] for label in _LAYER_LABELS.values()}

    print(f"\n{'='*80}")
    print(f"  TWISTED X LAYER COVERAGE TEST  —  {len(URLS)} URLs")
    print(f"{'='*80}\n")

    for i, url in enumerate(URLS, 1):
        print(f"[{i:02d}/{len(URLS)}] {url}")
        try:
            t0 = time.time()
            resp = requests.post(API_URL, json={"url": url}, timeout=120)
            elapsed = time.time() - t0
            data = resp.json()
        except Exception as exc:
            print(f"       ERROR: {exc}\n")
            continue

        raw_layer = data.get("detection_layer", "layer4_playwright")
        layer     = _LAYER_LABELS.get(raw_layer, f"Unknown ({raw_layer})")
        sells     = fmt_bool(data.get("sells_twisted_x"))
        online    = fmt_bool(data.get("sells_online"))
        confidence = data.get("confidence", "—")
        store_type = data.get("store_type", "—")
        blocked   = data.get("blocked", False)
        error     = data.get("error")
        products  = data.get("sample_products", [])

        print(f"       Layer:       {layer}")
        print(f"       Sells TX:    {sells}  |  Online: {online}  |  Confidence: {confidence}  |  Type: {store_type}")
        if blocked:
            print("       Blocked:     YES")
        if error:
            print(f"       Error:       {error}")
        if products:
            print(f"       Products:    {len(products)} found")
            for p in products[:2]:
                name = p.get("name", "?")[:60]
                sku  = p.get("sku", "?")
                price = p.get("price", "?")
                print(f"                    • {name}  SKU:{sku}  ${price}")
        if data.get("proof"):
            print("       Proof:")
            for line in data["proof"][:6]:
                print(f"                    {line}")
        print(f"       Time:        {elapsed:.1f}s\n")

        results.append({
            "url": url, "layer": layer, "sells_twisted_x": sells,
            "sells_online": online, "confidence": confidence,
            "store_type": store_type, "elapsed": round(elapsed, 1),
            "blocked": blocked, "products": len(products),
        })
        if layer in layer_counts:
            layer_counts[layer].append(url)

    # ── Layer coverage summary ────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  LAYER COVERAGE SUMMARY")
    print(f"{'='*80}")
    for label, urls in layer_counts.items():
        status = "✓" if urls else "✗ NOT HIT"
        print(f"  {status}  {label}: {len(urls)} URL(s)")
        for u in urls:
            print(f"           {u}")

    # ── Results table ─────────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  RESULTS TABLE")
    print(f"{'='*80}")
    header = f"{'URL':<45} {'Layer':<24} {'TX':>4} {'Online':>6} {'Time':>6}s"
    print(header)
    print("-" * len(header))
    for r in results:
        short_url = r["url"].replace("https://", "").replace("www.", "").rstrip("/")[:44]
        print(f"{short_url:<45} {r['layer']:<24} {r['sells_twisted_x']:>4} {r['sells_online']:>6} {r['elapsed']:>6.1f}")

    unhit = [label for label, urls in layer_counts.items() if not urls]
    if unhit:
        print(f"\n⚠  LAYERS NOT HIT: {', '.join(unhit)}")
        print("   Add URLs more likely to be caught by those layers.")
    else:
        print("\n✓  All 4 layers were exercised.")


if __name__ == "__main__":
    run()
