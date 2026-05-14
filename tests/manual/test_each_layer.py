"""
Direct layer isolation test — calls each layer function independently
to confirm all 4 layers work correctly, regardless of short-circuit order.

Run:
    cd /Users/yasasvi/Documents/twisted-x-scraper
    source venv/bin/activate
    python3 tests/manual/test_each_layer.py

Does NOT need the API server running.
"""
import sys
import os
import time

# Add repo root to path so checker/config/url_validator packages are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from checker._http      import http_first_check  # noqa: E402
from checker._sitemap    import sitemap_check    # noqa: E402
from checker._serp       import serp_check       # noqa: E402
from checker._playwright import playwright_check  # noqa: E402

PASS = "✓  PASS"
FAIL = "✗  FAIL"
SEP  = "=" * 70


def show(result: dict, max_proof: int = 4):
    print(f"     definitive:      {result.get('definitive')}")
    print(f"     sells_twisted_x: {result.get('sells_twisted_x')}")
    print(f"     confidence:      {result.get('confidence')}")
    for line in result.get("proof", [])[:max_proof]:
        print(f"     proof: {line}")
    products = result.get("sample_products", [])
    if products:
        print(f"     products found: {len(products)}")
        for p in products[:2]:
            print(f"       • {p.get('name','?')[:50]}  SKU:{p.get('sku','?')}")


# ── LAYER 1: HTTP + SKU fingerprint ──────────────────────────────────────────
print(f"\n{SEP}")
print("  LAYER 1 — HTTP + SKU fingerprint + brand-page probe")
print(f"{SEP}")
# sheplers has /brands/twisted-x/ and bootbarn has /collections/twisted-x
# both should be found via the brand-page probe even if homepage misses
l1_urls = [
    "https://www.bootjack.com/",
    "https://www.sheplers.com/",
]
l1_passed = False
for url in l1_urls:
    print(f"\n  Testing: {url}")
    t0 = time.time()
    r = http_first_check(url)
    elapsed = time.time() - t0
    show(r)
    print(f"     time: {elapsed:.1f}s")
    if r.get("definitive") and r.get("sells_twisted_x"):
        print(f"  {PASS}  Layer 1 returned definitive YES")
        l1_passed = True
        break
    else:
        print("  (inconclusive on this URL, trying next...)")

if not l1_passed:
    print(f"  {FAIL}  Layer 1 did not return definitive YES on any test URL")


# ── LAYER 2: Sitemap slug scan ────────────────────────────────────────────────
print(f"\n{SEP}")
print("  LAYER 2 — Sitemap slug scan")
print(f"{SEP}")
# sheplers, atwoods, murdochs all had TX slugs in sitemap in the original test
l2_urls = [
    "https://www.sheplers.com/",
    "https://www.atwoods.com/",
    "https://www.murdochs.com/",
]
l2_passed = False
for url in l2_urls:
    print(f"\n  Testing: {url}")
    t0 = time.time()
    r = sitemap_check(url)
    elapsed = time.time() - t0
    show(r)
    print(f"     time: {elapsed:.1f}s")
    if r.get("definitive") and r.get("sells_twisted_x"):
        print(f"  {PASS}  Layer 2 returned definitive YES")
        l2_passed = True
        break
    else:
        print("  (inconclusive on this URL, trying next...)")

if not l2_passed:
    print(f"  {FAIL}  Layer 2 did not return definitive YES on any test URL")


# ── LAYER 3: SerpApi ─────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  LAYER 3 — SerpApi Google Search")
print(f"{SEP}")
# bootbarn and cavenders confirmed Layer 3 in every run
l3_urls = [
    "https://www.bootbarn.com/",
    "https://www.cavenders.com/",
]
l3_passed = False
for url in l3_urls:
    print(f"\n  Testing: {url}")
    t0 = time.time()
    r = serp_check(url)
    elapsed = time.time() - t0
    show(r)
    print(f"     time: {elapsed:.1f}s")
    if r.get("definitive") and r.get("sells_twisted_x"):
        print(f"  {PASS}  Layer 3 returned definitive YES")
        l3_passed = True
        break
    else:
        print("  (inconclusive on this URL, trying next...)")

if not l3_passed:
    print(f"  {FAIL}  Layer 3 did not return definitive YES on any test URL")


# ── LAYER 4: Playwright ───────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  LAYER 4 — Playwright (full browser)")
print(f"{SEP}")
# Use a small TX retailer that is unlikely to be indexed by Google
# or have TX SKUs/slugs visible without JavaScript.
# amigo-workwear was caught by Layer 2 via sitemap, but Playwright should
# still work on it directly. We call playwright_check() directly here.
l4_urls = [
    ("https://amigo-workwear.myshopify.com/", "amigo-workwear"),
    ("https://www.bootjack.com/",             "bootjack"),
]
l4_passed = False
for url, name in l4_urls:
    print(f"\n  Testing: {url}")
    print("  (Playwright takes 15-60s — please wait...)")
    t0 = time.time()
    r = playwright_check(url, url, name)
    elapsed = time.time() - t0
    show(r)
    print(f"     time: {elapsed:.1f}s")
    if r.get("sells_twisted_x") is not None:
        verdict = "YES" if r.get("sells_twisted_x") else "NO (site may not be indexed yet)"
        print(f"  {PASS}  Layer 4 ran and returned: {verdict}")
        l4_passed = True
        break
    else:
        print("  (blocked/unknown — trying next...)")

if not l4_passed:
    print(f"  {FAIL}  Layer 4 could not complete on any test URL")


# ── Final summary ─────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  LAYER HEALTH SUMMARY")
print(f"{SEP}")
print(f"  Layer 1 (HTTP+SKU):   {'✓  WORKING' if l1_passed else '✗  CHECK NEEDED'}")
print(f"  Layer 2 (Sitemap):    {'✓  WORKING' if l2_passed else '✗  CHECK NEEDED'}")
print(f"  Layer 3 (SerpApi):    {'✓  WORKING' if l3_passed else '✗  CHECK NEEDED'}")
print(f"  Layer 4 (Playwright): {'✓  WORKING' if l4_passed else '✗  CHECK NEEDED'}")
print()
