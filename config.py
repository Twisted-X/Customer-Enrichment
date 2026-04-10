"""
Configuration for Twisted X Scraper API
"""
import os
import csv

# Playwright settings
HEADLESS = True
TIMEOUT_MS = 30000  # 30 seconds page load timeout

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Fallback retailer URLs (used when CSV is not available)
RETAILER_URLS = [
    "https://blueribboncountrystore.com/",
    "https://bluestemfarmandranch.com",
    "https://bobsmerch.com",
    "https://bomgaars.com",
    "https://bonypony.com",
    "https://bootamerica.com/",
    "https://bootbarn.com/",
    "https://bootbox.com/",
    "https://bootconnection.com",
    "https://bootcountryonline.com/",
    "https://bootjack.com/",
    "https://bootleggersfootwear.com/",
    "https://bootsandjeans.net",
    "https://bootsandmore.net/",
    "https://bootsblingcowboythings.com",
    "https://bootsforlessomaha.com",
    "https://bootsnbritchestx.com/",
    "https://bootvil.square.site/",
    "https://bootybrotherswestern.com",
    "https://bostonshoestoboots.com",
    "https://botasrojerowesternwear.com",
    "https://brayssaddlery.com",
    "https://brianfarmservice.com",
    "https://broadrivermercantile.com",
    "https://bromarion.com/",
    "https://broncotrading.co",
    "https://brownsshoefitco.com",
    "https://brownsshoes.com",
    "https://brutesafetywear.com",
    "https://buchheits.com/",
    "https://bullriderharlingen.com/",
    "https://burgershoes.com",
    "https://burkhartzmeyershoes.net",
    "https://burlapbagcos.com",
    "https://burrisfarmhome.com",
    "https://byrdswesternstore.com",
    "https://calfarley.org",
    "https://calliekays.com",
    "https://calranch.com",
    "https://cam-safety.com/",
    "https://capehornwesternwear.com/",
    "https://capitalcityshoes.com",
    "https://carrollsbootcountry.com",
    "https://casaraulww.com/",
    "https://cascademerchantile.com",
    "https://catalenahatters.com",
    "https://cattlemanswesternwear.com",
]


def get_retailer_name(url: str) -> str:
    """Extract retailer name from URL (e.g. 'https://www.bootbarn.com/' -> 'bootbarn')"""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "")
    name = domain.split(".")[0]
    return name


# ─── Twisted X SKU Fingerprint Database ─────────────────────────────────────
# Loaded once at import time. Used by /api/check for verification.
# Covers: Twisted X, Black Star, Tamarindo Footwear, Wrangler Footwear

def _load_style_codes() -> set:
    """
    Load unique ParentStyle codes from the full SKU xlsx (all sheets)
    with CSV fallback. These are product fingerprints for verification.
    Returns a set of uppercase style codes for O(1) lookup.
    """
    styles = set()

    # Primary source: full xlsx with all 4 sheets (~52K SKUs, ~3K styles)
    xlsx_path = os.path.join(DATA_DIR, "PowerAppsItemSearchDONOTEDITResults107 copy 2.xls.xlsx")
    if os.path.exists(xlsx_path):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(xlsx_path, read_only=True)
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                ps_idx = -1
                for i, row in enumerate(ws.iter_rows(values_only=True)):
                    if i == 0:
                        headers = list(row)
                        ps_idx = headers.index("ParentStyle") if "ParentStyle" in headers else -1
                        continue
                    if ps_idx >= 0 and row[ps_idx]:
                        style = str(row[ps_idx]).strip()
                        if ":" not in style and len(style) >= 4:
                            styles.add(style.upper())
            wb.close()
            print(f"[config] Loaded {len(styles)} style codes from xlsx ({len(wb.sheetnames)} sheets)")
            return styles
        except Exception as e:
            print(f"[config] WARNING: xlsx load failed ({e}), falling back to CSV")

    # Fallback: original CSV
    sku_csv = os.path.join(DATA_DIR, "twsited_x_sku.csv")
    if os.path.exists(sku_csv):
        try:
            with open(sku_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    style = row.get("ParentStyle", "").strip()
                    if style and ":" not in style and len(style) >= 4:
                        styles.add(style.upper())
            print(f"[config] Loaded {len(styles)} style codes from CSV (fallback)")
        except Exception as e:
            print(f"[config] WARNING: Failed to load SKU file: {e}")
    else:
        print(f"[config] WARNING: No SKU files found in {DATA_DIR}")

    return styles


TX_STYLE_CODES = _load_style_codes()
