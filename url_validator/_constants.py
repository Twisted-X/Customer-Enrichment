"""
Selector lists, phrase lists, and configuration constants for the URL validator.

Pure data — no Playwright imports. Edit here to tune selectors or thresholds
without touching any detection logic.
"""

# ── Timing / browser ──────────────────────────────────────────────────────

# Page-level timeout for interactive checks (search clicks, footwear detection).
# 20 s is the empirical sweet spot — long enough for slow shared-host sites,
# short enough that a hung site doesn't stall the whole batch.
TIMEOUT_MS = 20000

# Initial page-load timeout in check_url. Slightly tighter than TIMEOUT_MS so
# the validator fails fast on dead URLs; retries extend by +5 s each attempt.
VALIDATION_TIMEOUT = 18000

# Search results are considered "real" if rendered content grew by this ratio.
# 1.2 (20%) was chosen empirically: most search-result pages expand by 30%+,
# while no-op submissions tend to be flat or shrink slightly.
SEARCH_GROWTH_RATIO = 1.2

# Minimum delay between URL requests to reduce WAF trigger risk.
_RATE_LIMIT_S = 0.5

# Chrome desktop UA to reduce bot-detection blocks on retailer sites.
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# Generic brand words that only count as a TX hit when at least one
# non-generic brand indicator is also present on the same page.
_GENERIC_BRAND_WORDS = {"hooey"}


# ── Selector lists ────────────────────────────────────────────────────────
# Kept at module level so they can be reviewed, tuned, or extended in one place
# without hunting through function bodies.

_POPUP_CLOSE_SELECTORS = [
    '[class*="lightbox"] [class*="close"]',
    '[class*="modal"] [class*="close"]',
    '[class*="popup"] [class*="close"]',
    '[class*="overlay"] button',
    'button[aria-label="Close"]',
    'button[aria-label="close"]',
    '[class*="dismiss"]',
    '.close-button',
    '[data-dismiss]',
    'button:has-text("Close")',
    'button:has-text("No thanks")',
    'button:has-text("×")',
    '[class*="fb_lightbox"] button',
]

_SEARCH_INPUT_SELECTORS = [
    'input[type="search"]',
    'input[placeholder*="Search" i]',
    'input[placeholder*="search"]',
    '.chakra-input[type="search"]',
    'input[name="q"]',
    'input[name="s"]',                              # WooCommerce default
    'input[name="search"]',
    'input[name="query"]',
    'input[aria-label*="Search" i]',
    'input[aria-label*="search" i]',
    '#search',
    '#search-input',
    '#woocommerce-product-search-field',             # WooCommerce widget
    '.search-input',
    '.woocommerce-product-search input',             # WooCommerce search form
    'form.search-form input[type="search"]',         # WordPress/WooCommerce
    'form.search-form input[name="s"]',              # WordPress/WooCommerce
    'form[role="search"] input',                     # Accessibility-friendly
    '.dgwt-wcas-search-input',                       # AJAX Search for WooCommerce
    '#dgwt-wcas-search-input',                       # AJAX Search for WooCommerce
    '[class*="search-field"] input',
    '[class*="search"] input',
    '[class*="Search"] input',
]

_SEARCH_ICON_SELECTORS = [
    'button[aria-label*="Search" i]',
    'button[aria-label*="search" i]',
    '[class*="search-icon"]',
    '[class*="search-button"]',
    '[class*="search-toggle"]',                      # WooCommerce theme toggles
    '[class*="header-search"] a',                    # Theme header search triggers
    '[class*="header-search"] button',
    'a[href*="search"]',
    '.search-submit',                                # WordPress search submit
    'button[type="submit"][class*="search"]',
    'form.search-form button',                       # WordPress/WooCommerce
]

_PURCHASE_BUTTON_SELECTORS = [
    'button:has-text("Add to Cart")',
    'button:has-text("Add to Bag")',
    'button:has-text("Buy Now")',
    'button:has-text("Purchase")',
    'a:has-text("Add to Cart")',
    'a:has-text("Buy Now")',
    '[class*="add-to-cart"] button',
    '[class*="add-to-cart"] a',
    '[class*="buy-now"] button',
    '[id*="add-to-cart"]',
    '[id*="buy-now"]',
    'button[data-action="add-to-cart"]',
    'button[data-action="buy-now"]',
    # WooCommerce-specific
    'button.single_add_to_cart_button',
    '.add_to_cart_button',
    'a.add_to_cart_button',
    'button[name="add-to-cart"]',
    '.woocommerce-cart-form button',
    'a:has-text("Add to wishlist")',
    'button:has-text("Add to wishlist")',
    # Shopify-specific
    'button[name="add"]',
    'form[action*="/cart/add"] button',
]

_CART_SELECTORS = [
    'a[href*="cart"]',
    'a[href*="checkout"]',
    '[class*="cart"] a',
    '[class*="shopping-cart"]',
    '[id*="cart"]',
    'button:has-text("View Cart")',
    'a:has-text("Cart")',
    # WooCommerce-specific
    '.woocommerce-cart-form',
    'a[href*="wc-ajax"]',
    '.cart-contents',
    '.woocommerce-mini-cart',
    '[class*="woo"] [class*="cart"]',
    'a[href*="/cart/"]',
    '.cart-count',
    '.cart_totals',
]

# Phrases that explicitly indicate a site does NOT sell online.
# Intentionally narrow: broad phrases like 'store hours' appear on hybrid
# retailers that also sell online and would cause false negatives.
_ONLINE_BLOCKER_PHRASES = [
    'in-store only',
    'no online ordering',
    'call for availability',
    'contact us for pricing',
    'available in store only',
    'visit our store to purchase',
    'physical store only',
    'brick and mortar only',
]

_PHYSICAL_STORE_PHRASES = [
    'find a store', 'store locator', 'our locations', 'store locations',
    'visit us', 'visit our store', 'find a location', 'store hours',
    'locations', 'our stores',
]

# Any of these in a search-results body means the search found nothing.
_NO_RESULTS_PHRASES = [
    'no results', 'no results found', 'no results could be found',
    'did not match', 'no products found', 'no items found',
    '0 results', '0 items', 'nothing found',
    'no matches', 'could not find',
    'your search returned no', 'no search results',
    "we couldn't find", 'we could not find',
    'sorry, no results', "sorry, we couldn't find",
]

# Product container selectors used to scan headings and titles for brand names.
_PRODUCT_TITLE_SELECTORS = [
    '[class*="product"] [class*="title"]',
    '[class*="product"] [class*="name"]',
    '[class*="item"] [class*="title"]',
    'h2, h3, h4',
]

# Supported URL column names in input CSVs (tried in order).
_URL_COLUMN_CANDIDATES = ['website url', 'Web Address', 'url', 'URL', 'Website']
