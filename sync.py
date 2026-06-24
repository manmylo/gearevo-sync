"""
diag_inventory.py  —  READ-ONLY inventory diagnostic for Gearevo.

It does NOT touch Firestore, does NOT write anything, and does NOT change sync.py.
It just queries Shopify the same way sync.py does and shows you exactly where the
ending-inventory value goes, so you can see what makes up the gap vs Shopify Analytics.

HOW TO RUN (on your own machine):
    pip install requests
    export SHOPIFY_STORE=gearevo.myshopify.com     # same value as your GitHub secret
    export SHOPIFY_TOKEN=shpat_xxxxxxxxxxxxxxxx     # same value as your GitHub secret
    python diag_inventory.py

(Windows PowerShell: use  $env:SHOPIFY_STORE="..."  instead of export.)

WHAT TO COMPARE:
    COUNTED total            -> should match your dashboard (~RM 428,566.60)
    RAW total (no excludes)  -> should match Shopify's unfiltered figure (~RM 445,536.60)
    If RAW - COUNTED == your gap, the difference IS your exclusions (nothing is broken).
"""

import os
import requests

SHOPIFY_STORE = os.environ["SHOPIFY_STORE"]
SHOPIFY_TOKEN = os.environ["SHOPIFY_TOKEN"]

headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
}

# ── Keep these identical to sync.py so the test is faithful ──
EXCLUDED_TITLES = [
    'USED', 'Test', 'Hidden', 'Gearevo Kydex', 'PRE-ORDER', 'Gearevo Belt',
    'Servis Asah', 'Service Asah', 'Laser Engraving', 'T-Shirt',
    'Personalize Stylish', 'Gearevo Cap', 'Knife Sheath',
    'Kydex sheath for F. Herder',
]
EXCLUDED_KYDEX = [
    'GE-K3-', 'GE-K4-', 'GE-K5-', 'GE-K7-', 'GE-K8-', 'GE-K9-',
    'GE-K10-', 'GE-K11-', 'GE-K12-', 'GE-K14-', 'GE-K15-',
    'GE-K17-', 'GE-K19-', 'GE-K20-', 'GE-K21-',
    'GE-K26-', 'GE-K27-', 'GE-K34-',
]

GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-04/graphql.json"
INV_QUERY = """
query getProducts($cursor: String) {
  products(first: 50, after: $cursor, query: "status:active") {
    edges { node { title variants(first: 100) { edges { node {
      price inventoryQuantity inventoryItem { tracked }
    } } } } }
    pageInfo { hasNextPage endCursor }
  }
}
"""

counted_total = 0.0
title_excluded = []   # (title, value)
kydex_excluded = []   # (title, value)
untracked_total = 0.0
pages = 0
complete = True
cursor = None

print(f"Querying {SHOPIFY_STORE} ...\n")

while True:
    body = {"query": INV_QUERY, "variables": {"cursor": cursor} if cursor else {}}
    r = requests.post(GRAPHQL_URL, headers=headers, json=body)
    if r.status_code == 429:
        import time; time.sleep(float(r.headers.get("Retry-After", 2))); continue
    if r.status_code != 200:
        print(f"HTTP {r.status_code}: {r.text[:200]}"); complete = False; break
    data = r.json()
    if "errors" in data:
        print(f"GraphQL errors: {data['errors']}"); complete = False; break

    pd_ = data.get("data", {}).get("products", {})
    pages += 1
    for pe in pd_.get("edges", []):
        title = pe["node"]["title"]
        counted = 0.0
        untracked = 0.0
        for ve in pe["node"]["variants"]["edges"]:
            v = ve["node"]
            qty = int(v.get("inventoryQuantity") or 0)
            price = float(v.get("price") or 0)
            if not v.get("inventoryItem", {}).get("tracked", False):
                if qty > 0:
                    untracked += qty * price
                continue
            if qty < 1:
                continue
            counted += qty * price

        if any(ex in title for ex in EXCLUDED_TITLES):
            title_excluded.append((title, counted)); continue
        if any(code in title for code in EXCLUDED_KYDEX):
            kydex_excluded.append((title, counted)); continue

        counted_total += counted
        untracked_total += untracked

    page_info = pd_.get("pageInfo", {})
    if not page_info.get("hasNextPage"):
        break
    cursor = page_info.get("endCursor")

title_val = sum(v for _, v in title_excluded)
kydex_val = sum(v for _, v in kydex_excluded)
raw_total = counted_total + title_val + kydex_val

print("=" * 64)
print(f"  Pages fetched            : {pages}   (complete={complete})")
print("-" * 64)
print(f"  COUNTED  (your dashboard) : RM {counted_total:>14,.2f}")
print(f"  + excluded by TITLE list  : RM {title_val:>14,.2f}   ({len(title_excluded)} products)")
print(f"  + excluded by KYDEX list  : RM {kydex_val:>14,.2f}   ({len(kydex_excluded)} products)")
print(f"  RAW (counted + excluded)  : RM {raw_total:>14,.2f}   <- compare to Shopify")
print(f"  (untracked, not in either): RM {untracked_total:>14,.2f}")
print("=" * 64)
print(f"  GAP that exclusions explain: RM {title_val + kydex_val:>14,.2f}")
print("=" * 64)

print("\n  Excluded by TITLE list (title : value):")
for t, v in sorted(title_excluded, key=lambda x: -x[1]):
    print(f"    {v:>12,.2f}   {t[:60]}")
print("\n  Excluded by KYDEX list (title : value):")
for t, v in sorted(kydex_excluded, key=lambda x: -x[1]):
    print(f"    {v:>12,.2f}   {t[:60]}")
