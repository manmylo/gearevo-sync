import os
import json
import time
import sys
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta, date
import firebase_admin
from firebase_admin import credentials, firestore

# ---- Load secrets ----------------------------------------------------
SHOPIFY_STORE = os.environ["SHOPIFY_STORE"]
SHOPIFY_TOKEN = os.environ["SHOPIFY_TOKEN"]
FIREBASE_CREDS = json.loads(os.environ["FIREBASE_CREDENTIALS"])

# ---- Check run mode ----------------------------------------------------
# FULL_SYNC=1 -> historical Shopify backfill (push/manual only)
# Default (cron) -> today's Shopify + Excel sync for all rows
FULL_SYNC = os.environ.get("FULL_SYNC", "0") == "1"

# ---- Init Firebase ----------------------------------------------------
cred = credentials.Certificate(FIREBASE_CREDS)
firebase_admin.initialize_app(cred)
db = firestore.client()

headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}
# NOTE: ShopifyQL queries (compute_day_metrics below, and the inventory query
# further down) require the read_reports scope on this token/app, in addition
# to whatever order/product scopes it already has. If that scope isn't
# granted, every ShopifyQL call will fail -- check the Shopify custom app's
# API scopes if compute_day_metrics starts erroring after this change.
GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-04/graphql.json"

# ---- Malaysia timezone (UTC+8) ----------------------------------------------------
MY_TZ = timezone(timedelta(hours=8))
now_my = datetime.now(MY_TZ)
today_str = now_my.strftime("%Y-%m-%d")

start_my  = datetime(now_my.year, now_my.month, now_my.day, 0, 0, 0, tzinfo=MY_TZ)
start_utc = start_my.astimezone(timezone.utc)
end_utc   = now_my.astimezone(timezone.utc)
start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
end_str   = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"[DATE] {today_str} | Window: 12:00 AM -> {now_my.strftime('%I:%M:%S %p')} MYT")
print(f"[MODE] {'FULL SYNC (+ Shopify Backfill)' if FULL_SYNC else 'QUICK SYNC (today + Excel)'}")

# ---- STEP 1: Read Excel for today's row ----------------------------------------------------
# NOTE: dailyTarget is no longer read from Excel at all. The Calendar (CEO
# Dashboard) is now the sole source of truth for daily targets, written
# directly to Firestore client-side. This script only ever reads
# lastYearSale and dailyForecast from the Excel sheet.
last_year_sale  = 0.0
daily_forecast  = 0.0
excel_df        = None
date_col        = None
lastyear_col    = None
forecast_col    = None

try:
    excel_df = pd.read_excel("Sales_and_Target.xlsx")
    excel_df.columns = excel_df.columns.str.strip().str.lower().str.replace(" ", "_")

    date_col     = next((c for c in excel_df.columns if "date" in c), None)
    lastyear_col = next((c for c in excel_df.columns if "last" in c and "year" in c or "last_year" in c), None)
    forecast_col = next((c for c in excel_df.columns if "forecast" in c), None)

    print(f"   Excel columns  : {list(excel_df.columns)}")
    print(f"   Date col       : {date_col}")
    print(f"   Last year col  : {lastyear_col}")
    print(f"   Forecast col   : {forecast_col}")

    if date_col and lastyear_col:
        excel_df[date_col] = pd.to_datetime(excel_df[date_col], dayfirst=True, errors="coerce")
        today_dt = pd.Timestamp(now_my.year, now_my.month, now_my.day)
        row = excel_df[excel_df[date_col] == today_dt]

        if not row.empty:
            last_year_sale = float(row[lastyear_col].values[0]) if pd.notna(row[lastyear_col].values[0]) else 0.0

            if forecast_col:
                fval = row[forecast_col].values[0]
                daily_forecast = float(fval) if pd.notna(fval) and float(fval) > 0 else 0.0

            print(f"   [OK] Excel match : LastYear=RM{last_year_sale:.2f} | Forecast=RM{daily_forecast:.2f}")
        else:
            print(f"   [WARN] No row found for {today_str} in Excel -> using 0")
    else:
        print(f"   [ERROR] Could not find required columns in Excel")

except FileNotFoundError:
    print(f"   [WARN] Sales_and_Target.xlsx not found -> skipping Excel sync")
except Exception as e:
    print(f"   [ERROR] Excel read error: {e}")

# ---- Daily metrics helper ----------------------------------------------------
# Uses ShopifyQL (the `sales` table -- Shopify's actual sales ledger, the same
# thing that powers Shopify's own Analytics/Reports), NOT a reconstruction
# from Orders REST/GraphQL fields.
#
# Why: reconstructing "net sales" from Orders fields (subtotal_price, and a
# separate refunds[] scan) can only ever see two kinds of event -- an order's
# original creation, and formal refunds. It has NO way to see a THIRD kind of
# event: items added to an ALREADY-PLACED order via an edit/exchange. There is
# no Orders-API field for "a line item was added to this order today" at all.
# So any order that gets edited after its creation day -- a common real-world
# case (a customer exchanges a knife for a different model, say) -- was
# silently undercounted, no matter how the REST queries were tuned. This was
# root-caused directly against Shopify's own ShopifyQL "Net sales by order"
# report: one edited order accounted for the entire gap between this script's
# number and Shopify Analytics' number for the same day.
#
# ShopifyQL's `sales` table records each of the three event types (sale,
# refund, edit-addition) individually, each dated by when it actually
# happened -- exactly matching how Shopify's own dashboards attribute sales,
# and exactly the "count it on the day the transaction actually happened"
# principle this script was already using for refunds. This is the only way
# to get a number that matches Shopify Analytics exactly.
#
# Requires the read_reports scope on this Shopify app/token.
def fetch_shopifyql(ql_query):
    """Run one ShopifyQL query via the Admin GraphQL API. Returns the list of
    row dicts (one dict per row, keyed by column name), or None on error."""
    gql = {
        "query": """
            query($q: String!) {
              shopifyqlQuery(query: $q) {
                tableData { columns { name } rows }
                parseErrors
              }
            }
        """,
        "variables": {"q": ql_query},
    }
    resp = requests.post(GRAPHQL_URL, headers=headers, json=gql)
    if resp.status_code == 429:
        time.sleep(float(resp.headers.get("Retry-After", 2)))
        resp = requests.post(GRAPHQL_URL, headers=headers, json=gql)
    if resp.status_code != 200:
        print(f"   [ERROR] ShopifyQL HTTP error {resp.status_code}: {resp.text[:300]}")
        return None
    data = resp.json()
    if "errors" in data:
        print(f"   [ERROR] ShopifyQL GraphQL errors: {data['errors']}")
        return None
    result = data.get("data", {}).get("shopifyqlQuery") or {}
    if result.get("parseErrors"):
        print(f"   [ERROR] ShopifyQL parse errors: {result['parseErrors']} (query: {ql_query})")
        return None
    return result.get("tableData", {}).get("rows", [])


def fetch_channel_region_breakdown(day_start_my, day_end_my):
    """One MYT day's NET sales broken down by sales channel and shipping
    region, via plain Orders GraphQL -- NOT ShopifyQL.

    ShopifyQL's `sales_channel` dimension turned out to group by the
    connecting APP, not the per-order channel: on this store it lumped
    Shopee/TikTok/Lazada together under one bundled connector app's name
    ("Easy Shopee, TikTok & Lazada"), which doesn't match what the Shopify
    admin's own Orders list shows in its "Channel" column (a plain "Shopee"
    or "TikTok" per order). That per-order value lives at
    Order.channelInformation.channelDefinition.channelName -- an Orders API
    field, not exposed as a ShopifyQL dimension. Channel/shipping address
    don't change on an order edit, so the edit-tracking ShopifyQL was
    introduced for (see compute_day_metrics' docstring) isn't needed here --
    plain Orders GraphQL is both correct and simpler for this one.

    "Net" here means the same definitions the Worker/dashboard already use
    elsewhere for Returns/Cancelled (see fetchMonthOrderSummary): an order
    that was cancelled before ever shipping contributes nothing (it was
    never really a sale), and an order that shipped but was later refunded
    contributes its price MINUS the refunded amount. Without this, totals
    ran well above "Sales This Month" (which nets out refunds/cancellations
    via ShopifyQL's ledger) -- these two won't be dollar-exact (this is
    order-creation-dated, that's event-dated) but should land close.

    Orders come back sorted ascending by CREATED_AT, so once a page's order
    reaches day_end, every later page is also past it -- the loop breaks
    immediately instead of draining all the way to "now", which matters a
    lot for the historical backfill (one query per day, hundreds of days).

    Best-effort: returns ({}, {}) on failure rather than raising, so a
    breakdown hiccup never takes down the day's actual sales figure (the
    thing that matters most).
    """
    q = f"created_at:>={day_start_my.isoformat()} status:any"
    query = """
        query($cursor: String, $q: String) {
          orders(first: 50, after: $cursor, query: $q, sortKey: CREATED_AT) {
            pageInfo { hasNextPage endCursor }
            nodes {
              createdAt
              cancelledAt
              totalPriceSet { shopMoney { amount } }
              totalRefundedSet { shopMoney { amount } }
              fulfillments(first: 1) { id }
              channelInformation { channelDefinition { channelName } }
              shippingAddress { province }
            }
          }
        }
    """
    channels, regions = {}, {}
    cursor = None
    day_end_ms = day_end_my.timestamp() * 1000

    while True:
        resp = requests.post(GRAPHQL_URL, headers=headers,
                              json={"query": query, "variables": {"cursor": cursor, "q": q}})
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 2)))
            continue
        if resp.status_code != 200:
            print(f"   [ERROR] Channel/region Orders GraphQL error {resp.status_code}: {resp.text[:200]}")
            return channels, regions
        data = resp.json()
        if "errors" in data:
            print(f"   [ERROR] Channel/region Orders GraphQL errors: {data['errors']}")
            return channels, regions

        page = data.get("data", {}).get("orders", {})
        hit_end = False
        for o in page.get("nodes", []):
            created_ms = datetime.fromisoformat(o["createdAt"].replace("Z", "+00:00")).timestamp() * 1000
            if created_ms >= day_end_ms:
                hit_end = True
                break

            # Cancelled before ever shipping -- never a real sale, excluded
            # entirely (same rule "Cancelled This Month" uses elsewhere).
            shipped = len(o.get("fulfillments") or []) > 0
            if o.get("cancelledAt") and not shipped:
                continue

            total = float((o.get("totalPriceSet") or {}).get("shopMoney", {}).get("amount") or 0)
            refunded = float((o.get("totalRefundedSet") or {}).get("shopMoney", {}).get("amount") or 0)
            net = total - refunded

            ch = ((o.get("channelInformation") or {}).get("channelDefinition") or {}).get("channelName") or "Other"
            prov = (o.get("shippingAddress") or {}).get("province") or "Unknown"
            channels[ch] = channels.get(ch, 0.0) + net
            regions[prov] = regions.get(prov, 0.0) + net

        if hit_end or not page.get("pageInfo", {}).get("hasNextPage"):
            break
        cursor = page.get("pageInfo", {}).get("endCursor")

    return channels, regions


def compute_day_metrics(day_start_my, day_end_my):
    """One MYT calendar day's sales metrics via ShopifyQL. day_end_my is
    only used by the channel/region breakdown below -- ShopifyQL's own
    SINCE/UNTIL is date-only and always covers the full day in the store's
    configured timezone (confirmed set to GMT+08:00 Kuala Lumpur)."""
    date_str = day_start_my.strftime("%Y-%m-%d")
    rows = fetch_shopifyql(
        f"FROM sales SHOW net_sales, gross_sales, discounts, returns, orders "
        f"SINCE {date_str} UNTIL {date_str}"
    )
    if rows is None:
        return None

    channels, regions = fetch_channel_region_breakdown(day_start_my, day_end_my)

    if not rows:
        return {"net": 0.0, "gross": 0.0, "discounts": 0.0, "refunds": 0.0, "order_count": 0,
                "channels": channels, "regions": regions}

    row = rows[0]
    return {
        "net":         float(row.get("net_sales") or 0),
        "gross":       float(row.get("gross_sales") or 0),
        # discounts/returns come back as negative deltas (they're subtracted
        # into net_sales) -- store as positive magnitudes, matching how the
        # rest of this script (and its log output) already treats them.
        "discounts":   abs(float(row.get("discounts") or 0)),
        "refunds":     abs(float(row.get("returns") or 0)),
        "order_count": int(float(row.get("orders") or 0)),
        "channels":    channels,
        "regions":     regions,
    }


# ---- STEP 2: Fetch Shopify orders (today) ----------------------------------------------------
print(f"\n[FETCH] Fetching today's sales via ShopifyQL (the actual sales ledger)...")

day = compute_day_metrics(start_my, now_my)
if day is None:
    print("[ERROR] Could not fetch today's orders")
    exit(1)

current_sale    = day["net"]
gross_sale      = day["gross"]
total_returns   = day["refunds"]
total_orders    = day["order_count"]
total_discounts = day["discounts"]
today_channels  = day["channels"]
today_regions   = day["regions"]

print(f"   Orders         : {total_orders}")
print(f"   Refunds today  : RM{total_returns:.2f}  (dated by refund/edit processed date)")
print(f"   Channels       : {today_channels}")
print(f"   Regions        : {today_regions}")

# ---- STEP 2.5: Fetch Ending Inventory Retail Value ----------------------------------------------------
# Mirrors the ShopifyQL query used in Analytics:
#   FROM inventory
#   SHOW ending_inventory_retail_value
#   WHERE product_title NOT CONTAINS '...' (list below)
#   HAVING ending_inventory_units >= 1
#   DURING today
#
# Uses GraphQL (available on Grow plan) for paginated product fetch.
# Exclusion list is case-sensitive, matching ShopifyQL NOT CONTAINS behavior.
# Verified against Shopify Analytics JSONL export -- diff within ~RM 565 (real-time drift).

EXCLUDED_TITLES = [
    'USED',
    'Test',
    'Hidden',
    'Gearevo Kydex',
    'PRE-ORDER',
    'Gearevo Belt',
    'Servis Asah',
    'Service Asah',
    'Laser Engraving',
    'T-Shirt',
    'Personalize Stylish',
    'Gearevo Cap',
    'Knife Sheath',           # catches "18 inch Knife Sheath Made from Nylon" etc.
    'Kydex sheath for F. Herder',
]

# Kydex Sheaths stored at secondary location -- not counted by Shopify Analytics.
# 7 Kydex Sheaths ARE in Shopify (GE-K2, K6, K13, K16, K22, K32, K33) -- keep those.
# Your Shopify ShopifyQL query does NOT exclude these GE-K Kydex SKUs by title,
# so Shopify counts them. Leave empty to MATCH Shopify (~RM 16,540 of Kydex sheaths).
# To go back to excluding them, restore the SKU list below AND add matching
# `NOT CONTAINS 'GE-Kxx-'` lines to the ShopifyQL query so both stay in sync.
EXCLUDED_KYDEX = []
# EXCLUDED_KYDEX = [
#     'GE-K3-', 'GE-K4-', 'GE-K5-', 'GE-K7-', 'GE-K8-', 'GE-K9-',
#     'GE-K10-', 'GE-K11-', 'GE-K12-', 'GE-K14-', 'GE-K15-',
#     'GE-K17-', 'GE-K19-', 'GE-K20-', 'GE-K21-',
#     'GE-K26-', 'GE-K27-', 'GE-K34-',
# ]

INV_QUERY = """
query getProducts($cursor: String) {
  products(first: 50, after: $cursor, query: "status:active") {
    edges {
      node {
        title
        variants(first: 100) {
          edges {
            node {
              price
              inventoryQuantity
              inventoryItem {
                tracked
              }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

print(f"\n[FETCH] Fetching ending inventory retail value (GraphQL)...")
ending_inventory_retail_value = 0.0

try:
    inv_rows = []
    cursor = None
    pages = 0

    while True:
        variables = {"cursor": cursor} if cursor else {}
        inv_resp = requests.post(
            GRAPHQL_URL,
            headers=headers,
            json={"query": INV_QUERY, "variables": variables}
        )
        if inv_resp.status_code == 429:
            retry_after = float(inv_resp.headers.get("Retry-After", 2))
            print(f"   [WAIT] Rate limited -> waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        if inv_resp.status_code != 200:
            print(f"   [ERROR] GraphQL error {inv_resp.status_code}: {inv_resp.text[:200]}")
            break

        data = inv_resp.json()
        if "errors" in data:
            print(f"   [ERROR] GraphQL errors: {data['errors']}")
            break

        products_data = data.get("data", {}).get("products", {})
        edges = products_data.get("edges", [])
        page_info = products_data.get("pageInfo", {})
        pages += 1

        for pe in edges:
            title = pe["node"]["title"]
            # Skip excluded -- case-sensitive to match ShopifyQL NOT CONTAINS
            if any(ex in title for ex in EXCLUDED_TITLES):
                continue
            # Skip specific Kydex Sheaths at secondary location
            if any(code in title for code in EXCLUDED_KYDEX):
                continue
            for ve in pe["node"]["variants"]["edges"]:
                v = ve["node"]
                tracked = v.get("inventoryItem", {}).get("tracked", False)
                if not tracked:
                    continue
                qty = int(v.get("inventoryQuantity") or 0)
                if qty < 1:
                    continue
                price = float(v.get("price") or 0)
                value = qty * price
                ending_inventory_retail_value += value
                inv_rows.append((title, qty, price, value))

        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    # Print full inventory list sorted by value
    inv_rows.sort(key=lambda x: -x[3])
    print(f"\n   {'Product':<60} {'Qty':>6} {'Price':>10} {'Value':>12}")
    print(f"   {'-'*92}")
    for t, q, p, v in inv_rows:
        print(f"   {t[:60]:<60} {q:>6} {p:>10.2f} {v:>12.2f}")
    print(f"   {'-'*92}")
    print(f"   Pages fetched  : {pages}")
    print(f"   TOTAL: RM {ending_inventory_retail_value:,.2f}")
    print(f"\n   [OK] Ending Inventory Retail Value: RM{ending_inventory_retail_value:.2f}")

except Exception as e:
    print(f"   [ERROR] Inventory fetch error: {e}")

# ---- STEP 3: Daily summary ----------------------------------------------------
print(f"\n[SUMMARY]")
print(f"   Gross       : RM{gross_sale:.2f}")
print(f"   Discounts   : RM{total_discounts:.2f}")
print(f"   Returns     : -RM{total_returns:.2f}  -- refunds/edits dated today (ShopifyQL ledger)")
print(f"   Current     : RM{current_sale:.2f}  -- net sales via ShopifyQL (matches Shopify Analytics)")
print(f"   Orders      : {total_orders}")
print(f"   Last Year   : RM{last_year_sale:.2f}")
print(f"   Forecast    : RM{daily_forecast:.2f}")
print(f"   Inventory   : RM{ending_inventory_retail_value:.2f}")

updated_at = now_my.strftime("%H:%M:%S")

# ---- STEP 4: Push today to Firestore ----------------------------------------------------
# Batch write = 1 commit for 2 documents
# dailyTarget is intentionally never written here -- it's Calendar-only now
# (see note at Step 1). merge=True means this write never touches whatever
# dailyTarget value is already sitting on these docs.
today_data = {
    "currentSale":   float(f"{current_sale:.2f}"),
    "grossSale":     float(f"{gross_sale:.2f}"),
    "totalRefunds":  float(f"{total_returns:.2f}"),
    "totalOrders":   total_orders,
    "lastYearSale":  float(f"{last_year_sale:.2f}"),
    "dailyForecast": float(f"{daily_forecast:.2f}"),
    "endingInventory": float(f"{ending_inventory_retail_value:.2f}"),
    "channels":      today_channels,
    "regions":       today_regions,
    "syncedAt":      now_my.isoformat(),
    "source":        "shopify",
}

wb = db.batch()
wb.set(db.collection("sales").document("today"),
       {**today_data, "updatedAt": updated_at}, merge=True)
wb.set(db.collection("sales").document("daily").collection("days").document(today_str),
       {**today_data, "date": today_str}, merge=True)
wb.commit()

print(f"\n[OK] Firestore synced (today) -- 1 batch commit (2 docs)")
print(f"[RESULT] Gross RM{gross_sale:.2f} | Current RM{current_sale:.2f} | LY RM{last_year_sale:.2f} | Forecast RM{daily_forecast:.2f} | Orders {total_orders}")


# ---- Helper functions (used by sync request + backfill) ----------------------------------------------------

def excel_lookup(lookup_date):
    """Return (lastYearSale, dailyForecast) from the Excel DataFrame. No target -- Calendar-only now."""
    ly  = 0.0
    fc  = 0.0
    if excel_df is None or date_col is None or lastyear_col is None:
        return ly, fc

    dt = pd.Timestamp(lookup_date.year, lookup_date.month, lookup_date.day)
    row = excel_df[excel_df[date_col] == dt]

    if not row.empty:
        val = row[lastyear_col].values[0]
        ly = float(val) if pd.notna(val) else 0.0

        if forecast_col:
            fval = row[forecast_col].values[0]
            fc = float(fval) if pd.notna(fval) and float(fval) > 0 else 0.0
    return ly, fc


def fetch_shopify_orders_for_date(target_date):
    """One MYT day's metrics (net sales matches Shopify). Returns
    (net, gross, refunds, order_count, channels, regions)."""
    day_start_my = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=MY_TZ)
    day_end_my   = day_start_my + timedelta(days=1)
    m = compute_day_metrics(day_start_my, day_end_my)
    if m is None:
        return None
    return m["net"], m["gross"], m["refunds"], m["order_count"], m["channels"], m["regions"]


# ------------------------------------------------------------------------------
# ---- STEP 4B: Finalize yesterday (first sync of each day only) --------------
# Every other cron tick only ever computes TODAY's numbers (Step 2/4) --
# yesterday's figure is whatever the last tick of that day happened to
# produce, which can miss refunds/adjustments Shopify processes right around
# midnight. On the first sync of a new MYT day, re-fetch yesterday's FULL day
# from Shopify fresh and overwrite it with the authoritative final numbers.
# Gated by sales/syncState.lastYesterdayFinalizeDate so this only runs once
# per day, not on every cron tick (which would otherwise hammer Shopify for
# no reason). If the fetch fails, the gate is left untouched so the very next
# cron tick retries instead of silently giving up for the whole day.
# ------------------------------------------------------------------------------

try:
    sync_state_ref = db.collection("sales").document("syncState")
    sync_state = sync_state_ref.get()
    last_finalized = sync_state.to_dict().get("lastYesterdayFinalizeDate", "") if sync_state.exists else ""

    if last_finalized != today_str:
        yesterday_date = now_my.date() - timedelta(days=1)
        yesterday_str = yesterday_date.strftime("%Y-%m-%d")

        print(f"\n{'='*65}")
        print(f"[FINALIZE] First sync of {today_str} -> re-fetching yesterday ({yesterday_str}) from Shopify")
        print(f"{'='*65}")

        result = fetch_shopify_orders_for_date(yesterday_date)
        if result is not None:
            net, gross_y, refunds_y, order_count_y, channels_y, regions_y = result
            ly, fc = excel_lookup(yesterday_date)

            # dailyTarget deliberately omitted -- Calendar-only, and merge=True
            # means this write never touches whatever's already there.
            doc_ref = db.collection("sales").document("daily").collection("days").document(yesterday_str)
            doc_ref.set({
                "date":          yesterday_str,
                "currentSale":   float(f"{net:.2f}"),
                "grossSale":     float(f"{gross_y:.2f}"),
                "totalRefunds":  float(f"{refunds_y:.2f}"),
                "totalOrders":   order_count_y,
                "lastYearSale":  float(f"{ly:.2f}"),
                "dailyForecast": float(f"{fc:.2f}"),
                "channels":      channels_y,
                "regions":       regions_y,
                "syncedAt":      now_my.isoformat(),
                "source":        "shopify",
            }, merge=True)

            sync_state_ref.set({"lastYesterdayFinalizeDate": today_str}, merge=True)
            print(f"   [OK] Finalized {yesterday_str}: Current RM{net:.2f} | Orders {order_count_y}")
        else:
            print(f"   [ERROR] Could not fetch yesterday's orders -- will retry on next cron tick")
    else:
        print(f"\n[FINALIZE] Yesterday already finalized today -> skipped")

except Exception as e:
    print(f"\n[WARN] Yesterday finalize check failed: {e}")


# ------------------------------------------------------------------------------
# ---- STEP 4.5: Check for manual sync requests --------------------------------
# Runs on EVERY cron. Checks sales/syncRequest for pending jobs.
# ------------------------------------------------------------------------------

try:
    sync_req_ref = db.collection("sales").document("syncRequest")
    sync_req = sync_req_ref.get()

    if sync_req.exists:
        req_data = sync_req.to_dict()
        if req_data.get("status") == "pending":
            from_date = date.fromisoformat(req_data["fromDate"])
            to_date = date.fromisoformat(req_data["toDate"])

            print(f"\n{'='*65}")
            print(f"[SYNC] MANUAL SYNC REQUEST: {from_date} -> {to_date}")
            print(f"{'='*65}")

            # Mark as processing
            sync_req_ref.set({"status": "processing", "startedAt": now_my.isoformat()}, merge=True)

            req_synced = 0
            d = from_date
            while d <= to_date:
                ds = d.strftime("%Y-%m-%d")

                # Skip future dates
                if d > now_my.date():
                    print(f"   [SKIP] {ds} -> future date, skipping")
                    d += timedelta(days=1)
                    continue

                # Skip today -- already synced in Step 4
                if d == now_my.date():
                    print(f"   [SKIP] {ds} -> today (already synced)")
                    d += timedelta(days=1)
                    continue

                print(f"   [FETCH] {ds} -> fetching from Shopify...", end=" ")
                result = fetch_shopify_orders_for_date(d)

                if result is None:
                    print("FAILED")
                    d += timedelta(days=1)
                    continue

                net, gross_d, refunds_d, order_count, channels_d, regions_d = result
                ly, fc = excel_lookup(d)

                # dailyTarget deliberately omitted -- Calendar-only, and merge=True
                # means this write never touches whatever's already there.
                doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
                doc_ref.set({
                    "date":          ds,
                    "currentSale":   float(f"{net:.2f}"),
                    "grossSale":     float(f"{gross_d:.2f}"),
                    "totalRefunds":  float(f"{refunds_d:.2f}"),
                    "totalOrders":   order_count,
                    "lastYearSale":  float(f"{ly:.2f}"),
                    "dailyForecast": float(f"{fc:.2f}"),
                    "channels":      channels_d,
                    "regions":       regions_d,
                    "syncedAt":      now_my.isoformat(),
                    "source":        "shopify",
                }, merge=True)

                print(f"[OK] Current RM{net:.2f} | Orders {order_count}")
                req_synced += 1
                time.sleep(0.5)
                d += timedelta(days=1)

            # Mark as completed
            sync_req_ref.set({
                "status": "completed",
                "completedAt": now_my.isoformat(),
                "daysSynced": req_synced,
            }, merge=True)

            print(f"   [OK] Manual sync complete: {req_synced} days synced")

except Exception as e:
    print(f"\n[WARN] Sync request check failed: {e}")


# ------------------------------------------------------------------------------
# ---- STEP 5: Sync Excel rows to Firestore (hash-gated) -----------------------
# Computes a hash of the Excel file. If unchanged since last
# sync, skips entirely (1 read only). If changed, reads all
# existing docs, compares, and writes only changed rows.
# Empty Excel cells are never written (preserves Firestore data).
# ------------------------------------------------------------------------------
import hashlib

excel_synced = 0
excel_skipped = 0
existing_docs = {}   # shared with FULL_SYNC backfill below

try:
    # Compute hash of Excel file
    excel_hash = ""
    try:
        with open("Sales_and_Target.xlsx", "rb") as f:
            excel_hash = hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        excel_hash = ""

    if excel_hash and excel_df is not None and date_col is not None:
        # Read stored hash (1 read)
        hash_ref = db.collection("sales").document("excelSyncHash")
        hash_doc = hash_ref.get()
        stored_hash = hash_doc.to_dict().get("hash", "") if hash_doc.exists else ""

        if excel_hash == stored_hash:
            print(f"\n[EXCEL SYNC] File unchanged (hash match) -> skipped (0 reads)")
        else:
            print(f"\n{'='*65}")
            print(f"[EXCEL SYNC] File changed -> syncing to Firestore")
            print(f"{'='*65}")

            # Cache all existing daily docs in one read
            days_ref = db.collection("sales").document("daily").collection("days")
            for doc in days_ref.stream():
                existing_docs[doc.id] = doc.to_dict()
            print(f"   [INFO] Loaded {len(existing_docs)} existing docs (1 collection read)")

            wb = db.batch()
            batch_count = 0

            for _, erow in excel_df.iterrows():
                row_date = erow[date_col]
                if pd.isna(row_date):
                    continue

                ds = row_date.strftime("%Y-%m-%d")
                if ds == today_str:
                    continue

                # Build update dict -- ONLY include non-empty Excel cells.
                # dailyTarget is never written here -- Calendar-only now.
                update = {"date": ds}
                if lastyear_col and pd.notna(erow[lastyear_col]):
                    update["lastYearSale"] = float(f"{float(erow[lastyear_col]):.2f}")
                if forecast_col and pd.notna(erow[forecast_col]):
                    update["dailyForecast"] = float(f"{float(erow[forecast_col]):.2f}")

                # Nothing to write (all Excel cells empty for this row)
                if len(update) <= 1:  # only "date" key
                    excel_skipped += 1
                    continue

                # Compare with existing -- skip if all values are the same
                existing = existing_docs.get(ds)
                if existing:
                    all_same = True
                    for key, val in update.items():
                        if key == "date":
                            continue
                        if existing.get(key) != val:
                            all_same = False
                            break
                    if all_same:
                        excel_skipped += 1
                        continue
                    # Merge only the changed fields (preserves Shopify data)
                    wb.set(days_ref.document(ds), update, merge=True)
                else:
                    # New doc -- create with Excel data + zeroed Shopify fields
                    new_doc = {
                        "currentSale":   0.0,
                        "grossSale":     0.0,
                        "totalRefunds":  0.0,
                        "totalOrders":   0,
                        "syncedAt":      now_my.isoformat(),
                        "source":        "excel",
                    }
                    new_doc.update(update)
                    wb.set(days_ref.document(ds), new_doc)

                excel_synced += 1
                batch_count += 1

                # Firestore batch limit is 500
                if batch_count >= 490:
                    wb.commit()
                    print(f"   [COMMIT] Committed batch of {batch_count} writes")
                    wb = db.batch()
                    batch_count = 0

            # Save new hash in same batch
            wb.set(hash_ref, {"hash": excel_hash, "syncedAt": now_my.isoformat()})
            batch_count += 1

            if batch_count > 0:
                wb.commit()
                print(f"   [COMMIT] Committed batch of {batch_count} writes")

            print(f"   [OK] {excel_synced} rows written, {excel_skipped} unchanged/empty (skipped)")
    else:
        print(f"\n[EXCEL SYNC] No Excel data available -> skipped")

except Exception as e:
    print(f"\n[WARN] Excel sync failed: {e}")


# ------------------------------------------------------------------------------
# BELOW ONLY RUNS ON FULL_SYNC (push to main / manual trigger)
# Cron runs stop here.
# ------------------------------------------------------------------------------

if not FULL_SYNC:
    print(f"\n[SKIP] Quick sync done -- skipping historical Shopify backfill")
    print(f"   [INFO] To run full sync: push to main or dispatch manually")
    sys.exit(0)


# ---- STEP 6: Historical Shopify backfill ----------------------------------------------------
# Uses existing_docs cache -- load if not already populated
if not existing_docs:
    days_ref = db.collection("sales").document("daily").collection("days")
    for doc in days_ref.stream():
        existing_docs[doc.id] = doc.to_dict()
    print(f"   [INFO] Loaded {len(existing_docs)} existing docs for backfill")
HISTORY_START = date(2026, 3, 27)   # Day 61
HISTORY_END   = date(2026, 5, 27)

print(f"\n{'='*65}")
print(f"[BACKFILL] HISTORICAL BACKFILL: {HISTORY_START} -> {HISTORY_END}")
print(f"{'='*65}")


# ---- Loop -- uses existing_docs cache, zero extra reads ----------------------------------------------------
today_date = now_my.date()
current_date = HISTORY_START
synced  = 0
skipped = 0

while current_date <= HISTORY_END:
    ds = current_date.strftime("%Y-%m-%d")

    if current_date > today_date:
        print(f"   [SKIP] {ds} -> future date, stopping backfill")
        break

    if current_date == today_date:
        current_date += timedelta(days=1)
        continue

    # Use cache -- no Firestore read
    existing = existing_docs.get(ds)
    if existing and existing.get("source") == "shopify" and "channels" in existing:
        skipped += 1
        current_date += timedelta(days=1)
        continue

    print(f"   [FETCH] {ds} -> fetching from Shopify...", end=" ")
    result = fetch_shopify_orders_for_date(current_date)

    if result is None:
        print("FAILED -- skipping")
        current_date += timedelta(days=1)
        continue

    net, gross, refunds, order_count, channels, regions = result
    ly, fc = excel_lookup(current_date)

    # dailyTarget deliberately omitted -- Calendar-only, and merge=True means
    # this write never touches whatever's already there.
    doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
    doc_ref.set({
        "date":          ds,
        "currentSale":   float(f"{net:.2f}"),
        "grossSale":     float(f"{gross:.2f}"),
        "totalRefunds":  float(f"{refunds:.2f}"),
        "totalOrders":   order_count,
        "lastYearSale":  float(f"{ly:.2f}"),
        "dailyForecast": float(f"{fc:.2f}"),
        "channels":      channels,
        "regions":       regions,
        "syncedAt":      now_my.isoformat(),
        "source":        "shopify",
    }, merge=True)

    print(f"[OK] Gross RM{gross:.2f} | Current RM{net:.2f} | Orders {order_count} | LY RM{ly:.2f} | Forecast RM{fc:.2f}")
    synced += 1

    time.sleep(0.5)
    current_date += timedelta(days=1)

print(f"\n{'='*65}")
print(f"[DONE] Backfill complete: {synced} days synced, {skipped} days already existed")
print(f"{'='*65}")
