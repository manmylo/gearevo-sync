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
last_year_sale  = 0.0
daily_target    = 0.0
daily_target_explicit = False   # True only when today's Excel row has a real target cell
daily_forecast  = 0.0
excel_df        = None
date_col        = None
lastyear_col    = None
target_col      = None
forecast_col    = None

try:
    excel_df = pd.read_excel("Sales_and_Target.xlsx")
    excel_df.columns = excel_df.columns.str.strip().str.lower().str.replace(" ", "_")

    date_col     = next((c for c in excel_df.columns if "date" in c), None)
    lastyear_col = next((c for c in excel_df.columns if "last" in c and "year" in c or "last_year" in c), None)
    target_col   = next((c for c in excel_df.columns if "target" in c), None)
    forecast_col = next((c for c in excel_df.columns if "forecast" in c), None)

    print(f"   Excel columns  : {list(excel_df.columns)}")
    print(f"   Date col       : {date_col}")
    print(f"   Last year col  : {lastyear_col}")
    print(f"   Forecast col   : {forecast_col}")
    print(f"   Target col     : {target_col}")

    if date_col and lastyear_col:
        excel_df[date_col] = pd.to_datetime(excel_df[date_col], dayfirst=True, errors="coerce")
        today_dt = pd.Timestamp(now_my.year, now_my.month, now_my.day)
        row = excel_df[excel_df[date_col] == today_dt]

        if not row.empty:
            last_year_sale = float(row[lastyear_col].values[0]) if pd.notna(row[lastyear_col].values[0]) else 0.0

            if forecast_col:
                fval = row[forecast_col].values[0]
                daily_forecast = float(fval) if pd.notna(fval) and float(fval) > 0 else 0.0

            if target_col:
                target_val = row[target_col].values[0]
                if pd.notna(target_val) and float(target_val) > 0:
                    daily_target = float(target_val)
                    daily_target_explicit = True
                else:
                    past = excel_df[(excel_df[date_col] <= today_dt) & excel_df[target_col].notna() & (excel_df[target_col] > 0)]
                    if not past.empty:
                        daily_target = float(past.iloc[-1][target_col])
                        print(f"   [WARN] No target for today -> using last known: RM{daily_target:.2f}")

            print(f"   [OK] Excel match : LastYear=RM{last_year_sale:.2f} | Forecast=RM{daily_forecast:.2f} | Target=RM{daily_target:.2f}")
        else:
            print(f"   [WARN] No row found for {today_str} in Excel -> using 0")
    else:
        print(f"   [ERROR] Could not find required columns in Excel")

except FileNotFoundError:
    print(f"   [WARN] Sales_and_Target.xlsx not found -> skipping Excel sync")
except Exception as e:
    print(f"   [ERROR] Excel read error: {e}")

# ---- Daily metrics helper ----------------------------------------------------
# Net sales matches Shopify Analytics:
#   - sales (subtotal after discount) counted on the ORDER's date
#   - returns counted on the REFUND's processed date (NOT the order's date)
#   - order count INCLUDES cancelled orders (to match Shopify's order count)
def _fetch_orders(query_params):
    """Paginate Shopify orders for the given params. Returns list, or None on error."""
    out = []
    page_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
    p = dict(query_params)
    while page_url:
        resp = requests.get(page_url, params=p, headers=headers)
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 2)))
            continue
        if resp.status_code != 200:
            print(f"   [ERROR] Shopify API error {resp.status_code}: {resp.text[:200]}")
            return None
        out.extend(resp.json().get("orders", []))
        link = resp.headers.get("Link", "")
        page_url = None
        p = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    page_url = part.split(";")[0].strip().strip("<>")
                    break
    return out


def _parse_dt(s):
    """Parse a Shopify ISO timestamp to an aware UTC datetime (or None)."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_day_metrics(day_start_my, day_end_my):
    """Compute one MYT window's sales the way Shopify net sales does."""
    start_utc = day_start_my.astimezone(timezone.utc)
    end_utc   = day_end_my.astimezone(timezone.utc)
    start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str   = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1) Orders CREATED in the window -> sales + order count
    created = _fetch_orders({
        "created_at_min": start_str, "created_at_max": end_str,
        "status": "any", "financial_status": "any", "limit": 250,
        "fields": "id,order_number,subtotal_price,total_discounts,financial_status,cancel_reason",
    })
    # 2) Orders UPDATED in the window -> refunds processed in the window (any order, any date)
    updated = _fetch_orders({
        "updated_at_min": start_str, "updated_at_max": end_str,
        "status": "any", "financial_status": "any", "limit": 250,
        "fields": "id,order_number,created_at,refunds",
    })
    if created is None or updated is None:
        return None

    active    = [o for o in created if o.get("cancel_reason") is None]
    cancelled = [o for o in created if o.get("cancel_reason") is not None]

    # Count EVERY order placed in the window as a sale on its order date (incl. ones
    # later cancelled) -- exactly like Shopify. A cancellation is removed by its refund
    # on the refund's date in the refunds loop below, so same-day cancels net to zero
    # (sale +X, refund -X) and prior-day cancels correctly land as a return today.
    subtotal_sum    = sum(float(o.get("subtotal_price", 0)) for o in created)
    discounts       = sum(float(o.get("total_discounts", 0)) for o in created)
    gross           = sum(float(o.get("subtotal_price", 0)) + float(o.get("total_discounts", 0)) for o in created)
    cancelled_total = sum(float(o.get("subtotal_price", 0)) + float(o.get("total_discounts", 0)) for o in cancelled)

    # Refunds whose processed/created date falls inside this window
    refunds_in_window = 0.0
    for o in updated:
        for refund in o.get("refunds", []):
            rdt = _parse_dt(refund.get("processed_at") or refund.get("created_at"))
            if rdt is not None and start_utc <= rdt < end_utc:
                refunds_in_window += sum(float(rli.get("subtotal", 0)) for rli in refund.get("refund_line_items", []))

    return {
        "net":             subtotal_sum - refunds_in_window,
        "gross":           gross,
        "subtotal":        subtotal_sum,
        "discounts":       discounts,
        "refunds":         refunds_in_window,
        "order_count":     len(created),        # incl. cancelled - matches Shopify
        "active_count":    len(active),
        "cancelled_count": len(cancelled),
        "cancelled_total": cancelled_total,
    }


# ---- STEP 2: Fetch Shopify orders (today) ----------------------------------------------------
print(f"\n[FETCH] Fetching Shopify orders (net sales = sales by order date - refunds by refund date)...")

day = compute_day_metrics(start_my, now_my)
if day is None:
    print("[ERROR] Could not fetch today's orders")
    exit(1)

current_sale    = day["net"]
gross_sale      = day["gross"]
total_returns   = day["refunds"]
total_orders    = day["order_count"]
total_discounts = day["discounts"]
cancelled_count = day["cancelled_count"]
cancelled_total = day["cancelled_total"]

print(f"   Orders         : {total_orders}  (active {day['active_count']}, cancelled {cancelled_count})")
print(f"   Refunds today  : RM{total_returns:.2f}  (dated by refund processed date)")

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

GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-04/graphql.json"

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
print(f"   Gross       : RM{gross_sale:.2f}  -- all orders (incl. cancelled) + discounts")
print(f"   Discounts   : RM{total_discounts:.2f}")
print(f"   Cancelled   : RM{cancelled_total:.2f}  ({cancelled_count} orders)")
print(f"   Returns     : -RM{total_returns:.2f}  -- refunds dated today")
print(f"   Current     : RM{current_sale:.2f}  -- net sales (subtotal - refunds dated today)")
print(f"   Orders      : {total_orders}  (incl. cancelled, matches Shopify)")
print(f"   Last Year   : RM{last_year_sale:.2f}")
print(f"   Forecast    : RM{daily_forecast:.2f}")
print(f"   Target      : RM{daily_target:.2f}")
print(f"   Inventory   : RM{ending_inventory_retail_value:.2f}")

updated_at = now_my.strftime("%H:%M:%S")

# ---- STEP 4: Push today to Firestore ----------------------------------------------------
# Batch write = 1 commit for 2 documents
today_data = {
    "currentSale":   float(f"{current_sale:.2f}"),
    "grossSale":     float(f"{gross_sale:.2f}"),
    "totalRefunds":  float(f"{total_returns:.2f}"),
    "totalOrders":   total_orders,
    "lastYearSale":  float(f"{last_year_sale:.2f}"),
    "dailyForecast": float(f"{daily_forecast:.2f}"),
    "endingInventory": float(f"{ending_inventory_retail_value:.2f}"),
    "syncedAt":      now_my.isoformat(),
    "source":        "shopify",
}
# Only push dailyTarget from Excel when today's row actually has a real value.
# Otherwise leave the field alone -- it may have been set live via the
# CEO Dashboard Calendar, and this cron shouldn't stomp that on every run.
if daily_target_explicit:
    today_data["dailyTarget"] = float(f"{daily_target:.2f}")

wb = db.batch()
wb.set(db.collection("sales").document("today"),
       {**today_data, "updatedAt": updated_at}, merge=True)
wb.set(db.collection("sales").document("daily").collection("days").document(today_str),
       {**today_data, "date": today_str}, merge=True)
wb.commit()

print(f"\n[OK] Firestore synced (today) -- 1 batch commit (2 docs)")
print(f"[RESULT] Gross RM{gross_sale:.2f} | Current RM{current_sale:.2f} | LY RM{last_year_sale:.2f} | Forecast RM{daily_forecast:.2f} | Target RM{daily_target:.2f} | Orders {total_orders}")


# ---- Helper functions (used by sync request + backfill) ----------------------------------------------------

def excel_lookup(lookup_date):
    """Return (lastYearSale, dailyForecast, dailyTarget) from the Excel DataFrame."""
    ly  = 0.0
    fc  = 0.0
    tgt = 0.0
    if excel_df is None or date_col is None or lastyear_col is None:
        return ly, fc, tgt

    dt = pd.Timestamp(lookup_date.year, lookup_date.month, lookup_date.day)
    row = excel_df[excel_df[date_col] == dt]

    if not row.empty:
        val = row[lastyear_col].values[0]
        ly = float(val) if pd.notna(val) else 0.0

        if forecast_col:
            fval = row[forecast_col].values[0]
            fc = float(fval) if pd.notna(fval) and float(fval) > 0 else 0.0

        if target_col:
            tval = row[target_col].values[0]
            if pd.notna(tval) and float(tval) > 0:
                tgt = float(tval)
            else:
                past = excel_df[(excel_df[date_col] <= dt) & excel_df[target_col].notna() & (excel_df[target_col] > 0)]
                if not past.empty:
                    tgt = float(past.iloc[-1][target_col])
    return ly, fc, tgt


def fetch_shopify_orders_for_date(target_date):
    """One MYT day's metrics (net sales matches Shopify). Returns (net, gross, refunds, order_count)."""
    day_start_my = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=MY_TZ)
    day_end_my   = day_start_my + timedelta(days=1)
    m = compute_day_metrics(day_start_my, day_end_my)
    if m is None:
        return None
    return m["net"], m["gross"], m["refunds"], m["order_count"]


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

                net, gross_d, refunds_d, order_count = result
                ly, fc, tgt = excel_lookup(d)

                doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
                doc_ref.set({
                    "date":          ds,
                    "currentSale":   float(f"{net:.2f}"),
                    "grossSale":     float(f"{gross_d:.2f}"),
                    "totalRefunds":  float(f"{refunds_d:.2f}"),
                    "totalOrders":   order_count,
                    "lastYearSale":  float(f"{ly:.2f}"),
                    "dailyForecast": float(f"{fc:.2f}"),
                    "dailyTarget":   float(f"{tgt:.2f}"),
                    "syncedAt":      now_my.isoformat(),
                    "source":        "shopify",
                })

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

                # Build update dict -- ONLY include non-empty Excel cells
                update = {"date": ds}
                if lastyear_col and pd.notna(erow[lastyear_col]):
                    update["lastYearSale"] = float(f"{float(erow[lastyear_col]):.2f}")
                if forecast_col and pd.notna(erow[forecast_col]):
                    update["dailyForecast"] = float(f"{float(erow[forecast_col]):.2f}")
                if target_col and pd.notna(erow[target_col]):
                    update["dailyTarget"] = float(f"{float(erow[target_col]):.2f}")

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
    if existing and existing.get("source") == "shopify":
        skipped += 1
        current_date += timedelta(days=1)
        continue

    print(f"   [FETCH] {ds} -> fetching from Shopify...", end=" ")
    result = fetch_shopify_orders_for_date(current_date)

    if result is None:
        print("FAILED -- skipping")
        current_date += timedelta(days=1)
        continue

    net, gross, refunds, order_count = result
    ly, fc, tgt = excel_lookup(current_date)

    doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
    doc_ref.set({
        "date":          ds,
        "currentSale":   float(f"{net:.2f}"),
        "grossSale":     float(f"{gross:.2f}"),
        "totalRefunds":  float(f"{refunds:.2f}"),
        "totalOrders":   order_count,
        "lastYearSale":  float(f"{ly:.2f}"),
        "dailyForecast": float(f"{fc:.2f}"),
        "dailyTarget":   float(f"{tgt:.2f}"),
        "syncedAt":      now_my.isoformat(),
        "source":        "shopify",
    })

    print(f"[OK] Gross RM{gross:.2f} | Current RM{net:.2f} | Orders {order_count} | LY RM{ly:.2f} | Forecast RM{fc:.2f} | Tgt RM{tgt:.2f}")
    synced += 1

    time.sleep(0.5)
    current_date += timedelta(days=1)

print(f"\n{'='*65}")
print(f"[DONE] Backfill complete: {synced} days synced, {skipped} days already existed")
print(f"{'='*65}")
