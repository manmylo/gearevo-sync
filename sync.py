import os
import json
import time
import sys
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta, date
import firebase_admin
from firebase_admin import credentials, firestore

# ── Load secrets ─────────────────────────────────────────────
SHOPIFY_STORE = os.environ["SHOPIFY_STORE"]
SHOPIFY_TOKEN = os.environ["SHOPIFY_TOKEN"]
FIREBASE_CREDS = json.loads(os.environ["FIREBASE_CREDENTIALS"])

# ── Check run mode ───────────────────────────────────────────
# FULL_SYNC=1 → historical Shopify backfill (push/manual only)
# Default (cron) → today's Shopify + Excel sync for all rows
FULL_SYNC = os.environ.get("FULL_SYNC", "0") == "1"

# ── Init Firebase ────────────────────────────────────────────
cred = credentials.Certificate(FIREBASE_CREDS)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ── REST headers (used for inventory only) ───────────────────
rest_headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

# ── GraphQL endpoint + headers ───────────────────────────────
GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/2026-01/graphql.json"
gql_headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
}

# ── Malaysia timezone (UTC+8) ────────────────────────────────
MY_TZ = timezone(timedelta(hours=8))
now_my = datetime.now(MY_TZ)
today_str = now_my.strftime("%Y-%m-%d")

print(f"📅 {today_str} | Now: {now_my.strftime('%I:%M:%S %p')} MYT")
print(f"🔧 Mode: {'FULL SYNC (+ Shopify Backfill)' if FULL_SYNC else 'QUICK SYNC (today + Excel)'}")


# ══════════════════════════════════════════════════════════════
# ── ShopifyQL helper ──────────────────────────────────────────
# Uses the shopifyqlQuery GraphQL endpoint — the ONLY source
# that matches what Shopify's Analytics dashboard shows.
#
# REQUIRES:  read_reports  scope on your custom app.
#
# net_sales  = gross_sales - discounts - returns
#             (no taxes, no shipping — matches Analytics exactly)
# gross_sales = price × quantity  (before discounts/returns)
# orders     = count of orders (excl. test/cancelled depends on
#              Shopify's own filter — matches dashboard)
# ══════════════════════════════════════════════════════════════

def shopifyql_query(shopifyql: str) -> dict:
    """
    Execute a ShopifyQL query via the GraphQL Admin API.
    Returns parsed tableData as {col_name: value} for the first row,
    or a dict of all rows keyed by date if there are multiple rows.
    Raises on HTTP error or parse errors.
    """
    gql = """
    query RunShopifyQL($q: String!) {
      shopifyqlQuery(query: $q) {
        tableData {
          columns { name dataType displayName }
          rows
        }
        parseErrors
      }
    }
    """
    payload = {"query": gql, "variables": {"q": shopifyql}}
    resp = requests.post(GRAPHQL_URL, headers=gql_headers, json=payload, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(f"ShopifyQL HTTP {resp.status_code}: {resp.text[:400]}")

    body = resp.json()

    # Surface GraphQL-level errors
    if "errors" in body:
        raise RuntimeError(f"ShopifyQL GraphQL errors: {body['errors']}")

    result = body.get("data", {}).get("shopifyqlQuery", {})
    parse_errors = result.get("parseErrors") or []
    if parse_errors:
        raise RuntimeError(f"ShopifyQL parse errors: {parse_errors}")

    table = result.get("tableData")
    if not table:
        return {}

    columns = [c["name"] for c in table.get("columns", [])]
    rows    = table.get("rows", [])

    if not rows:
        return {}

    # Single-row result → flat dict
    if len(rows) == 1:
        row = rows[0]
        return {col: row.get(col) for col in columns}

    # Multi-row result → list of flat dicts
    return [
        {col: row.get(col) for col in columns}
        for row in rows
    ]


def fetch_shopifyql_for_date(target_date_str: str) -> dict:
    """
    Fetch net_sales, gross_sales, returns, discounts, orders for a single
    calendar day in MYT.  Date string = 'YYYY-MM-DD'.

    ShopifyQL SINCE/UNTIL are INCLUSIVE calendar-day boundaries in the
    store's local timezone — no UTC conversion needed, which is exactly
    why the numbers match Analytics.

    Returns dict with keys:
        net_sales, gross_sales, returns, discounts, orders
    All monetary values are float (RM). orders is int.
    Returns zeros on failure (logged).
    """
    shopifyql = (
        f"FROM sales "
        f"SHOW net_sales, gross_sales, returns, discounts, orders "
        f"SINCE {target_date_str} UNTIL {target_date_str}"
    )
    print(f"   🔍 ShopifyQL: {shopifyql}")
    try:
        raw = shopifyql_query(shopifyql)
    except RuntimeError as e:
        print(f"   ❌ ShopifyQL error: {e}")
        return {"net_sales": 0.0, "gross_sales": 0.0,
                "returns": 0.0, "discounts": 0.0, "orders": 0}

    if not raw:
        print(f"   ⚠️  No data returned for {target_date_str}")
        return {"net_sales": 0.0, "gross_sales": 0.0,
                "returns": 0.0, "discounts": 0.0, "orders": 0}

    def safe_float(val):
        try: return round(float(val), 2)
        except (TypeError, ValueError): return 0.0

    def safe_int(val):
        try: return int(float(val))
        except (TypeError, ValueError): return 0

    result = {
        "net_sales":   safe_float(raw.get("net_sales")),
        "gross_sales": safe_float(raw.get("gross_sales")),
        "returns":     safe_float(raw.get("returns")),
        "discounts":   safe_float(raw.get("discounts")),
        "orders":      safe_int(raw.get("orders")),
    }

    print(
        f"   ✅ net=RM{result['net_sales']:.2f} | "
        f"gross=RM{result['gross_sales']:.2f} | "
        f"returns=RM{result['returns']:.2f} | "
        f"discounts=RM{result['discounts']:.2f} | "
        f"orders={result['orders']}"
    )
    return result


# ── STEP 1: Read Excel for today's row ───────────────────────
last_year_sale  = 0.0
daily_target    = 0.0
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

    print(f"\n📊 Excel columns  : {list(excel_df.columns)}")
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
                else:
                    past = excel_df[(excel_df[date_col] <= today_dt) & excel_df[target_col].notna() & (excel_df[target_col] > 0)]
                    if not past.empty:
                        daily_target = float(past.iloc[-1][target_col])
                        print(f"   ⚠️  No target for today — using last known: RM{daily_target:.2f}")

            print(f"   ✅ Excel match : LastYear=RM{last_year_sale:.2f} | Forecast=RM{daily_forecast:.2f} | Target=RM{daily_target:.2f}")
        else:
            print(f"   ⚠️  No row found for {today_str} in Excel — using 0")
    else:
        print(f"   ❌ Could not find required columns in Excel")

except FileNotFoundError:
    print(f"   ❌ Sales_and_Target.xlsx not found — skipping Excel sync")
except Exception as e:
    print(f"   ❌ Excel read error: {e}")


# ── STEP 2: Fetch today's sales via ShopifyQL ─────────────────
print(f"\n📦 Fetching today's sales via ShopifyQL Analytics API...")
today_shopify = fetch_shopifyql_for_date(today_str)

current_sale  = today_shopify["net_sales"]
gross_sale    = today_shopify["gross_sales"]
total_returns = today_shopify["returns"]
total_discounts = today_shopify["discounts"]
total_orders  = today_shopify["orders"]

print(f"\n📊 Today's summary:")
print(f"   Net Sales (current) : RM{current_sale:.2f}  ← matches Analytics 'Net sales'")
print(f"   Gross Sales         : RM{gross_sale:.2f}")
print(f"   Returns             : RM{total_returns:.2f}")
print(f"   Discounts           : RM{total_discounts:.2f}")
print(f"   Orders              : {total_orders}")
print(f"   Last Year           : RM{last_year_sale:.2f}")
print(f"   Forecast            : RM{daily_forecast:.2f}")
print(f"   Target              : RM{daily_target:.2f}")


# ── STEP 2.5: Fetch Ending Inventory Retail Value ────────────
# Inventory is still fetched via REST Products API — no ShopifyQL
# equivalent for inventory_quantity × price.
EXCLUDED_TITLES = [
    'USED', 'Test', 'Hidden', 'Gearevo Kydex', 'PRE-ORDER',
    'Gearevo Belt', 'Servis Asah', 'Service Asah', 'Laser Engraving',
    'T-Shirt', 'Personalize Stylish', 'Gearevo Cap',
    'Knife Sheath for f. herder', 'Kydex sheath for F. Herder',
]

print(f"\n📦 Fetching ending inventory retail value...")
ending_inventory_retail_value = 0.0

try:
    all_products = []
    inv_url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json"
    inv_params = {"limit": 250, "status": "active"}
    while inv_url:
        inv_resp = requests.get(inv_url, params=inv_params, headers=rest_headers)
        if inv_resp.status_code != 200:
            print(f"   ❌ Shopify API error {inv_resp.status_code}: {inv_resp.text}")
            break
        batch = inv_resp.json().get("products", [])
        all_products.extend(batch)
        link = inv_resp.headers.get("Link", "")
        inv_url = None
        inv_params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    inv_url = part.split(";")[0].strip().strip("<>")
                    break

    print(f"   Total products fetched: {len(all_products)}")

    inv_rows = []
    for product in all_products:
        title = product.get("title", "")
        # Skip excluded titles — case-sensitive to match ShopifyQL NOT CONTAINS behavior
        if any(ex in title for ex in EXCLUDED_TITLES):
            continue
        for variant in product.get("variants", []):
            if variant.get("inventory_management") != "shopify":
                continue
            qty = int(variant.get("inventory_quantity", 0) or 0)
            price = float(variant.get("price", 0) or 0)
            if qty >= 1:
                value = qty * price
                ending_inventory_retail_value += value
                inv_rows.append((title, qty, price, value))

    inv_rows.sort(key=lambda x: -x[3])
    print(f"\n   {'Product':<60} {'Qty':>6} {'Price':>10} {'Value':>12}")
    print(f"   {'-'*92}")
    for t, q, p, v in inv_rows:
        print(f"   {t[:60]:<60} {q:>6} {p:>10.2f} {v:>12.2f}")
    print(f"   {'-'*92}")
    print(f"   TOTAL: RM {ending_inventory_retail_value:,.2f}")
    print(f"\n   ✅ Ending Inventory Retail Value: RM{ending_inventory_retail_value:.2f}")

except Exception as e:
    print(f"   ❌ Inventory fetch error: {e}")


# ── STEP 3: Push today to Firestore ──────────────────────────
updated_at = now_my.strftime("%H:%M:%S")

today_data = {
    "currentSale":    round(current_sale, 2),
    "grossSale":      round(gross_sale, 2),
    "totalRefunds":   round(total_returns, 2),
    "totalDiscounts": round(total_discounts, 2),
    "totalOrders":    total_orders,
    "lastYearSale":   round(last_year_sale, 2),
    "dailyForecast":  round(daily_forecast, 2),
    "dailyTarget":    round(daily_target, 2),
    "endingInventory": round(ending_inventory_retail_value, 2),
    "syncedAt":       now_my.isoformat(),
    "source":         "shopify",
}

wb = db.batch()
wb.set(db.collection("sales").document("today"),
       {**today_data, "updatedAt": updated_at}, merge=False)
wb.set(db.collection("sales").document("daily").collection("days").document(today_str),
       {**today_data, "date": today_str}, merge=False)
wb.commit()

print(f"\n✅ Firestore synced (today) — 1 batch commit (2 docs)")
print(f"🔥 Net RM{current_sale:.2f} | Gross RM{gross_sale:.2f} | LY RM{last_year_sale:.2f} | Forecast RM{daily_forecast:.2f} | Target RM{daily_target:.2f} | Orders {total_orders}")


# ── Helper: Excel lookup ──────────────────────────────────────
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


# ══════════════════════════════════════════════════════════════
# ── STEP 4: Check for manual sync requests ───────────────────
# Runs on EVERY cron. Checks sales/syncRequest for pending jobs.
# ══════════════════════════════════════════════════════════════

try:
    sync_req_ref = db.collection("sales").document("syncRequest")
    sync_req = sync_req_ref.get()

    if sync_req.exists:
        req_data = sync_req.to_dict()
        if req_data.get("status") == "pending":
            from_date = date.fromisoformat(req_data["fromDate"])
            to_date = date.fromisoformat(req_data["toDate"])

            print(f"\n{'═'*65}")
            print(f"🔄 MANUAL SYNC REQUEST: {from_date} → {to_date}")
            print(f"{'═'*65}")

            sync_req_ref.set({"status": "processing", "startedAt": now_my.isoformat()}, merge=True)

            req_synced = 0
            d = from_date
            while d <= to_date:
                ds = d.strftime("%Y-%m-%d")

                if d > now_my.date():
                    print(f"   ⏭  {ds} — future date, skipping")
                    d += timedelta(days=1)
                    continue

                if d == now_my.date():
                    print(f"   ⏭  {ds} — today (already synced)")
                    d += timedelta(days=1)
                    continue

                print(f"   📦 {ds} — fetching via ShopifyQL...", end=" ", flush=True)
                shopify_data = fetch_shopifyql_for_date(ds)
                ly, fc, tgt = excel_lookup(d)

                doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
                doc_ref.set({
                    "date":           ds,
                    "currentSale":    shopify_data["net_sales"],
                    "grossSale":      shopify_data["gross_sales"],
                    "totalRefunds":   shopify_data["returns"],
                    "totalDiscounts": shopify_data["discounts"],
                    "totalOrders":    shopify_data["orders"],
                    "lastYearSale":   round(ly, 2),
                    "dailyForecast":  round(fc, 2),
                    "dailyTarget":    round(tgt, 2),
                    "syncedAt":       now_my.isoformat(),
                    "source":         "shopify",
                })

                req_synced += 1
                time.sleep(0.5)   # ShopifyQL has its own rate limit bucket
                d += timedelta(days=1)

            sync_req_ref.set({
                "status": "completed",
                "completedAt": now_my.isoformat(),
                "daysSynced": req_synced,
            }, merge=True)

            print(f"   ✅ Manual sync complete: {req_synced} days synced")

except Exception as e:
    print(f"\n⚠️  Sync request check failed: {e}")


# ══════════════════════════════════════════════════════════════
# ── STEP 5: Sync Excel rows to Firestore (hash-gated) ────────
# ══════════════════════════════════════════════════════════════
import hashlib

excel_synced = 0
excel_skipped = 0
existing_docs = {}

try:
    excel_hash = ""
    try:
        with open("Sales_and_Target.xlsx", "rb") as f:
            excel_hash = hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        excel_hash = ""

    if excel_hash and excel_df is not None and date_col is not None:
        hash_ref = db.collection("sales").document("excelSyncHash")
        hash_doc = hash_ref.get()
        stored_hash = hash_doc.to_dict().get("hash", "") if hash_doc.exists else ""

        if excel_hash == stored_hash:
            print(f"\n📊 EXCEL SYNC: File unchanged (hash match) — skipped (0 reads)")
        else:
            print(f"\n{'═'*65}")
            print(f"📊 EXCEL SYNC: File changed — syncing to Firestore")
            print(f"{'═'*65}")

            days_ref = db.collection("sales").document("daily").collection("days")
            for doc in days_ref.stream():
                existing_docs[doc.id] = doc.to_dict()
            print(f"   📖 Loaded {len(existing_docs)} existing docs (1 collection read)")

            wb = db.batch()
            batch_count = 0

            for _, erow in excel_df.iterrows():
                row_date = erow[date_col]
                if pd.isna(row_date):
                    continue

                ds = row_date.strftime("%Y-%m-%d")
                if ds == today_str:
                    continue

                update = {"date": ds}
                if lastyear_col and pd.notna(erow[lastyear_col]):
                    update["lastYearSale"] = float(f"{float(erow[lastyear_col]):.2f}")
                if forecast_col and pd.notna(erow[forecast_col]):
                    update["dailyForecast"] = float(f"{float(erow[forecast_col]):.2f}")
                if target_col and pd.notna(erow[target_col]):
                    update["dailyTarget"] = float(f"{float(erow[target_col]):.2f}")

                if len(update) <= 1:
                    excel_skipped += 1
                    continue

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
                    wb.set(days_ref.document(ds), update, merge=True)
                else:
                    new_doc = {
                        "currentSale":    0.0,
                        "grossSale":      0.0,
                        "totalRefunds":   0.0,
                        "totalDiscounts": 0.0,
                        "totalOrders":    0,
                        "syncedAt":       now_my.isoformat(),
                        "source":         "excel",
                    }
                    new_doc.update(update)
                    wb.set(days_ref.document(ds), new_doc)

                excel_synced += 1
                batch_count += 1

                if batch_count >= 490:
                    wb.commit()
                    print(f"   📤 Committed batch of {batch_count} writes")
                    wb = db.batch()
                    batch_count = 0

            wb.set(hash_ref, {"hash": excel_hash, "syncedAt": now_my.isoformat()})
            batch_count += 1

            if batch_count > 0:
                wb.commit()
                print(f"   📤 Committed batch of {batch_count} writes")

            print(f"   ✅ {excel_synced} rows written, {excel_skipped} unchanged/empty (skipped)")
    else:
        print(f"\n📊 EXCEL SYNC: No Excel data available — skipped")

except Exception as e:
    print(f"\n⚠️  Excel sync failed: {e}")


# ══════════════════════════════════════════════════════════════
# BELOW ONLY RUNS ON FULL_SYNC (push to main / manual trigger)
# ══════════════════════════════════════════════════════════════

if not FULL_SYNC:
    print(f"\n⏩ Quick sync done — skipping historical Shopify backfill")
    print(f"   💡 To run full sync: push to main or dispatch manually")
    sys.exit(0)


# ── STEP 6: Historical Shopify backfill via ShopifyQL ────────
if not existing_docs:
    days_ref = db.collection("sales").document("daily").collection("days")
    for doc in days_ref.stream():
        existing_docs[doc.id] = doc.to_dict()
    print(f"   📖 Loaded {len(existing_docs)} existing docs for backfill")

HISTORY_START = date(2026, 3, 27)   # Day 61
HISTORY_END   = date(2026, 5, 27)

print(f"\n{'═'*65}")
print(f"📜 HISTORICAL BACKFILL: {HISTORY_START} → {HISTORY_END}")
print(f"{'═'*65}")

today_date   = now_my.date()
current_date = HISTORY_START
synced  = 0
skipped = 0

while current_date <= HISTORY_END:
    ds = current_date.strftime("%Y-%m-%d")

    if current_date > today_date:
        print(f"   ⏭  {ds} — future date, stopping backfill")
        break

    if current_date == today_date:
        current_date += timedelta(days=1)
        continue

    existing = existing_docs.get(ds)
    if existing and existing.get("source") == "shopify":
        skipped += 1
        current_date += timedelta(days=1)
        continue

    print(f"   📦 {ds} — fetching via ShopifyQL...", end=" ", flush=True)
    shopify_data = fetch_shopifyql_for_date(ds)
    ly, fc, tgt = excel_lookup(current_date)

    doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
    doc_ref.set({
        "date":           ds,
        "currentSale":    shopify_data["net_sales"],
        "grossSale":      shopify_data["gross_sales"],
        "totalRefunds":   shopify_data["returns"],
        "totalDiscounts": shopify_data["discounts"],
        "totalOrders":    shopify_data["orders"],
        "lastYearSale":   round(ly, 2),
        "dailyForecast":  round(fc, 2),
        "dailyTarget":    round(tgt, 2),
        "syncedAt":       now_my.isoformat(),
        "source":         "shopify",
    })

    print(f"✅ Net RM{shopify_data['net_sales']:.2f} | Gross RM{shopify_data['gross_sales']:.2f} | Orders {shopify_data['orders']} | LY RM{ly:.2f}")
    synced += 1

    time.sleep(0.5)
    current_date += timedelta(days=1)

print(f"\n{'═'*65}")
print(f"📜 Backfill complete: {synced} days synced, {skipped} days already existed")
print(f"{'═'*65}")
