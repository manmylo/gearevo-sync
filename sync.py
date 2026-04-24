import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta, date
import firebase_admin
from firebase_admin import credentials, firestore

# ── Load secrets ─────────────────────────────────────────────
SHOPIFY_STORE = os.environ["SHOPIFY_STORE"]
SHOPIFY_TOKEN = os.environ["SHOPIFY_TOKEN"]
FIREBASE_CREDS = json.loads(os.environ["FIREBASE_CREDENTIALS"])

# ── Init Firebase ────────────────────────────────────────────
cred = credentials.Certificate(FIREBASE_CREDS)
firebase_admin.initialize_app(cred)
db = firestore.client()

headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

# ── Malaysia timezone (UTC+8) ────────────────────────────────
MY_TZ = timezone(timedelta(hours=8))
now_my = datetime.now(MY_TZ)
today_str = now_my.strftime("%Y-%m-%d")

start_my  = datetime(now_my.year, now_my.month, now_my.day, 0, 0, 0, tzinfo=MY_TZ)
start_utc = start_my.astimezone(timezone.utc)
end_utc   = now_my.astimezone(timezone.utc)
start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
end_str   = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"📅 {today_str} | Window: 12:00 AM → {now_my.strftime('%I:%M:%S %p')} MYT")

# ── STEP 1: Read Excel for lastYearSale, dailyForecast & dailyTarget ──
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

# ── STEP 2: Fetch Shopify orders (today) ──────────────────────
print(f"\n📦 Fetching Shopify orders...")

all_orders = []
url    = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
params = {
    "created_at_min": start_str,
    "created_at_max": end_str,
    "status":           "any",
    "financial_status": "any",
    "limit":            250,
    "fields":           "id,order_number,subtotal_price,total_discounts,financial_status,cancel_reason,refunds",
}

while url:
    response = requests.get(url, params=params, headers=headers)
    if response.status_code != 200:
        print(f"❌ Shopify API error {response.status_code}: {response.text}")
        exit(1)
    batch = response.json().get("orders", [])
    all_orders.extend(batch)
    print(f"   Page: {len(batch)} orders (total: {len(all_orders)})")
    link   = response.headers.get("Link", "")
    url    = None
    params = {}
    if 'rel="next"' in link:
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                break

active_orders = [o for o in all_orders if o.get("cancel_reason") is None]
total_orders  = len(active_orders)

print(f"   Active orders  : {total_orders} (cancelled excluded: {len(all_orders) - total_orders})")

# ── STEP 3: Per-order breakdown + totals ─────────────────────
print(f"\n{'─'*75}")
print(f"{'Order':<10} {'Subtotal':>12} {'Returns':>12} {'Net':>12}  Status")
print(f"{'─'*75}")

current_sale  = 0.0
total_returns = 0.0

for o in active_orders:
    order_num     = o.get("order_number", o["id"])
    subtotal      = float(o.get("subtotal_price", 0))
    order_returns = sum(
        float(rli.get("subtotal", 0))
        for refund in o.get("refunds", [])
        for rli in refund.get("refund_line_items", [])
    )
    order_net     = subtotal - order_returns
    current_sale  += order_net
    total_returns += order_returns

    refund_flag = " ↩" if order_returns > 0 else ""
    print(f"#{order_num:<9} {subtotal:>12.2f} {order_returns:>12.2f} {order_net:>12.2f}  {o.get('financial_status','')}{refund_flag}")

gross_sale = sum(
    float(o.get("subtotal_price", 0)) + float(o.get("total_discounts", 0))
    for o in all_orders
)
total_discounts = sum(float(o.get("total_discounts", 0)) for o in all_orders)

cancelled_orders = [o for o in all_orders if o.get("cancel_reason") is not None]
cancelled_total  = sum(
    float(o.get("subtotal_price", 0)) + float(o.get("total_discounts", 0))
    for o in cancelled_orders
)

print(f"{'─'*75}")
print(f"{'ACTIVE':<10} {current_sale + total_returns:>12.2f} {total_returns:>12.2f} {current_sale:>12.2f}")
if cancelled_orders:
    print(f"{'CANCELLED':<10} {cancelled_total:>12.2f} {'—':>12} {'—':>12}  ({len(cancelled_orders)} orders)")
print(f"{'GROSS':<10} {gross_sale:>12.2f}  (incl. discounts RM{total_discounts:.2f})")
print(f"{'─'*75}")

print(f"\n📊 Summary:")
print(f"   Gross       : RM{gross_sale:.2f}  ← all orders incl. cancelled + discounts")
print(f"   Discounts   : RM{total_discounts:.2f}")
print(f"   Cancelled   : RM{cancelled_total:.2f}  ({len(cancelled_orders)} orders)")
print(f"   Returns     : -RM{total_returns:.2f}")
print(f"   Current     : RM{current_sale:.2f}  ← active orders minus returns")
print(f"   Last Year   : RM{last_year_sale:.2f}")
print(f"   Forecast    : RM{daily_forecast:.2f}")
print(f"   Target      : RM{daily_target:.2f}")

updated_at = now_my.strftime("%H:%M:%S")

# ── STEP 4: Push today to Firestore ──────────────────────────
doc_ref = db.collection("sales").document("today")
doc_ref.set({
    "currentSale":   float(f"{current_sale:.2f}"),
    "grossSale":     float(f"{gross_sale:.2f}"),
    "totalRefunds":  float(f"{total_returns:.2f}"),
    "totalOrders":   total_orders,
    "lastYearSale":  float(f"{last_year_sale:.2f}"),
    "dailyForecast": float(f"{daily_forecast:.2f}"),
    "dailyTarget":   float(f"{daily_target:.2f}"),
    "updatedAt":     updated_at,
    "syncedAt":      now_my.isoformat(),
    "source":        "shopify",
}, merge=False)

today_daily_ref = db.collection("sales").document("daily").collection("days").document(today_str)
today_daily_ref.set({
    "date":          today_str,
    "currentSale":   float(f"{current_sale:.2f}"),
    "grossSale":     float(f"{gross_sale:.2f}"),
    "totalRefunds":  float(f"{total_returns:.2f}"),
    "totalOrders":   total_orders,
    "lastYearSale":  float(f"{last_year_sale:.2f}"),
    "dailyForecast": float(f"{daily_forecast:.2f}"),
    "dailyTarget":   float(f"{daily_target:.2f}"),
    "syncedAt":      now_my.isoformat(),
    "source":        "shopify",
}, merge=False)

print(f"\n✅ Firestore synced (today)!")
print(f"🔥 Gross RM{gross_sale:.2f} | Current RM{current_sale:.2f} | LY RM{last_year_sale:.2f} | Forecast RM{daily_forecast:.2f} | Target RM{daily_target:.2f} | Orders {total_orders}")


# ══════════════════════════════════════════════════════════════
# ── STEP 5: Sync ALL Excel rows to Firestore ─────────────────
# Pushes lastYearSale, dailyForecast, dailyTarget for EVERY row
# in the Excel file (past AND future dates).
# For past dates that already have Shopify data, merges the
# Excel fields — does NOT overwrite currentSale/orders.
# For future dates (no Shopify data), creates the doc with
# Excel data so the chart can show forecast/lastYear ahead.
# ══════════════════════════════════════════════════════════════

print(f"\n{'═'*65}")
print(f"📊 EXCEL SYNC: Pushing all Excel rows to Firestore")
print(f"{'═'*65}")

excel_synced = 0

if excel_df is not None and date_col is not None and lastyear_col is not None:
    for _, erow in excel_df.iterrows():
        row_date = erow[date_col]
        if pd.isna(row_date):
            continue

        ds = row_date.strftime("%Y-%m-%d")

        # Skip today — already synced in Step 4
        if ds == today_str:
            continue

        # Build Excel fields
        ly  = float(erow[lastyear_col]) if lastyear_col and pd.notna(erow[lastyear_col]) else 0.0
        fc  = float(erow[forecast_col]) if forecast_col and pd.notna(erow[forecast_col]) else 0.0
        tgt = float(erow[target_col])   if target_col   and pd.notna(erow[target_col])   else 0.0

        excel_patch = {
            "date":          ds,
            "lastYearSale":  float(f"{ly:.2f}"),
            "dailyForecast": float(f"{fc:.2f}"),
            "dailyTarget":   float(f"{tgt:.2f}"),
        }

        doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
        doc = doc_ref.get()

        if doc.exists:
            # Document exists (has Shopify data) — merge Excel fields only
            doc_ref.set(excel_patch, merge=True)
        else:
            # No Shopify data yet — create doc with Excel data + zeroed Shopify fields
            excel_patch.update({
                "currentSale":  0.0,
                "grossSale":    0.0,
                "totalRefunds": 0.0,
                "totalOrders":  0,
                "syncedAt":     now_my.isoformat(),
                "source":       "excel",
            })
            doc_ref.set(excel_patch)

        excel_synced += 1

    print(f"   ✅ {excel_synced} Excel rows synced to Firestore")
else:
    print(f"   ⚠️  No Excel data available — skipping")


# ══════════════════════════════════════════════════════════════
# ── STEP 6: Historical Shopify backfill (26/3/2026 → 27/5/2026)
# Only fetches days missing Shopify data from Firestore.
# Skips future dates. Uses same net-sale logic as today's sync.
# ══════════════════════════════════════════════════════════════

HISTORY_START = date(2026, 3, 27)   # Day 61
HISTORY_END   = date(2026, 5, 27)

print(f"\n{'═'*65}")
print(f"📜 HISTORICAL BACKFILL: {HISTORY_START} → {HISTORY_END}")
print(f"{'═'*65}")


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
    """Fetch all Shopify orders for a single MYT day."""
    day_start_my  = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=MY_TZ)
    day_end_my    = day_start_my + timedelta(days=1)
    day_start_utc = day_start_my.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    day_end_utc   = day_end_my.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    orders = []
    url    = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
    p      = {
        "created_at_min": day_start_utc,
        "created_at_max": day_end_utc,
        "status":           "any",
        "financial_status": "any",
        "limit":            250,
        "fields":           "id,order_number,subtotal_price,total_discounts,financial_status,cancel_reason,refunds",
    }

    while url:
        resp = requests.get(url, params=p, headers=headers)
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2))
            print(f"      ⏳ Rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        if resp.status_code != 200:
            print(f"      ❌ Shopify API error {resp.status_code}: {resp.text}")
            return None
        batch = resp.json().get("orders", [])
        orders.extend(batch)
        link = resp.headers.get("Link", "")
        url  = None
        p    = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    break

    gross = sum(
        float(o.get("subtotal_price", 0)) + float(o.get("total_discounts", 0))
        for o in orders
    )

    active = [o for o in orders if o.get("cancel_reason") is None]
    current_total = 0.0
    refunds_total = 0.0
    for o in active:
        subtotal = float(o.get("subtotal_price", 0))
        order_refunds = sum(
            float(rli.get("subtotal", 0))
            for refund in o.get("refunds", [])
            for rli in refund.get("refund_line_items", [])
        )
        current_total += subtotal - order_refunds
        refunds_total += order_refunds

    return current_total, gross, refunds_total, len(active)


# ── Loop through each date in range ──────────────────────────
today_date = now_my.date()
current = HISTORY_START
synced  = 0
skipped = 0

while current <= HISTORY_END:
    ds = current.strftime("%Y-%m-%d")

    # Skip future dates
    if current > today_date:
        print(f"   ⏭  {ds} — future date, stopping backfill")
        break

    # Skip today — already synced in Step 4
    if current == today_date:
        print(f"   ⏭  {ds} — today (already synced above)")
        current += timedelta(days=1)
        continue

    # Check if this date already has Shopify data in Firestore
    doc_ref = db.collection("sales").document("daily").collection("days").document(ds)
    doc = doc_ref.get()
    if doc.exists and doc.to_dict().get("source") == "shopify":
        skipped += 1
        current += timedelta(days=1)
        continue

    # Fetch from Shopify
    print(f"   📦 {ds} — fetching from Shopify...", end=" ")
    result = fetch_shopify_orders_for_date(current)

    if result is None:
        print("FAILED — skipping")
        current += timedelta(days=1)
        continue

    net, gross, refunds, order_count = result

    # Excel lookup
    ly, fc, tgt = excel_lookup(current)

    # Write to Firestore
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

    print(f"✅ Gross RM{gross:.2f} | Current RM{net:.2f} | Orders {order_count} | LY RM{ly:.2f} | Forecast RM{fc:.2f} | Tgt RM{tgt:.2f}")
    synced += 1

    time.sleep(0.5)
    current += timedelta(days=1)

print(f"\n{'═'*65}")
print(f"📜 Backfill complete: {synced} days synced, {skipped} days already existed")
print(f"{'═'*65}")
