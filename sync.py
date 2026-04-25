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
# FULL_SYNC=1 → Excel sync + historical backfill (push/manual only)
# Default (cron) → today's Shopify only = 1 batch write (2 docs)
FULL_SYNC = os.environ.get("FULL_SYNC", "0") == "1"

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
print(f"🔧 Mode: {'FULL SYNC (Excel + Backfill)' if FULL_SYNC else 'QUICK SYNC (today only)'}")

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
# Batch write = 1 commit for 2 documents
today_data = {
    "currentSale":   float(f"{current_sale:.2f}"),
    "grossSale":     float(f"{gross_sale:.2f}"),
    "totalRefunds":  float(f"{total_returns:.2f}"),
    "totalOrders":   total_orders,
    "lastYearSale":  float(f"{last_year_sale:.2f}"),
    "dailyForecast": float(f"{daily_forecast:.2f}"),
    "dailyTarget":   float(f"{daily_target:.2f}"),
    "syncedAt":      now_my.isoformat(),
    "source":        "shopify",
}

wb = db.batch()
wb.set(db.collection("sales").document("today"),
       {**today_data, "updatedAt": updated_at}, merge=False)
wb.set(db.collection("sales").document("daily").collection("days").document(today_str),
       {**today_data, "date": today_str}, merge=False)
wb.commit()

print(f"\n✅ Firestore synced (today) — 1 batch commit (2 docs)")
print(f"🔥 Gross RM{gross_sale:.2f} | Current RM{current_sale:.2f} | LY RM{last_year_sale:.2f} | Forecast RM{daily_forecast:.2f} | Target RM{daily_target:.2f} | Orders {total_orders}")

# ══════════════════════════════════════════════════════════════
# BELOW ONLY RUNS ON FULL_SYNC (push to main / manual trigger)
# Cron runs stop here = 1 batch write per run
# ══════════════════════════════════════════════════════════════

if not FULL_SYNC:
    print(f"\n⏩ Quick sync done — skipping Excel sync & backfill")
    print(f"   💡 To run full sync: push to main or dispatch manually")
    sys.exit(0)


# ── STEP 5: Sync ALL Excel rows to Firestore ─────────────────
# Fetches all existing docs in ONE read, compares values,
# only writes rows that actually changed. Uses batch writes.
print(f"\n{'═'*65}")
print(f"📊 EXCEL SYNC: Checking Excel rows against Firestore")
print(f"{'═'*65}")

excel_synced = 0
excel_skipped = 0

# Cache all existing daily docs in one read
days_ref = db.collection("sales").document("daily").collection("days")
existing_docs = {}
for doc in days_ref.stream():
    existing_docs[doc.id] = doc.to_dict()
print(f"   📖 Loaded {len(existing_docs)} existing docs (1 collection read)")

if excel_df is not None and date_col is not None and lastyear_col is not None:
    wb = db.batch()
    batch_count = 0

    for _, erow in excel_df.iterrows():
        row_date = erow[date_col]
        if pd.isna(row_date):
            continue

        ds = row_date.strftime("%Y-%m-%d")
        if ds == today_str:
            continue

        # Build new values
        ly  = float(erow[lastyear_col]) if lastyear_col and pd.notna(erow[lastyear_col]) else 0.0
        fc  = float(erow[forecast_col]) if forecast_col and pd.notna(erow[forecast_col]) else 0.0
        tgt = float(erow[target_col])   if target_col   and pd.notna(erow[target_col])   else 0.0

        new_ly  = float(f"{ly:.2f}")
        new_fc  = float(f"{fc:.2f}")
        new_tgt = float(f"{tgt:.2f}")

        # Compare with existing — skip if unchanged
        existing = existing_docs.get(ds)
        if existing:
            if (existing.get("lastYearSale") == new_ly and
                existing.get("dailyForecast") == new_fc and
                existing.get("dailyTarget") == new_tgt):
                excel_skipped += 1
                continue
            # Only merge the changed fields
            wb.set(days_ref.document(ds), {
                "date":          ds,
                "lastYearSale":  new_ly,
                "dailyForecast": new_fc,
                "dailyTarget":   new_tgt,
            }, merge=True)
        else:
            # New doc — create with zeroed Shopify fields
            wb.set(days_ref.document(ds), {
                "date":          ds,
                "lastYearSale":  new_ly,
                "dailyForecast": new_fc,
                "dailyTarget":   new_tgt,
                "currentSale":   0.0,
                "grossSale":     0.0,
                "totalRefunds":  0.0,
                "totalOrders":   0,
                "syncedAt":      now_my.isoformat(),
                "source":        "excel",
            })

        excel_synced += 1
        batch_count += 1

        # Firestore batch limit is 500
        if batch_count >= 490:
            wb.commit()
            print(f"   📤 Committed batch of {batch_count} writes")
            wb = db.batch()
            batch_count = 0

    if batch_count > 0:
        wb.commit()
        print(f"   📤 Committed batch of {batch_count} writes")

    print(f"   ✅ {excel_synced} rows written, {excel_skipped} unchanged (skipped)")
else:
    print(f"   ⚠️  No Excel data available — skipping")


# ── STEP 6: Historical Shopify backfill ──────────────────────
# Uses existing_docs cache — zero extra reads
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


# ── Loop — uses existing_docs cache, zero extra reads ────────
today_date = now_my.date()
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

    # Use cache — no Firestore read
    existing = existing_docs.get(ds)
    if existing and existing.get("source") == "shopify":
        skipped += 1
        current_date += timedelta(days=1)
        continue

    print(f"   📦 {ds} — fetching from Shopify...", end=" ")
    result = fetch_shopify_orders_for_date(current_date)

    if result is None:
        print("FAILED — skipping")
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

    print(f"✅ Gross RM{gross:.2f} | Current RM{net:.2f} | Orders {order_count} | LY RM{ly:.2f} | Forecast RM{fc:.2f} | Tgt RM{tgt:.2f}")
    synced += 1

    time.sleep(0.5)
    current_date += timedelta(days=1)

print(f"\n{'═'*65}")
print(f"📜 Backfill complete: {synced} days synced, {skipped} days already existed")
print(f"{'═'*65}")
