import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
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

# ── STEP 1: Read Excel for today's lastYearSale & dailyTarget ─
last_year_sale = 0.0
daily_target   = 0.0

try:
    df = pd.read_excel("Sales_and_Target.xlsx")

    # Normalize column names (strip spaces, lowercase)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Parse date column — try common column name variants
    date_col = next((c for c in df.columns if "date" in c), None)
    lastyear_col = next((c for c in df.columns if "last" in c and "year" in c or "last_year" in c), None)
    target_col = next((c for c in df.columns if "target" in c), None)

    print(f"   Excel columns  : {list(df.columns)}")
    print(f"   Date col       : {date_col}")
    print(f"   Last year col  : {lastyear_col}")
    print(f"   Target col     : {target_col}")

    if date_col and lastyear_col:
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
        today_dt = pd.Timestamp(now_my.year, now_my.month, now_my.day)

        row = df[df[date_col] == today_dt]

        if not row.empty:
            last_year_sale = float(row[lastyear_col].values[0]) if pd.notna(row[lastyear_col].values[0]) else 0.0

            if target_col:
                target_val = row[target_col].values[0]
                if pd.notna(target_val) and float(target_val) > 0:
                    daily_target = float(target_val)
                else:
                    # Target missing for today — use last known non-zero target
                    past = df[(df[date_col] <= today_dt) & df[target_col].notna() & (df[target_col] > 0)]
                    if not past.empty:
                        daily_target = float(past.iloc[-1][target_col])
                        print(f"   ⚠️  No target for today — using last known: RM{daily_target}")

            print(f"   ✅ Excel match : LastYear=RM{last_year_sale} | Target=RM{daily_target}")
        else:
            print(f"   ⚠️  No row found for {today_str} in Excel — using 0")
    else:
        print(f"   ❌ Could not find required columns in Excel")

except FileNotFoundError:
    print(f"   ❌ Sales_and_Target.xlsx not found in repo — skipping Excel sync")
except Exception as e:
    print(f"   ❌ Excel read error: {e}")

# ── STEP 2: Fetch Shopify orders ──────────────────────────────
print(f"\n📦 Fetching Shopify orders...")

all_orders = []
url    = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
params = {
    "created_at_min": start_str,
    "created_at_max": end_str,
    "status":           "any",
    "financial_status": "any",
    "limit":            250,
    "fields":           "id,subtotal_price,financial_status,cancel_reason,refunds",
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

# ── STEP 3: Calculate net sales ───────────────────────────────
gross_sale    = 0.0
total_returns = 0.0

for o in active_orders:
    subtotal = float(o.get("subtotal_price", 0))
    order_returns = sum(
        float(rli.get("subtotal", 0))
        for refund in o.get("refunds", [])
        for rli in refund.get("refund_line_items", [])
    )
    gross_sale    += subtotal
    total_returns += order_returns

net_sale = gross_sale - total_returns

print(f"\n📊 Results:")
print(f"   Orders      : {total_orders}")
print(f"   Gross       : RM{gross_sale:.2f}")
print(f"   Returns     : -RM{total_returns:.2f}")
print(f"   Net         : RM{net_sale:.2f}")
print(f"   Last Year   : RM{last_year_sale:.2f}")
print(f"   Target      : RM{daily_target:.2f}")

updated_at = now_my.strftime("%H:%M:%S")

# ── STEP 4: Push ALL fields to Firestore ─────────────────────
doc_ref = db.collection("sales").document("today")
doc_ref.set({
    "currentSale":  round(net_sale, 2),
    "grossSale":    round(gross_sale, 2),
    "totalRefunds": round(total_returns, 2),
    "totalOrders":  total_orders,
    "lastYearSale": round(last_year_sale, 2),
    "dailyTarget":  round(daily_target, 2),
    "updatedAt":    updated_at,
    "syncedAt":     now_my.isoformat(),
    "source":       "shopify",
}, merge=False)   # overwrite fully — Excel is now the source of truth

print(f"\n✅ Firestore synced!")
print(f"🔥 Net RM{net_sale:.2f} | LastYear RM{last_year_sale:.2f} | Target RM{daily_target:.2f} | Orders {total_orders}")
