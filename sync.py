import os
import json
import requests
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

# ── Malaysia timezone (UTC+8) ────────────────────────────────
MY_TZ = timezone(timedelta(hours=8))
now_my = datetime.now(MY_TZ)
today_str = now_my.strftime("%Y-%m-%d")

# Shopify stores timestamps in UTC — convert start/end of MY day to UTC
start_of_day_utc = datetime(now_my.year, now_my.month, now_my.day, 0, 0, 0, tzinfo=MY_TZ).astimezone(timezone.utc)
end_of_day_utc   = datetime(now_my.year, now_my.month, now_my.day, 23, 59, 59, tzinfo=MY_TZ).astimezone(timezone.utc)

start_str = start_of_day_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
end_str   = end_of_day_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"📅 Fetching orders for {today_str} (MY time)")
print(f"   UTC range: {start_str} → {end_str}")

# ── Fetch ALL paid orders with pagination ────────────────────
headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

all_orders = []
url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
params = {
    "financial_status": "paid",
    "created_at_min": start_str,
    "created_at_max": end_str,
    "limit": 250,
    "fields": "id,total_price,currency"
}

# Paginate through all results
while url:
    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        print(f"❌ Shopify API error {response.status_code}: {response.text}")
        exit(1)

    batch = response.json().get("orders", [])
    all_orders.extend(batch)
    print(f"   Fetched {len(batch)} orders (total so far: {len(all_orders)})")

    # Check for next page via Link header
    link_header = response.headers.get("Link", "")
    if 'rel="next"' in link_header:
        # Extract next URL
        parts = link_header.split(",")
        next_url = None
        for part in parts:
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                break
        url = next_url
        params = {}  # params are in the URL already
    else:
        url = None

# ── Calculate totals ─────────────────────────────────────────
total_sale   = sum(float(o.get("total_price", 0)) for o in all_orders)
total_orders = len(all_orders)
updated_at   = now_my.strftime("%H:%M:%S")  # 24h format e.g. 14:30:00

print(f"✅ Total Sale: RM{total_sale:.2f} | Orders: {total_orders} | Updated: {updated_at}")

# ── Push to Firestore ────────────────────────────────────────
doc_ref = db.collection("sales").document("today")

doc_ref.set({
    "currentSale":  round(total_sale, 2),
    "totalOrders":  total_orders,
    "updatedAt":    updated_at,
    "syncedAt":     now_my.isoformat(),
    "source":       "shopify",
}, merge=True)  # merge=True keeps lastYearSale, dailyTarget set manually

print("🔥 Firestore updated successfully!")
