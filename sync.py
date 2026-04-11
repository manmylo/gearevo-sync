import os
import json
import requests
from datetime import datetime, timezone, timedelta
import firebase_admin
from firebase_admin import credentials, firestore

# ── Load secrets from environment variables ──────────────────
SHOPIFY_STORE = os.environ["SHOPIFY_STORE"]   # e.g. gearevo.myshopify.com
SHOPIFY_TOKEN = os.environ["SHOPIFY_TOKEN"]   # shpat_xxxx
FIREBASE_CREDS = json.loads(os.environ["FIREBASE_CREDENTIALS"])

# ── Init Firebase ────────────────────────────────────────────
cred = credentials.Certificate(FIREBASE_CREDS)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ── Malaysia timezone (UTC+8) ────────────────────────────────
MY_TZ = timezone(timedelta(hours=8))
now_my = datetime.now(MY_TZ)
today_str = now_my.strftime("%Y-%m-%d")
start_of_day = today_str + "T00:00:00+08:00"
end_of_day   = today_str + "T23:59:59+08:00"

print(f"📅 Fetching orders for {today_str}...")

# ── Fetch paid orders from Shopify ───────────────────────────
url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
params = {
    "status": "any",
    "financial_status": "paid",
    "created_at_min": start_of_day,
    "created_at_max": end_of_day,
    "limit": 250,
    "fields": "id,total_price,currency"
}
headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

response = requests.get(url, params=params, headers=headers)

if response.status_code != 200:
    print(f"❌ Shopify API error {response.status_code}: {response.text}")
    exit(1)

orders = response.json().get("orders", [])
total_sale = sum(float(o.get("total_price", 0)) for o in orders)
total_orders = len(orders)
updated_at = now_my.strftime("%I:%M %p")  # e.g. 03:45 PM

print(f"✅ Total Sale: RM{total_sale:.2f} | Orders: {total_orders} | Updated: {updated_at}")

# ── Push to Firestore ────────────────────────────────────────
doc_ref = db.collection("sales").document("today")

# Only update live fields — preserve lastYearSale & dailyTarget set manually
doc_ref.set({
    "currentSale": round(total_sale, 2),
    "totalOrders": total_orders,
    "updatedAt": updated_at,
    "syncedAt": now_my.isoformat(),
    "source": "shopify",
}, merge=True)   # merge=True keeps existing fields like lastYearSale & dailyTarget

print("🔥 Firestore updated successfully!")
