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

start_utc = datetime(now_my.year, now_my.month, now_my.day, 0, 0, 0, tzinfo=MY_TZ).astimezone(timezone.utc)
end_utc   = datetime(now_my.year, now_my.month, now_my.day, 23, 59, 59, tzinfo=MY_TZ).astimezone(timezone.utc)
start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
end_str   = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"📅 Fetching orders for {today_str} (MY time)")
print(f"   UTC range: {start_str} → {end_str}")

headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

def fetch_all_orders(extra_params={}):
    all_orders = []
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
    params = {
        "created_at_min": start_str,
        "created_at_max": end_str,
        "limit": 250,
        # current_subtotal_price = line items after discounts, before shipping/tax
        # current_total_price    = final price after ALL refunds already applied
        # subtotal_price         = original line items total (no refunds)
        # We need gross (original) and net (after refunds) to mirror Shopify analytics
        "fields": "id,subtotal_price,current_subtotal_price,total_price,current_total_price,financial_status,cancel_reason,refunds",
        **extra_params
    }
    while url:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code != 200:
            print(f"❌ Shopify API error {response.status_code}: {response.text}")
            exit(1)
        batch = response.json().get("orders", [])
        all_orders.extend(batch)
        print(f"   Got {len(batch)} orders (total: {len(all_orders)})")
        link = response.headers.get("Link", "")
        url = None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    params = {}
                    break
    return all_orders

# ── Fetch ALL active orders ───────────────────────────────────
all_orders = fetch_all_orders({
    "status": "any",
    "financial_status": "any"
})

# Filter out cancelled orders
active_orders = [o for o in all_orders if o.get("cancel_reason") is None]
total_orders  = len(active_orders)

# ── Match Shopify Analytics exactly ──────────────────────────
# Shopify "Gross sales" = sum of subtotal_price (original line items, before refunds)
# Shopify "Returns"     = gross - net  (difference between original and current)
# Shopify "Net sales"   = sum of current_subtotal_price (after refunds already applied)
# Using current_subtotal_price avoids double-counting that happens when you
# subtract refund_line_items from total_price manually.
gross_sale    = sum(float(o.get("subtotal_price", 0))         for o in active_orders)
net_sale      = sum(float(o.get("current_subtotal_price", 0)) for o in active_orders)
total_returns = gross_sale - net_sale

print(f"\n📊 Results:")
print(f"   Total orders : {total_orders}")
print(f"   Gross sale   : RM{gross_sale:.2f}")
print(f"   Returns      : -RM{total_returns:.2f}")
print(f"   Net sale     : RM{net_sale:.2f}")

updated_at = now_my.strftime("%H:%M:%S")

# ── Push to Firestore ─────────────────────────────────────────
doc_ref = db.collection("sales").document("today")
doc_ref.set({
    "currentSale":  round(net_sale, 2),
    "grossSale":    round(gross_sale, 2),
    "totalRefunds": round(total_returns, 2),
    "totalOrders":  total_orders,
    "updatedAt":    updated_at,
    "syncedAt":     now_my.isoformat(),
    "source":       "shopify",
}, merge=True)

print(f"\n✅ Synced to Firestore!")
print(f"🔥 Net: RM{net_sale:.2f} | Gross: RM{gross_sale:.2f} | Returns: -RM{total_returns:.2f} | Orders: {total_orders}")
