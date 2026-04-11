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

# Window: 12:00:00 AM → now (MY time), converted to UTC
# Matches Shopify Analytics "Today" range exactly (12am–now)
start_my  = datetime(now_my.year, now_my.month, now_my.day, 0, 0, 0, tzinfo=MY_TZ)
start_utc = start_my.astimezone(timezone.utc)
end_utc   = now_my.astimezone(timezone.utc)   # "now" — not end of day
start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
end_str   = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"📅 Fetching orders for {today_str} (MY time)")
print(f"   MY  range : 12:00:00 AM → {now_my.strftime('%I:%M:%S %p')}")
print(f"   UTC range : {start_str} → {end_str}")

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
        # subtotal_price         = gross line items (no refunds, no shipping, no tax)
        # refunds[]              = all refund events on the order
        # refund_line_items[].subtotal = the pre-tax refunded line item amount
        #                          → this is exactly what Shopify calls "Returns"
        "fields": "id,subtotal_price,financial_status,cancel_reason,refunds",
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

# ── Fetch ALL orders in today's window ───────────────────────
all_orders = fetch_all_orders({
    "status": "any",
    "financial_status": "any"
})

# Filter out cancelled orders (matching Shopify Analytics behaviour)
active_orders = [o for o in all_orders if o.get("cancel_reason") is None]
total_orders  = len(active_orders)

# ── Gross sales ───────────────────────────────────────────────
# subtotal_price = line items total before any refunds, discounts already applied
gross_sale = sum(float(o.get("subtotal_price", 0)) for o in active_orders)

# ── Returns (Shopify Analytics definition) ────────────────────
# Sum refund_line_items[].subtotal across every refund on every active order.
# "subtotal" on refund_line_items is the pre-tax line item refund amount —
# this is the exact figure Shopify uses for the "Returns" row in analytics.
total_returns = 0.0
for order in active_orders:
    for refund in order.get("refunds", []):
        for rli in refund.get("refund_line_items", []):
            total_returns += float(rli.get("subtotal", 0))

net_sale = gross_sale - total_returns

print(f"\n📊 Results:")
print(f"   Total orders : {total_orders}")
print(f"   Gross sale   : RM{gross_sale:.2f}")
print(f"   Returns      : -RM{total_returns:.2f}")
print(f"   Net sale     : RM{net_sale:.2f}")

# ── Debug: print per-order breakdown ─────────────────────────
print(f"\n📋 Per-order breakdown:")
for o in active_orders:
    oid      = o["id"]
    subtotal = float(o.get("subtotal_price", 0))
    refunded = sum(
        float(rli.get("subtotal", 0))
        for refund in o.get("refunds", [])
        for rli in refund.get("refund_line_items", [])
    )
    print(f"   #{oid} | subtotal={subtotal:.2f} | refunded={refunded:.2f} | net={subtotal - refunded:.2f} | status={o.get('financial_status')}")

updated_at = now_my.strftime("%H:%M:%S")

# ── Push to Firestore ─────────────────────────────────────────
# Use merge=True so manual fields (lastYearSale, dailyTarget) are preserved
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
