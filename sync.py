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

headers = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

# ── STEP 0: Print granted scopes so we can verify ────────────
print("🔑 Checking granted API scopes...")
scope_resp = requests.get(
    f"https://{SHOPIFY_STORE}/admin/api/2024-01/oauth/access_scopes.json",
    headers=headers
)
if scope_resp.status_code == 200:
    scopes = [s["handle"] for s in scope_resp.json().get("access_scopes", [])]
    print(f"   Granted scopes: {scopes}")
    required = {"read_orders"}
    missing  = required - set(scopes)
    if missing:
        print(f"   ❌ MISSING required scopes: {missing}")
        exit(1)
    else:
        print(f"   ✅ All required scopes present")
else:
    print(f"   ⚠️  Could not fetch scopes (status {scope_resp.status_code}) — continuing anyway")

# ── Malaysia timezone (UTC+8) ────────────────────────────────
MY_TZ = timezone(timedelta(hours=8))
now_my = datetime.now(MY_TZ)
today_str = now_my.strftime("%Y-%m-%d")

# Window: 12:00:00 AM MY time → now (matches Shopify "Today" filter exactly)
start_my  = datetime(now_my.year, now_my.month, now_my.day, 0, 0, 0, tzinfo=MY_TZ)
start_utc = start_my.astimezone(timezone.utc)
end_utc   = now_my.astimezone(timezone.utc)
start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
end_str   = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"\n📅 Date  : {today_str} (MY time)")
print(f"   Window: 12:00:00 AM → {now_my.strftime('%I:%M:%S %p')} MYT")
print(f"   UTC   : {start_str} → {end_str}")

def fetch_all_orders(extra_params={}):
    all_orders = []
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
    params = {
        "created_at_min": start_str,
        "created_at_max": end_str,
        "limit": 250,
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
        print(f"   Got {len(batch)} orders (total so far: {len(all_orders)})")
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
print(f"\n📦 Fetching orders...")
all_orders = fetch_all_orders({
    "status": "any",
    "financial_status": "any"
})

# Filter out cancelled orders
active_orders = [o for o in all_orders if o.get("cancel_reason") is None]
total_orders  = len(active_orders)
cancelled     = len(all_orders) - total_orders

print(f"\n   Raw fetched  : {len(all_orders)} orders")
print(f"   Cancelled    : {cancelled} (excluded)")
print(f"   Active       : {total_orders} (included)")

# ── Per-order breakdown ───────────────────────────────────────
print(f"\n📋 Per-order breakdown:")
gross_sale    = 0.0
total_returns = 0.0

for o in active_orders:
    oid      = o["id"]
    subtotal = float(o.get("subtotal_price", 0))    # original line items (gross)
    refunds_on_order = o.get("refunds", [])

    # Sum refund_line_items[].subtotal — this is exactly Shopify's "Returns" figure
    # (pre-tax line item refund amounts only, excludes shipping refunds)
    order_returns = sum(
        float(rli.get("subtotal", 0))
        for refund in refunds_on_order
        for rli in refund.get("refund_line_items", [])
    )

    order_net = subtotal - order_returns
    gross_sale    += subtotal
    total_returns += order_returns

    num_refund_events = len(refunds_on_order)
    print(f"   #{oid} | gross={subtotal:.2f} | returns={order_returns:.2f} | net={order_net:.2f} | status={o.get('financial_status')} | refund_events={num_refund_events}")

net_sale = gross_sale - total_returns

print(f"\n📊 Summary:")
print(f"   Gross sales : RM{gross_sale:.2f}")
print(f"   Returns     : -RM{total_returns:.2f}")
print(f"   Net sales   : RM{net_sale:.2f}   ← should match Shopify Analytics")
print(f"   Orders      : {total_orders}")

updated_at = now_my.strftime("%H:%M:%S")

# ── Push to Firestore (merge — preserves lastYearSale & dailyTarget) ──
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

print(f"\n✅ Firestore updated!")
print(f"🔥 Net RM{net_sale:.2f} | Gross RM{gross_sale:.2f} | Returns -RM{total_returns:.2f} | Orders {total_orders}")
