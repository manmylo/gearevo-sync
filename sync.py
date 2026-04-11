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
print(f"   UTC: {start_str} → {end_str}")

# ── STEP 1: Fetch orders WITHOUT specifying fields ────────────
# Not using &fields= so Shopify returns the FULL order object.
# This lets us see exactly what refund data is available.
print(f"\n📦 Fetching full order objects (no field filter)...")

all_orders = []
url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json"
params = {
    "created_at_min": start_str,
    "created_at_max": end_str,
    "status": "any",
    "financial_status": "any",
    "limit": 250,
}

while url:
    response = requests.get(url, params=params, headers=headers)
    if response.status_code != 200:
        print(f"❌ Shopify API error {response.status_code}: {response.text}")
        exit(1)
    batch = response.json().get("orders", [])
    all_orders.extend(batch)
    print(f"   Page fetched: {len(batch)} orders (total: {len(all_orders)})")
    link = response.headers.get("Link", "")
    url = None
    params = {}
    if 'rel="next"' in link:
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                break

# Filter cancelled
active_orders = [o for o in all_orders if o.get("cancel_reason") is None]
print(f"   Active (non-cancelled): {len(active_orders)}")

# ── STEP 2: Dump raw refund data for every order ──────────────
# This is the most important output — paste this in chat so we
# can see exactly what Shopify is returning for refunds.
print(f"\n🔍 RAW REFUND DATA PER ORDER:")
print(f"{'─'*70}")

gross_sale    = 0.0
total_returns = 0.0

for o in active_orders:
    oid      = o["id"]
    subtotal = float(o.get("subtotal_price", 0))
    refunds  = o.get("refunds", [])

    print(f"\n  Order #{oid}")
    print(f"    subtotal_price         : {o.get('subtotal_price')}")
    print(f"    current_subtotal_price : {o.get('current_subtotal_price')}")
    print(f"    total_price            : {o.get('total_price')}")
    print(f"    current_total_price    : {o.get('current_total_price')}")
    print(f"    financial_status       : {o.get('financial_status')}")
    print(f"    refunds count          : {len(refunds)}")

    order_returns = 0.0
    for ri, refund in enumerate(refunds):
        rlis = refund.get("refund_line_items", [])
        transactions = refund.get("transactions", [])
        print(f"    refund[{ri}]:")
        print(f"      refund_line_items count : {len(rlis)}")
        for rli in rlis:
            print(f"        rli subtotal={rli.get('subtotal')} qty={rli.get('quantity')} line_item_id={rli.get('line_item_id')}")
            order_returns += float(rli.get("subtotal", 0))
        print(f"      transactions count      : {len(transactions)}")
        for tx in transactions:
            print(f"        tx amount={tx.get('amount')} kind={tx.get('kind')} status={tx.get('status')}")

    order_net = subtotal - order_returns
    gross_sale    += subtotal
    total_returns += order_returns
    print(f"    → gross={subtotal:.2f} | returns={order_returns:.2f} | net={order_net:.2f}")

print(f"\n{'─'*70}")
print(f"📊 TOTALS:")
net_sale = gross_sale - total_returns
print(f"   Gross  : RM{gross_sale:.2f}")
print(f"   Returns: -RM{total_returns:.2f}")
print(f"   Net    : RM{net_sale:.2f}  ← compare this to Shopify Analytics")
print(f"   Orders : {len(active_orders)}")

updated_at = now_my.strftime("%H:%M:%S")

# ── Push to Firestore ─────────────────────────────────────────
doc_ref = db.collection("sales").document("today")
doc_ref.set({
    "currentSale":  round(net_sale, 2),
    "grossSale":    round(gross_sale, 2),
    "totalRefunds": round(total_returns, 2),
    "totalOrders":  len(active_orders),
    "updatedAt":    updated_at,
    "syncedAt":     now_my.isoformat(),
    "source":       "shopify",
}, merge=True)

print(f"\n✅ Firestore synced!")
