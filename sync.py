"""
diag_day.py  —  READ-ONLY daily net-sales diagnostic for Gearevo.

Does NOT write anything and does NOT change sync.py. It shows, for one MYT day,
every order created that day and every refund PROCESSED that day, so you can see
exactly what makes up the difference vs Shopify's net sales.

RUN (on your machine):
    pip install requests
    export SHOPIFY_STORE=gearevo.myshopify.com
    export SHOPIFY_TOKEN=shpat_xxxxxxxxxxxx
    python diag_day.py                 # today
    python diag_day.py 2026-06-24      # a specific day

COMPARE the two NET figures at the bottom against Shopify's net sales for that day:
    NET (incl cancelled refunds)  = what sync.py currently produces
    NET (excl cancelled refunds)  = what it'd be if we skip cancelled-order refunds
Whichever matches Shopify tells us the correct rule.
"""

import os, sys
from datetime import datetime, timezone, timedelta

import requests

SHOPIFY_STORE = os.environ["SHOPIFY_STORE"]
SHOPIFY_TOKEN = os.environ["SHOPIFY_TOKEN"]
headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}
MY_TZ = timezone(timedelta(hours=8))

# Window: a full MYT day, or midnight->now if it's today
now_my = datetime.now(MY_TZ)
if len(sys.argv) > 1:
    d = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    start_my = datetime(d.year, d.month, d.day, tzinfo=MY_TZ)
    end_my = start_my + timedelta(days=1)
    if start_my.date() == now_my.date():
        end_my = now_my
else:
    start_my = datetime(now_my.year, now_my.month, now_my.day, tzinfo=MY_TZ)
    end_my = now_my

start_utc, end_utc = start_my.astimezone(timezone.utc), end_my.astimezone(timezone.utc)
S = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
E = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch(params):
    out, url, p = [], f"https://{SHOPIFY_STORE}/admin/api/2024-01/orders.json", dict(params)
    while url:
        r = requests.get(url, params=p, headers=headers)
        if r.status_code == 429:
            import time; time.sleep(float(r.headers.get("Retry-After", 2))); continue
        r.raise_for_status()
        out.extend(r.json().get("orders", []))
        link, url, p = r.headers.get("Link", ""), None, {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return out


def pdt(s):
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


print(f"Window (MYT): {start_my:%Y-%m-%d %H:%M} -> {end_my:%Y-%m-%d %H:%M}\n")

created = fetch({"created_at_min": S, "created_at_max": E, "status": "any",
                 "financial_status": "any", "limit": 250,
                 "fields": "id,order_number,created_at,subtotal_price,total_discounts,financial_status,cancel_reason"})
updated = fetch({"updated_at_min": S, "updated_at_max": E, "status": "any",
                 "financial_status": "any", "limit": 250,
                 "fields": "id,order_number,created_at,cancel_reason,refunds"})

active = [o for o in created if o.get("cancel_reason") is None]
cancelled = [o for o in created if o.get("cancel_reason") is not None]
subtotal_active = sum(float(o.get("subtotal_price", 0)) for o in active)

print("ORDERS CREATED THIS DAY")
print(f"  {'Order':<9}{'Subtotal':>11}{'Discount':>11}  {'Status':<12}{'Cancelled?':<10}")
for o in sorted(created, key=lambda x: x.get("created_at", "")):
    print(f"  #{str(o.get('order_number','')):<8}"
          f"{float(o.get('subtotal_price',0)):>11.2f}"
          f"{float(o.get('total_discounts',0)):>11.2f}  "
          f"{str(o.get('financial_status','')):<12}"
          f"{('CANCELLED' if o.get('cancel_reason') else ''):<10}")
print(f"  -> {len(created)} orders ({len(active)} active, {len(cancelled)} cancelled), "
      f"active subtotal RM {subtotal_active:,.2f}")

print("\nREFUNDS PROCESSED THIS DAY (any order)")
print(f"  {'Order':<9}{'OrderDate':<12}{'RefundProcessed':<22}{'Amount':>11}  {'OrderCancelled?'}")
ref_all = 0.0
ref_excl_cancelled = 0.0
for o in updated:
    o_cancelled = o.get("cancel_reason") is not None
    for refund in o.get("refunds", []):
        rdt = pdt(refund.get("processed_at") or refund.get("created_at"))
        if rdt is None or not (start_utc <= rdt < end_utc):
            continue
        amt = sum(float(rli.get("subtotal", 0)) for rli in refund.get("refund_line_items", []))
        if amt == 0:
            continue
        ref_all += amt
        if not o_cancelled:
            ref_excl_cancelled += amt
        od = (o.get("created_at") or "")[:10]
        print(f"  #{str(o.get('order_number','')):<8}{od:<12}"
              f"{rdt.astimezone(MY_TZ):%Y-%m-%d %H:%M}{'':<6}{amt:>11.2f}  {'YES' if o_cancelled else ''}")
print(f"  -> refunds total RM {ref_all:,.2f}   (excl. cancelled-order refunds: RM {ref_excl_cancelled:,.2f})")

print("\n" + "=" * 60)
print(f"  Active subtotal              : RM {subtotal_active:>12,.2f}")
print(f"  NET (incl cancelled refunds) : RM {subtotal_active - ref_all:>12,.2f}   <- sync.py now")
print(f"  NET (excl cancelled refunds) : RM {subtotal_active - ref_excl_cancelled:>12,.2f}")
print(f"  Order count (incl cancelled) : {len(created)}")
print("=" * 60)
