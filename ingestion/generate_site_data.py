"""
Generate synthetic Shopify and GA4 data for the same clients and dates as the
ad data, so all sources line up the way they would for a real client.

Run AFTER generate_ad_data.py. It reads the ad files and drives site traffic
and orders from that day's clicks and conversions.

Outputs (newline-delimited JSON):
    data/raw_shopify_orders.jsonl   - one row per order (line items nested)
    data/raw_shopify_refunds.jsonl  - one row per refund
    data/raw_ga4_sessions.jsonl     - one row per property, day, source,
                                      medium and campaign (connector-style
                                      aggregate, like a GA4 report export)

How the sources relate (all realistic, all deliberate):
    - Ad platforms over-credit themselves. Actual paid-driven revenue is about
      1/1.35 of what Google and Meta report, because both claim the same
      customers and count view-through conversions.
    - The store also sells through organic search, direct, email and referral.
    - GA4 misses ~8% of orders (ad blockers, consent banners), so GA4 revenue
      is a little lower than Shopify revenue.

Deliberate messiness for the pipeline to handle:
    1. Shopify timestamps are UTC. Late-evening orders land on the next UTC
       day, so the order date must be converted to the store's time zone.
    2. Orders are re-sent when they change (a refund updates the order), so
       the same order_id appears twice. Keep the latest updated_at.
    3. Prices and amounts arrive as strings, as in the Shopify API.
    4. A card-testing spam burst: ~85 one-dollar orders in 45 minutes on one
       night. Listed in reference/order_exclusions.csv.
    5. Test orders (test = true) from the store owner checking checkout.
    6. Refunds happen days after the order and are reported on the refund
       date, the way Shopify's own reports do it.
    7. GA4 source/medium values are inconsistent: "facebook / paid_social",
       "fb / paid", "ig / paid_social", "Facebook / CPC", "Klaviyo / Email",
       "(not set)" campaigns, etc. They are grouped into channels with a
       mapping table, not a hard-coded CASE statement.

All names and IDs are invented.
"""

import json
import math
import random
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

random.seed(7)

DATA = Path("data")
REFERENCE = Path("reference")
START = date(2024, 1, 1)
END = date(2026, 9, 29)
STORE_TZ = ZoneInfo("America/New_York")
PLATFORM_OVERCREDIT = 1.35
GA4_TRACKING_RATE = 0.92

CLIENTS = {
    "c01": dict(name="Northwind", type="ecommerce", ga4="318204771",
                store="northwind-candle.myshopify.com",
                google="381-204-5519", meta="act_7712094433",
                nonpaid_ratio=1.3, order_id_start=5812000000001),
    "c02": dict(name="Harbor", type="lead_gen", ga4="318204802", store=None,
                google="552-918-0346", meta="act_3390581126"),
    "c03": dict(name="Summit", type="ecommerce", ga4="318204839",
                store="summit-outdoor.myshopify.com",
                google="704-116-2873", meta="act_9045127768",
                nonpaid_ratio=1.0, order_id_start=6240000000001),
}

# product, price, weight, seasonal months with extra weight
CATALOG = {
    "c01": [
        ("Soy Candle - Cedar & Smoke", 28, 18, ()),
        ("Soy Candle - Fig & Vetiver", 28, 16, ()),
        ("Soy Candle - Sea Salt & Sage", 26, 14, ()),
        ("Travel Tin Trio", 32, 10, ()),
        ("Reed Diffuser - Amber", 38, 8, ()),
        ("Candle Refill Kit", 22, 6, ()),
        ("Wick Trimmer", 16, 5, ()),
        ("Holiday Gift Set", 64, 3, (10, 11, 12)),
    ],
    "c03": [
        ("Insulated Water Bottle 32oz", 34, 16, ()),
        ("Headlamp 400lm", 45, 13, ()),
        ("Merino Base Layer", 79, 11, (10, 11, 12, 1, 2)),
        ("Trekking Poles (Pair)", 89, 9, (4, 5, 6, 7, 8, 9)),
        ("Camp Stove Kit", 64, 8, (5, 6, 7, 8)),
        ("Down Sleeping Bag 20F", 219, 5, ()),
        ("Ultralight Tent 2P", 289, 4, (4, 5, 6, 7)),
        ("Winter Shell Jacket", 249, 4, (10, 11, 12, 1, 2)),
    ],
}

# Non-paid revenue split and GA4 behaviour by source group.
# cr = order conversion rate per session, eng = engaged-session rate,
# new = share of sessions from new users, ret = chance an order is from a
# returning customer.
GROUPS = {
    "google_paid": dict(eng=0.62, new=0.72, ret=0.25),
    "meta_paid":   dict(eng=0.46, new=0.78, ret=0.22),
    "organic":     dict(share=0.38, cr=0.022, eng=0.66, new=0.62, ret=0.35),
    "direct":      dict(share=0.30, cr=0.028, eng=0.55, new=0.30, ret=0.55),
    "email":       dict(share=0.22, cr=0.035, eng=0.70, new=0.12, ret=0.75),
    "referral":    dict(share=0.10, cr=0.018, eng=0.50, new=0.70, ret=0.30),
}

# How each group shows up in GA4: (source, medium, campaign, weight)
GA4_VARIANTS = {
    "meta_paid": [("facebook", "paid_social", 55), ("ig", "paid_social", 25),
                  ("fb", "paid", 15), ("Facebook", "CPC", 5)],
    "organic": [("google", "organic", None, 88), ("bing", "organic", None, 9),
                ("duckduckgo", "organic", None, 3)],
    "direct": [("(direct)", "(none)", None, 97), ("(not set)", "(not set)", None, 3)],
    "email": [("klaviyo", "email", "Weekly Newsletter", 45),
              ("klaviyo", "email", "Abandoned Cart Flow", 25),
              ("Klaviyo", "Email", "Welcome Series", 20),
              ("klaviyo", "email", None, 10)],
    "referral": [("pinterest.com", "referral", None, 45),
                 ("linktr.ee", "referral", None, 30),
                 ("chatgpt.com", "referral", None, 25)],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def noise(mean=1.0, sd=0.1, low=0.3, high=2.0):
    return min(max(random.gauss(mean, sd), low), high)


def poisson(lam):
    if lam <= 0:
        return 0
    if lam > 50:
        return max(0, round(random.gauss(lam, math.sqrt(lam))))
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= random.random()
        if p <= threshold:
            return k
        k += 1


def weighted(options, weights):
    return random.choices(options, weights=weights, k=1)[0]


def daterange(a, b):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def iso_utc(dt_local):
    return dt_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def local_time_on(d):
    """Random order time, weighted toward evenings like real stores."""
    hour = weighted(list(range(24)),
                    [1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7, 7, 7, 7, 7, 8, 9, 10, 10, 9, 6, 3])
    return datetime(d.year, d.month, d.day, hour, random.randint(0, 59),
                    random.randint(0, 59), tzinfo=STORE_TZ)


# ---------------------------------------------------------------------------
# Read ad data (latest sync of each campaign-day)
# ---------------------------------------------------------------------------

def load_latest(path, key=("date", "account_id", "campaign_id")):
    latest = {}
    for line in open(path):
        r = json.loads(line)
        k = tuple(r[x] for x in key)
        if k not in latest or r["_synced_at"] > latest[k]["_synced_at"]:
            latest[k] = r
    return list(latest.values())


def ad_daily():
    """Per client and day: platform-reported value/conversions, plus clicks by campaign."""
    acct_to_client = {}
    for cid, c in CLIENTS.items():
        acct_to_client[c["google"]] = (cid, "google_paid")
        acct_to_client[c["meta"]] = (cid, "meta_paid")

    days = defaultdict(lambda: defaultdict(lambda: dict(value=0.0, conv=0.0, campaigns=[])))
    for r in load_latest(DATA / "raw_google_ads_campaigns.jsonl"):
        cid, grp = acct_to_client[r["account_id"]]
        g = days[(cid, r["date"])][grp]
        g["value"] += r["conversions_value"]
        g["conv"] += r["conversions"]
        g["campaigns"].append((r["campaign_name"], r["clicks"], r["conversions"]))
    for r in load_latest(DATA / "raw_meta_ads_campaigns.jsonl"):
        cid, grp = acct_to_client[r["account_id"]]
        g = days[(cid, r["date"])][grp]
        acts = {a["action_type"]: float(a["value"]) for a in r["actions"]}
        vals = {a["action_type"]: float(a["value"]) for a in r["action_values"]}
        conv = acts.get("purchase", acts.get("lead", 0.0))
        g["value"] += vals.get("purchase", 0.0)
        g["conv"] += conv
        g["campaigns"].append((r["campaign_name"].strip(), r["clicks"], conv))
    return days


# ---------------------------------------------------------------------------
# Shopify
# ---------------------------------------------------------------------------

class Store:
    def __init__(self, cid):
        c = CLIENTS[cid]
        self.cid, self.store_id = cid, c["store"]
        self.next_order_id = c["order_id_start"]
        self.next_order_number = 1001
        self.next_customer = 1
        self.customers = []
        self.orders, self.refunds = [], []
        self.line_seq = 9000000000001 if cid == "c01" else 9500000000001
        self.refund_seq = 880000001 if cid == "c01" else 990000001

    def customer(self, returning_prob):
        if self.customers and random.random() < returning_prob:
            # recent customers are more likely to come back
            idx = min(len(self.customers) - 1,
                      int(random.expovariate(1 / 400)))
            return self.customers[-1 - idx]
        cust = f"{self.cid}-cust-{self.next_customer:06d}"
        self.next_customer += 1
        self.customers.append(cust)
        return cust

    def basket(self, d):
        cat = CATALOG[self.cid]
        # seasonal items sell mostly in season
        weights = [w * (3 if d.month in months else (0.15 if months else 1))
                   for _, _, w, months in cat]
        items = []
        for _ in range(weighted([1, 2, 3], [60, 28, 12])):
            name, price, _, _ = weighted(cat, weights)
            items.append((name, price, weighted([1, 2], [88, 12])))
        return items

    def make_order(self, d, group, *, when=None, items=None, customer=None,
                   discount=None, test=False):
        when = when or local_time_on(d)
        items = items or self.basket(d)
        subtotal = sum(p * q for _, p, q in items)
        if discount is None:
            pct = weighted([0, 0.10, 0.15], [82, 12, 6])
            if group == "email":
                pct = weighted([0, 0.10, 0.15], [55, 30, 15])
            discount = round(subtotal * pct, 2)
        cust = customer or self.customer(GROUPS[group]["ret"])
        order = {
            "order_id": str(self.next_order_id),
            "order_number": f"#{self.next_order_number}",
            "store_id": self.store_id,
            "customer_id": cust,
            "created_at": iso_utc(when),
            "updated_at": iso_utc(when + timedelta(minutes=random.randint(1, 30))),
            "sales_channel": weighted(
                ["Online Store", "Shop", "Facebook & Instagram", "Draft Orders"],
                [88, 6, 4, 2]) if not test else "Online Store",
            "financial_status": "paid",
            "test": test,
            "total_discounts": f"{discount:.2f}",
            "total_tax": f"{(subtotal - discount) * 0.07:.2f}",
            "total_shipping": "0.00" if subtotal - discount >= 75 else "6.95",
            "line_items": [],
        }
        for name, price, qty in items:
            order["line_items"].append({
                "line_item_id": str(self.line_seq), "product_title": name,
                "quantity": qty, "price": f"{price:.2f}"})
            self.line_seq += 1
        self.next_order_id += 1
        self.next_order_number += 1
        self.orders.append(order)
        return order, subtotal - discount, when

    def maybe_refund(self, order, net, when):
        if order["test"] or random.random() > 0.045:
            return
        refund_time = when + timedelta(days=random.randint(2, 21), hours=random.randint(0, 8))
        if refund_time.date() > END:
            return
        amount = net if random.random() < 0.7 else round(net * random.uniform(0.3, 0.6), 2)
        self.refunds.append({
            "refund_id": str(self.refund_seq), "order_id": order["order_id"],
            "created_at": iso_utc(refund_time), "amount": f"{amount:.2f}",
            "reason": weighted(["damaged", "not as described", "changed mind", "other"],
                               [30, 20, 40, 10]),
        })
        self.refund_seq += 1
        # Shopify re-sends the order when it changes: same order_id, newer updated_at
        resent = json.loads(json.dumps(order))
        resent["updated_at"] = iso_utc(refund_time)
        resent["financial_status"] = "refunded" if amount == net else "partially_refunded"
        self.orders.append(resent)


def spam_burst(store, d):
    """Card testing: many $1 orders from throwaway customers in under an hour."""
    ids = []
    start = datetime(d.year, d.month, d.day, 2, 3, tzinfo=STORE_TZ)
    for i in range(85):
        when = start + timedelta(seconds=int(i * 31 + random.randint(0, 20)))
        order, _, _ = store.make_order(
            d, "direct", when=when, items=[("Wick Trimmer", 16, 1)],
            customer=f"spam-{random.randint(10**7, 10**8 - 1)}", discount=15.00)
        ids.append(order["order_id"])
    return ids


# ---------------------------------------------------------------------------
# GA4
# ---------------------------------------------------------------------------

def ga4_rows_for_day(cid, d, days_ad, orders_by_group):
    """Session rows for one property-day, with tracked orders attached."""
    c = CLIENTS[cid]
    rows = []

    def add(source, medium, campaign, sessions, group):
        if sessions <= 0:
            return
        g = GROUPS[group]
        rows.append(dict(
            date=d.isoformat(), property_id=c["ga4"], session_source=source,
            session_medium=medium, session_campaign_name=campaign,
            sessions=sessions,
            engaged_sessions=round(sessions * g["eng"] * noise(1, 0.05, 0.8, 1.2)),
            new_users=round(sessions * g["new"] * noise(1, 0.06, 0.8, 1.2)),
            total_users=round(sessions * 0.86),
            key_events=0, purchase_revenue=0.0, _group=group))

    ad = days_ad.get((cid, d.isoformat()), {})
    # Paid search: one row per Google campaign, campaign name as of that day
    for name, clicks, _ in ad.get("google_paid", {}).get("campaigns", []):
        add("google", "cpc", name, round(clicks * 0.88 * noise(1, 0.05)), "google_paid")
    # Paid social: the same campaign shows up under several source/medium spellings
    for name, clicks, _ in ad.get("meta_paid", {}).get("campaigns", []):
        total = round(clicks * 0.72 * noise(1, 0.06))
        variants = GA4_VARIANTS["meta_paid"]
        weights = [v[2] for v in variants]
        for (src, med, _), w in zip(variants, weights):
            add(src, med, name, round(total * w / sum(weights)), "meta_paid")

    # Non-paid: sessions back-solved from orders and conversion rate
    for group in ("organic", "direct", "email", "referral"):
        if c["type"] == "ecommerce":
            n = len(orders_by_group.get(group, []))
            total = round((n + 0.5) / GROUPS[group]["cr"] * noise(1, 0.08))
        else:  # lead gen: steady baseline traffic
            base = dict(organic=95, direct=60, email=12, referral=10)[group]
            weekday = 0.65 if d.weekday() >= 5 else 1.0
            total = round(base * weekday * noise(1, 0.15))
        variants = GA4_VARIANTS[group]
        weights = [v[3] for v in variants]
        for (src, med, camp, _), w in zip(variants, weights):
            add(src, med, camp, round(total * w / sum(weights)), group)

    # Attach conversions
    if c["type"] == "ecommerce":
        for group, orders in orders_by_group.items():
            candidates = [r for r in rows if r["_group"] == group]
            if not candidates:
                candidates = [r for r in rows if r["_group"] == "direct"] or rows
            for net in orders:
                if random.random() > GA4_TRACKING_RATE:
                    continue  # blocked by consent banner / ad blocker
                r = weighted(candidates, [x["sessions"] for x in candidates])
                r["key_events"] += 1
                r["purchase_revenue"] = round(r["purchase_revenue"] + net, 2)
    else:
        for group in ("google_paid", "meta_paid"):
            leads = poisson(ad.get(group, {}).get("conv", 0) / PLATFORM_OVERCREDIT)
            candidates = [r for r in rows if r["_group"] == group]
            for _ in range(leads if candidates else 0):
                weighted(candidates, [x["sessions"] for x in candidates])["key_events"] += 1
        for r in rows:
            if r["_group"] not in ("google_paid", "meta_paid"):
                r["key_events"] += poisson(r["sessions"] * 0.035)

    for r in rows:
        del r["_group"]
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    days_ad = ad_daily()
    stores = {cid: Store(cid) for cid, c in CLIENTS.items() if c["store"]}
    ga4 = []
    exclusions = []

    for d in daterange(START, END):
        for cid, c in CLIENTS.items():
            ad = days_ad.get((cid, d.isoformat()), {})
            orders_by_group = defaultdict(list)

            if c["type"] == "ecommerce":
                store = stores[cid]
                paid = {g: ad.get(g, {}).get("value", 0.0) / PLATFORM_OVERCREDIT
                        for g in ("google_paid", "meta_paid")}
                nonpaid_total = sum(paid.values()) * c["nonpaid_ratio"] * noise(1, 0.12)
                targets = dict(paid)
                for g in ("organic", "direct", "email", "referral"):
                    targets[g] = nonpaid_total * GROUPS[g]["share"]
                avg_order = sum(p * w for _, p, w, _ in CATALOG[cid]) / \
                    sum(w for *_, w, _ in CATALOG[cid]) * 1.65 * 0.97
                for group, revenue in targets.items():
                    for _ in range(poisson(revenue / avg_order)):
                        order, net, when = store.make_order(d, group)
                        store.maybe_refund(order, net, when)
                        orders_by_group[group].append(net)

                if cid == "c01" and d == date(2025, 11, 3):
                    for oid in spam_burst(store, d):
                        exclusions.append((oid, "spam: card-testing burst", "2025-11-03"))
                if d.day == 15 and d.month in (1, 6) and d.year in (2024, 2025):
                    store.make_order(d, "direct", test=True)  # owner testing checkout

            ga4.extend(ga4_rows_for_day(cid, d, days_ad, orders_by_group))

    # Connector re-syncs a few GA4 days with slightly updated numbers
    resent = []
    for r in random.sample(ga4, 40):
        dup = dict(r, sessions=r["sessions"] + random.randint(0, 3))
        resent.append(dup)
    synced = lambda r, late: (datetime.fromisoformat(r["date"]) +
                              timedelta(days=4 if late else 1, hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for r in ga4:
        r["_synced_at"] = synced(r, False)
    for r in resent:
        r["_synced_at"] = synced(r, True)
    ga4 += resent

    def write(path, rows):
        with open(path, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        print(f"{path}: {len(rows):,} rows")

    orders = [o for s in stores.values() for o in s.orders]
    refunds = [r for s in stores.values() for r in s.refunds]
    write(DATA / "raw_shopify_orders.jsonl", orders)
    write(DATA / "raw_shopify_refunds.jsonl", refunds)
    write(DATA / "raw_ga4_sessions.jsonl", ga4)

    REFERENCE.mkdir(exist_ok=True)
    with open(REFERENCE / "order_exclusions.csv", "w") as f:
        f.write("order_id,reason,flagged_on\n")
        for oid, reason, flagged in exclusions:
            f.write(f"{oid},{reason},{flagged}\n")
    print(f"reference/order_exclusions.csv: {len(exclusions)} rows")


if __name__ == "__main__":
    main()
