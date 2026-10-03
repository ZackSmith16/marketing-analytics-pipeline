"""
Generate synthetic raw ad platform data, shaped like the output of a
connector tool (Funnel, Windsor, Fivetran, etc.) landing in BigQuery.

Outputs (newline-delimited JSON, ready to upload to BigQuery):
    data/raw_clients.jsonl               - which ad account belongs to which client
    data/raw_google_ads_campaigns.jsonl  - one row per campaign per day
    data/raw_meta_ads_campaigns.jsonl    - one row per campaign per day, conversions nested

Deliberate real-world messiness for the pipeline to handle:
    1. Connector re-syncs: some days appear twice. The later sync (higher
       _synced_at) has slightly updated conversions because of attribution lag,
       so deduplication must keep the LATEST row, not just DISTINCT.
    2. A Google campaign is renamed mid-year (same campaign_id, new name).
    3. A Meta campaign name has a trailing space.
    4. Meta reports the same purchase under several action types
       (purchase, offsite_conversion.fb_pixel_purchase, omni_purchase).
       Summing all of them double/triple counts.
    5. Meta omits actions with a value of zero, and returns action values
       as strings, not numbers.
    6. Campaigns launch, end, and run seasonally, and platforms don't
       return rows for days with no delivery.

Covers Jan 2024 through Sep 2026 so reports can compare year over year.
Budgets grow each year, and seasonal campaigns return every year.

All client names, account IDs, and campaign IDs are invented.
"""

import json
import math
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

random.seed(42)  # same "random" data every run

START = date(2024, 1, 1)
END = date(2026, 9, 29)
OUT_DIR = Path("data")


# ---------------------------------------------------------------------------
# Clients and campaigns
# ---------------------------------------------------------------------------

CLIENTS = [
    {
        "client_id": "c01",
        "client_name": "Northwind Candle Co.",
        "client_type": "ecommerce",
        "annual_growth": 0.20,
        "google_account_id": "381-204-5519",
        "shopify_store_id": "northwind-candle.myshopify.com",
        "ga4_property_id": "318204771",
        "meta_account_id": "act_7712094433",
    },
    {
        "client_id": "c02",
        "client_name": "Harbor Dental Group",
        "client_type": "lead_gen",
        "annual_growth": 0.08,
        "google_account_id": "552-918-0346",
        "shopify_store_id": None,
        "ga4_property_id": "318204802",
        "meta_account_id": "act_3390581126",
    },
    {
        "client_id": "c03",
        "client_name": "Summit Outdoor Supply",
        "client_type": "ecommerce",
        "annual_growth": 0.15,
        "google_account_id": "704-116-2873",
        "shopify_store_id": "summit-outdoor.myshopify.com",
        "ga4_property_id": "318204839",
        "meta_account_id": "act_9045127768",
    },
]

# budget = average daily spend in the first year, cpc/cpm = cost per click /
# per 1,000 impressions, ctr = click-through rate, cvr = conversion rate,
# aov = average order value.
# start/end = one-time launch or shutdown dates.
# season = ((month, day), (month, day)) window that repeats every year.
# season="bfcm" runs the week around Black Friday / Cyber Monday each year.
GOOGLE_CAMPAIGNS = {
    "c01": [
        dict(name="Brand | Search", budget=60, cpc=0.90, ctr=0.12, cvr=0.080, aov=48),
        dict(name="Non-Brand | Search | Candles", budget=150, cpc=1.60, ctr=0.045, cvr=0.025, aov=45,
             renamed_on=date(2025, 6, 15), new_name="NB | Search | Soy Candles"),
        dict(name="Performance Max | All Products", budget=200, cpc=0.70, ctr=0.020, cvr=0.018, aov=52,
             start=date(2025, 3, 1)),
        dict(name="Shopping | Holiday Gift Sets", budget=120, cpc=0.80, ctr=0.018, cvr=0.030, aov=65,
             season=((10, 15), (12, 24))),
    ],
    "c02": [
        dict(name="Brand | Search", budget=30, cpc=2.50, ctr=0.15, cvr=0.12),
        dict(name="Emergency Dentist | Search", budget=120, cpc=9.50, ctr=0.06, cvr=0.09),
        dict(name="Invisalign | Search", budget=90, cpc=7.00, ctr=0.05, cvr=0.05,
             end=date(2025, 8, 31)),
    ],
    "c03": [
        dict(name="Brand | Search", budget=50, cpc=1.10, ctr=0.10, cvr=0.070, aov=120),
        dict(name="Shopping | Camping Gear", budget=220, cpc=0.95, ctr=0.015, cvr=0.020, aov=135),
        dict(name="Performance Max | Winter", budget=180, cpc=0.85, ctr=0.017, cvr=0.019, aov=160,
             season=((10, 1), (2, 28))),
    ],
}

META_CAMPAIGNS = {
    "c01": [
        dict(name="ECOM_Prospecting_Broad", budget=180, cpm=14, ctr=0.011, cvr=0.018, aov=47),
        dict(name="ECOM_Retargeting_Cart Abandoners ", budget=50, cpm=22, ctr=0.020, cvr=0.050, aov=50),
        dict(name="ECOM_BFCM_Sale", budget=300, cpm=20, ctr=0.016, cvr=0.035, aov=58,
             season="bfcm"),
    ],
    "c02": [
        dict(name="LEADGEN_New Patient Offer_Instant Form", budget=70, cpm=11, ctr=0.012, cvr=0.080,
             lead_type="instant_form"),
        dict(name="LEADGEN_Whitening Promo", budget=40, cpm=13, ctr=0.010, cvr=0.060,
             lead_type="website", season=((4, 1), (6, 30))),
    ],
    "c03": [
        dict(name="ECOM_Prospecting_Lookalike 2%", budget=150, cpm=12, ctr=0.009, cvr=0.014, aov=125),
        dict(name="ECOM_Retargeting_Site Visitors 30D", budget=45, cpm=19, ctr=0.018, cvr=0.040, aov=130),
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def noise(mean=1.0, sd=0.1, low=0.3, high=2.0):
    """A random multiplier around `mean`, clipped so nothing goes wild."""
    return min(max(random.gauss(mean, sd), low), high)


def poisson(lam):
    """Random whole-number count with average `lam` (e.g. orders in a day)."""
    if lam <= 0:
        return 0
    if lam > 50:  # normal approximation for large averages
        return max(0, round(random.gauss(lam, math.sqrt(lam))))
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= random.random()
        if p <= threshold:
            return k
        k += 1


def seasonality(d, client_type):
    """Demand multiplier for a given day."""
    weekday = d.weekday()  # Monday = 0
    if client_type == "ecommerce":
        m = [0.95, 0.97, 1.00, 1.00, 1.03, 1.08, 1.05][weekday]
        m *= {1: 0.85, 11: 1.35, 12: 1.30}.get(d.month, 1.0)
        if in_bfcm(d):
            m *= 1.6
    else:
        m = 0.70 if weekday >= 5 else 1.05  # dental offices: quiet weekends
        m *= {1: 1.15, 12: 0.85}.get(d.month, 1.0)
    return m


def black_friday(year):
    """Day after the fourth Thursday of November."""
    nov1 = date(year, 11, 1)
    first_thursday = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    return first_thursday + timedelta(weeks=3, days=1)


def in_bfcm(d):
    bf = black_friday(d.year)
    return bf - timedelta(days=4) <= d <= bf + timedelta(days=3)  # through Cyber Monday


def in_season(season, d):
    if season == "bfcm":
        return in_bfcm(d)
    (m1, d1), (m2, d2) = season
    start, end = (m1, d1), (m2, d2)
    today = (d.month, d.day)
    if start <= end:
        return start <= today <= end
    return today >= start or today <= end  # window wraps past New Year


def is_active(camp, d):
    if not camp.get("start", START) <= d <= camp.get("end", END):
        return False
    return in_season(camp["season"], d) if "season" in camp else True


def growth(client, d):
    """Budget multiplier: clients spend more each year."""
    years = (d - START).days / 365.25
    return (1 + client["annual_growth"]) ** years


def campaign_name_on(camp, d):
    if "renamed_on" in camp and d >= camp["renamed_on"]:
        return camp["new_name"]
    return camp["name"]


def synced_at(d, days_later=1):
    """Connectors load yesterday's data early the next morning (UTC)."""
    t = datetime(d.year, d.month, d.day, 6, random.randint(0, 59), random.randint(0, 59),
                 tzinfo=timezone.utc) + timedelta(days=days_later)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def new_id(digits):
    return str(random.randint(10 ** (digits - 1), 10 ** digits - 1))


# ---------------------------------------------------------------------------
# Google Ads
# ---------------------------------------------------------------------------

def google_rows():
    rows = []
    for client in CLIENTS:
        cid, ctype = client["client_id"], client["client_type"]
        for camp in GOOGLE_CAMPAIGNS[cid]:
            camp_id = new_id(11)
            for d in daterange(START, END):
                if not is_active(camp, d):
                    continue
                s = seasonality(d, ctype)
                cost = camp["budget"] * growth(client, d) * noise(0.95, 0.08, 0.6, 1.15) * (1 + 0.5 * (s - 1))
                clicks = round(cost / (camp["cpc"] * noise(1, 0.10)))
                if clicks == 0:
                    continue  # no delivery = no row
                impressions = round(clicks / (camp["ctr"] * noise(1, 0.12)))
                orders = poisson(clicks * camp["cvr"] * s * noise(1, 0.15))
                # Data-driven attribution hands out fractional credit
                conversions = round(orders * random.uniform(0.85, 1.0), 2) if orders else 0.0
                value = round(conversions * camp["aov"] * noise(1, 0.15), 2) if "aov" in camp else 0.0
                rows.append({
                    "date": d.isoformat(),
                    "account_id": client["google_account_id"],
                    "campaign_id": camp_id,
                    "campaign_name": campaign_name_on(camp, d),
                    "impressions": impressions,
                    "clicks": clicks,
                    "cost": round(cost, 2),
                    "conversions": conversions,
                    "conversions_value": value,
                    "_synced_at": synced_at(d),
                })
    return rows


# ---------------------------------------------------------------------------
# Meta Ads
# ---------------------------------------------------------------------------

def meta_actions(camp, clicks, results, value):
    """Build Meta's nested actions / action_values arrays.
    Zero-value actions are omitted and values are strings, as in the real API."""
    actions = {"link_click": clicks, "landing_page_view": round(clicks * noise(0.8, 0.05))}
    action_values = {}
    if "aov" in camp:  # e-commerce: the same purchase appears under three names
        actions["add_to_cart"] = round(results * noise(4, 0.5, 2, 6))
        actions["initiate_checkout"] = round(results * noise(1.8, 0.2, 1.2, 2.5))
        for t in ("purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase"):
            actions[t] = results
            action_values[t] = value
    elif camp["lead_type"] == "instant_form":
        actions["lead"] = results
        actions["onsite_conversion.lead_grouped"] = results
    else:
        actions["lead"] = results
        actions["offsite_conversion.fb_pixel_lead"] = results

    def to_list(d):
        return [{"action_type": k, "value": str(v)} for k, v in d.items() if v]

    return to_list(actions), to_list(action_values)


def meta_rows():
    rows = []
    for client in CLIENTS:
        cid, ctype = client["client_id"], client["client_type"]
        for camp in META_CAMPAIGNS[cid]:
            camp_id = new_id(18)
            objective = "OUTCOME_SALES" if "aov" in camp else "OUTCOME_LEADS"
            for d in daterange(START, END):
                if not is_active(camp, d):
                    continue
                s = seasonality(d, ctype)
                spend = camp["budget"] * growth(client, d) * noise(0.95, 0.08, 0.6, 1.15) * (1 + 0.5 * (s - 1))
                impressions = round(spend / (camp["cpm"] * noise(1, 0.12)) * 1000)
                clicks = round(impressions * camp["ctr"] * noise(1, 0.15))
                if impressions == 0:
                    continue
                results = poisson(clicks * camp["cvr"] * s * noise(1, 0.15))
                value = round(results * camp["aov"] * noise(1, 0.15), 2) if "aov" in camp else 0
                actions, action_values = meta_actions(camp, clicks, results, value)
                rows.append({
                    "date": d.isoformat(),
                    "account_id": client["meta_account_id"],
                    "campaign_id": camp_id,
                    "campaign_name": camp["name"],
                    "objective": objective,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend": round(spend, 2),
                    "actions": actions,
                    "action_values": action_values,
                    "_synced_at": synced_at(d),
                })
    return rows


# ---------------------------------------------------------------------------
# Connector re-syncs (duplicates)
# ---------------------------------------------------------------------------

def add_resync_duplicates(rows, account_id, n_days, conversion_bump):
    """Re-send every row for `n_days` random days of one account, two days later,
    with conversions nudged up (late-arriving attribution)."""
    days = sorted({r["date"] for r in rows if r["account_id"] == account_id})
    for day in random.sample(days, n_days):
        for r in [r for r in rows if r["account_id"] == account_id and r["date"] == day]:
            dup = json.loads(json.dumps(r))  # deep copy
            dup["_synced_at"] = synced_at(date.fromisoformat(day), days_later=3)
            conversion_bump(dup)
            rows.append(dup)
    return rows


def bump_google(row):
    if row["conversions"]:
        row["conversions"] = round(row["conversions"] + random.choice([0.5, 1.0]), 2)


def bump_meta(row):
    for a in row["actions"]:
        if a["action_type"] in ("purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase"):
            a["value"] = str(int(a["value"]) + 1)


# ---------------------------------------------------------------------------
# Write files
# ---------------------------------------------------------------------------

def write_jsonl(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"{path}: {len(rows):,} rows")


def main():
    OUT_DIR.mkdir(exist_ok=True)

    clients = []
    for c in CLIENTS:
        accounts = {
            "google_ads": c["google_account_id"],
            "meta_ads": c["meta_account_id"],
            "shopify": c["shopify_store_id"],   # lead-gen clients have no store
            "ga4": c["ga4_property_id"],
        }
        for platform, account_id in accounts.items():
            if account_id is None:
                continue
            clients.append({
                "client_id": c["client_id"],
                "client_name": c["client_name"],
                "client_type": c["client_type"],
                "platform": platform,
                "account_id": account_id,
            })

    google = add_resync_duplicates(google_rows(), "381-204-5519", n_days=6, conversion_bump=bump_google)
    meta = add_resync_duplicates(meta_rows(), "act_9045127768", n_days=4, conversion_bump=bump_meta)

    write_jsonl(OUT_DIR / "raw_clients.jsonl", clients)
    write_jsonl(OUT_DIR / "raw_google_ads_campaigns.jsonl", google)
    write_jsonl(OUT_DIR / "raw_meta_ads_campaigns.jsonl", meta)


if __name__ == "__main__":
    main()
