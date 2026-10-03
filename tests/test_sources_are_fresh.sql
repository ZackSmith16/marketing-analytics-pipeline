-- Every connected account (ad platforms, Shopify stores, GA4 properties)
-- should have data through (nearly) the latest date loaded.
-- Catches a source that silently stops syncing partway through a month,
-- which otherwise shows up as a sudden drop or "No data" on a report.
--
-- Accounts are compared to the newest date in the warehouse, not to today,
-- so the check also works on the static sample data. In production you'd
-- compare to CURRENT_DATE() instead.
WITH latest_by_account AS (
  SELECT platform, account_id, MAX(date) AS latest_date
  FROM marts.mart_campaign_daily
  WHERE cost > 0
  GROUP BY 1, 2
  UNION ALL
  SELECT 'shopify', store_id, MAX(order_date)
  FROM staging.stg_shopify__orders
  GROUP BY 1, 2
  UNION ALL
  SELECT 'ga4', property_id, MAX(date)
  FROM staging.stg_ga4__sessions
  GROUP BY 1, 2
),
latest_overall AS (
  SELECT MAX(latest_date) AS latest_date FROM latest_by_account
)
SELECT
  c.client_name,
  c.platform,
  c.account_id,
  a.latest_date,
  o.latest_date AS warehouse_latest_date
FROM staging.stg_clients AS c
LEFT JOIN latest_by_account AS a
  ON a.platform = c.platform
  AND a.account_id = c.account_id
CROSS JOIN latest_overall AS o
WHERE a.latest_date IS NULL
   OR DATE_DIFF(o.latest_date, a.latest_date, DAY) > 2
