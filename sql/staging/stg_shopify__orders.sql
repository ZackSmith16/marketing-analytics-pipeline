-- Grain: one row per order (latest version).
--
-- Shopify re-sends an order whenever it changes (for example when it's
-- refunded), so the same order_id can arrive more than once. Keep the most
-- recent version.
--
-- Timestamps arrive in UTC. Orders placed late in the evening would land on
-- the next day, so the order date is converted to the store's time zone.
-- Test and excluded orders are flagged here and filtered out in the marts,
-- so staging still shows everything that arrived.
WITH latest AS (
  SELECT *
  FROM raw.shopify_orders
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
)

SELECT
  o.order_id,
  o.order_number,
  o.store_id,
  o.customer_id,
  o.created_at,
  DATE(o.created_at, 'America/New_York') AS order_date,
  o.sales_channel,
  o.financial_status,
  o.test AS is_test,
  e.order_id IS NOT NULL AS is_excluded,
  e.reason AS exclusion_reason,
  SAFE_CAST(o.total_discounts AS FLOAT64) AS discounts,
  SAFE_CAST(o.total_tax AS FLOAT64) AS tax,
  SAFE_CAST(o.total_shipping AS FLOAT64) AS shipping,
  o.updated_at
FROM latest AS o
LEFT JOIN reference.order_exclusions AS e
  ON CAST(e.order_id AS STRING) = o.order_id
