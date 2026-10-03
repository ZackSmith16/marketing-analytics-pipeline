-- Grain: one row per refund.
-- Refunds are reported on the date they happen, not the original order date,
-- which matches how Shopify's own sales reports treat returns.
SELECT
  refund_id,
  order_id,
  created_at,
  DATE(created_at, 'America/New_York') AS refund_date,
  SAFE_CAST(amount AS FLOAT64) AS amount,
  reason
FROM raw.shopify_refunds
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY refund_id ORDER BY created_at DESC) = 1
