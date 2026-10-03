-- Spam and test orders must never reach reporting tables.
-- (Replaces a hard-coded NOT IN list of order IDs with an auditable table.)
SELECT m.order_id, s.is_test, s.exclusion_reason
FROM marts.mart_shopify_orders AS m
JOIN staging.stg_shopify__orders AS s USING (order_id)
WHERE s.is_test OR s.is_excluded
