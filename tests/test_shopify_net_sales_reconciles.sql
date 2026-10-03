-- Net sales in the daily mart must equal order-level sales minus refunds
-- of real orders. Catches dropped or double-counted refunds and orders.
WITH expected AS (
  SELECT
    (SELECT SUM(net_sales_before_returns) FROM marts.mart_shopify_orders)
    - (SELECT SUM(r.amount)
       FROM staging.stg_shopify__refunds AS r
       JOIN marts.mart_shopify_orders AS o USING (order_id)) AS net_sales
),
actual AS (
  SELECT SUM(net_sales) AS net_sales FROM marts.mart_shopify_daily
)
SELECT e.net_sales AS expected, a.net_sales AS actual
FROM expected AS e
CROSS JOIN actual AS a
WHERE ABS(e.net_sales - a.net_sales) > 0.01
