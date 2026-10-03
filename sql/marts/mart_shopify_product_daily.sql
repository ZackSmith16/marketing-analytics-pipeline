-- Product sales by day, for "top products" tables.
-- Grain: one row per client, product, and day.
SELECT
  o.date,
  o.client_id,
  o.client_name,
  li.product_title,
  COUNT(DISTINCT o.order_id) AS orders,
  SUM(li.quantity) AS units,
  SUM(li.gross_sales) AS gross_sales
FROM marts.mart_shopify_orders AS o
JOIN staging.stg_shopify__line_items AS li
  USING (order_id)
GROUP BY 1, 2, 3, 4
