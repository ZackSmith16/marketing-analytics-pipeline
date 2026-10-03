-- Grain: one row per order line item.
-- Line items are nested inside each order and prices arrive as strings.
WITH latest AS (
  SELECT *
  FROM raw.shopify_orders
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
)

SELECT
  o.order_id,
  li.line_item_id,
  li.product_title,
  li.quantity,
  SAFE_CAST(li.price AS FLOAT64) AS price,
  li.quantity * SAFE_CAST(li.price AS FLOAT64) AS gross_sales
FROM latest AS o
CROSS JOIN UNNEST(o.line_items) AS li
