-- Store sales by day.
-- Grain: one row per client, sales channel, and day.
--
-- Matches Shopify's own definitions:
--   net sales = gross sales - discounts - returns (tax and shipping excluded)
-- Returns land on the refund date, so a day's net sales can include refunds
-- of orders placed earlier.
WITH order_days AS (
  SELECT
    date, client_id, client_name, client_type, sales_channel,
    COUNT(*) AS orders,
    COUNTIF(is_new_customer) AS new_customer_orders,
    COUNTIF(NOT is_new_customer) AS returning_customer_orders,
    SUM(units) AS units,
    SUM(gross_sales) AS gross_sales,
    SUM(discounts) AS discounts,
    0 AS returns
  FROM marts.mart_shopify_orders
  GROUP BY 1, 2, 3, 4, 5
),

refund_days AS (
  -- Only refunds of real orders (the join drops refunds of test/spam orders)
  SELECT
    r.refund_date AS date, o.client_id, o.client_name, o.client_type, o.sales_channel,
    0 AS orders, 0 AS new_customer_orders, 0 AS returning_customer_orders,
    0 AS units, 0 AS gross_sales, 0 AS discounts,
    SUM(r.amount) AS returns
  FROM staging.stg_shopify__refunds AS r
  JOIN marts.mart_shopify_orders AS o
    USING (order_id)
  GROUP BY 1, 2, 3, 4, 5
),

daily AS (
  SELECT
    date, client_id, client_name, client_type, sales_channel,
    SUM(orders) AS orders,
    SUM(new_customer_orders) AS new_customer_orders,
    SUM(returning_customer_orders) AS returning_customer_orders,
    SUM(units) AS units,
    SUM(gross_sales) AS gross_sales,
    SUM(discounts) AS discounts,
    SUM(returns) AS returns,
    SUM(gross_sales) - SUM(discounts) - SUM(returns) AS net_sales
  FROM (
    SELECT * FROM order_days
    UNION ALL
    SELECT * FROM refund_days
  )
  GROUP BY 1, 2, 3, 4, 5
),

-- Last year's numbers shifted forward one year (same pattern as the ad mart)
prior_year AS (
  SELECT
    DATE_ADD(date, INTERVAL 1 YEAR) AS date,
    client_id, client_name, client_type, sales_channel,
    SUM(net_sales) AS net_sales_prev_year,
    SUM(orders) AS orders_prev_year
  FROM daily
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  COALESCE(d.date, p.date) AS date,
  COALESCE(d.client_id, p.client_id) AS client_id,
  COALESCE(d.client_name, p.client_name) AS client_name,
  COALESCE(d.client_type, p.client_type) AS client_type,
  COALESCE(d.sales_channel, p.sales_channel) AS sales_channel,
  COALESCE(d.orders, 0) AS orders,
  COALESCE(d.new_customer_orders, 0) AS new_customer_orders,
  COALESCE(d.returning_customer_orders, 0) AS returning_customer_orders,
  COALESCE(d.units, 0) AS units,
  COALESCE(d.gross_sales, 0) AS gross_sales,
  COALESCE(d.discounts, 0) AS discounts,
  COALESCE(d.returns, 0) AS returns,
  COALESCE(d.net_sales, 0) AS net_sales,
  COALESCE(p.net_sales_prev_year, 0) AS net_sales_prev_year,
  COALESCE(p.orders_prev_year, 0) AS orders_prev_year
FROM daily AS d
FULL OUTER JOIN prior_year AS p
  ON p.date = d.date
  AND p.client_id = d.client_id
  AND p.sales_channel = d.sales_channel
WHERE COALESCE(d.date, p.date) <= (SELECT MAX(date) FROM daily)
