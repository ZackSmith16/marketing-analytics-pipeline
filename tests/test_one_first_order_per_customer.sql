-- Each customer can only be "new" once. If customers were ranked inside a
-- date window instead of across their full history, returning customers
-- would be counted as new again.
SELECT store_id, customer_id, COUNTIF(is_new_customer) AS first_orders
FROM marts.mart_shopify_orders
GROUP BY 1, 2
HAVING COUNTIF(is_new_customer) <> 1
