-- Each order appears once after deduplication. Shopify re-sends changed
-- orders, so a missing dedup step would double-count sales.
SELECT order_id, COUNT(*) AS row_count
FROM staging.stg_shopify__orders
GROUP BY 1
HAVING COUNT(*) > 1
