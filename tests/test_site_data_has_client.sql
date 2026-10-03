-- Every store and GA4 property must map to a client.
SELECT 'shopify' AS source, COUNT(*) AS unmapped_rows
FROM marts.mart_shopify_daily WHERE client_id IS NULL
HAVING COUNT(*) > 0
UNION ALL
SELECT 'ga4', COUNT(*)
FROM marts.mart_ga4_channel_daily WHERE client_id IS NULL
HAVING COUNT(*) > 0
