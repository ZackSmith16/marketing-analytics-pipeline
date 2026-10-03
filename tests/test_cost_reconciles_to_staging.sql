-- Total cost in the mart must equal total cost in staging, per platform.
-- If a join duplicates rows (fan-out) or drops them, these totals diverge.
WITH staging_cost AS (
  SELECT 'google_ads' AS platform, SUM(cost) AS cost FROM staging.stg_google_ads__campaigns
  UNION ALL
  SELECT 'meta_ads', SUM(cost) FROM staging.stg_meta_ads__campaigns
),
mart_cost AS (
  SELECT platform, SUM(cost) AS cost
  FROM marts.mart_campaign_daily
  GROUP BY 1
)
SELECT s.platform, s.cost AS staging_cost, m.cost AS mart_cost
FROM staging_cost AS s
FULL OUTER JOIN mart_cost AS m USING (platform)
WHERE m.cost IS NULL
   OR s.cost IS NULL
   OR ABS(s.cost - m.cost) > 0.01
