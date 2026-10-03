-- The overview must add up to exactly the campaign-level table.
-- This is the guarantee the old setup lacked when the two dashboards were
-- built from two separate 350-line queries.
WITH campaign_totals AS (
  SELECT client_id, platform, SUM(cost) AS cost, SUM(conversions) AS conversions
  FROM marts.mart_campaign_daily
  GROUP BY 1, 2
),
client_totals AS (
  SELECT client_id, platform, SUM(cost) AS cost, SUM(conversions) AS conversions
  FROM marts.mart_client_daily
  GROUP BY 1, 2
)
-- Columns are named explicitly: Dataform saves each assertion as a view,
-- and a view can't have two columns called `cost`.
SELECT
  client_id,
  platform,
  c.cost AS campaign_cost,
  o.cost AS client_cost,
  c.conversions AS campaign_conversions,
  o.conversions AS client_conversions
FROM campaign_totals AS c
FULL OUTER JOIN client_totals AS o USING (client_id, platform)
WHERE ABS(COALESCE(c.cost, 0) - COALESCE(o.cost, 0)) > 0.01
   OR ABS(COALESCE(c.conversions, 0) - COALESCE(o.conversions, 0)) > 0.01
