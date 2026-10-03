-- Grain: one row per account, campaign, and day.
--
-- The connector sometimes re-syncs a day. The later sync has more complete
-- conversion data (attribution arrives late), so keep only the most recent
-- load of each campaign-day. SELECT DISTINCT would not work here because the
-- duplicate rows are not identical.
SELECT
  date,
  'google_ads' AS platform,
  account_id,
  campaign_id,
  TRIM(campaign_name) AS campaign_name,
  impressions,
  clicks,
  cost,
  conversions,
  conversions_value AS conversion_value,
  _synced_at
FROM raw.google_ads_campaigns
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id
  ORDER BY _synced_at DESC
) = 1
