-- Each campaign should appear once per day.
-- Catches connector re-syncs that slip through deduplication.
SELECT platform, campaign_id, date, COUNT(*) AS row_count
FROM marts.mart_campaign_daily
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
