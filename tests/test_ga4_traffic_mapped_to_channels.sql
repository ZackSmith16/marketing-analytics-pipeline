-- Almost all traffic should map to a named channel. A growing "Other"
-- bucket means a new source/medium needs a row in the mapping table.
WITH totals AS (
  SELECT
    SUM(sessions) AS sessions,
    SUM(CASE WHEN channel = 'Other' THEN sessions ELSE 0 END) AS other_sessions
  FROM marts.mart_ga4_channel_daily
)
SELECT sessions, other_sessions, other_sessions / sessions AS other_share
FROM totals
WHERE other_sessions / sessions > 0.02
