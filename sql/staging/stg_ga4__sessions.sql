-- Grain: one row per property, day, and raw source / medium / campaign.
--
-- 1. Keep the latest sync of each row (the connector re-sends some days).
-- 2. Clean source and medium (lowercase, trimmed, '(not set)' for blanks).
-- 3. Assign a marketing channel from reference.ga4_channel_mapping.
--
-- The mapping table replaces a long hard-coded CASE statement. Each rule is
-- a pair of regex patterns with a priority; a row takes the first rule it
-- matches. Adding a new source is a one-line change to the CSV, and the
-- "Other" catch-all makes unmapped traffic visible instead of silently lost.
WITH latest AS (
  SELECT *
  FROM raw.ga4_sessions
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date, property_id, session_source, session_medium, session_campaign_name
    ORDER BY _synced_at DESC
  ) = 1
),

cleaned AS (
  SELECT
    date,
    property_id,
    session_source AS source_raw,
    session_medium AS medium_raw,
    session_campaign_name AS campaign_raw,
    LOWER(TRIM(COALESCE(session_source, '(not set)'))) AS source,
    LOWER(TRIM(COALESCE(session_medium, '(not set)'))) AS medium,
    COALESCE(TRIM(session_campaign_name), '(not set)') AS campaign,
    sessions,
    engaged_sessions,
    new_users,
    total_users,
    key_events,
    purchase_revenue
  FROM latest
)

SELECT
  c.*,
  m.channel
FROM cleaned AS c
JOIN reference.ga4_channel_mapping AS m
  ON REGEXP_CONTAINS(c.source, m.source_regex)
  AND REGEXP_CONTAINS(c.medium, m.medium_regex)
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY c.date, c.property_id, c.source_raw, c.medium_raw, c.campaign_raw
  ORDER BY m.priority
) = 1
