-- Website traffic by channel, with last year alongside.
-- Grain: one row per client, channel, and day.
WITH daily AS (
  SELECT
    date, client_id, client_name, client_type, channel,
    SUM(sessions) AS sessions,
    SUM(engaged_sessions) AS engaged_sessions,
    SUM(new_users) AS new_users,
    SUM(total_users) AS total_users,
    SUM(key_events) AS key_events,
    SUM(revenue) AS revenue
  FROM marts.mart_ga4_source_daily
  GROUP BY 1, 2, 3, 4, 5
),

prior_year AS (
  SELECT
    DATE_ADD(date, INTERVAL 1 YEAR) AS date,
    client_id, client_name, client_type, channel,
    SUM(sessions) AS sessions_prev_year,
    SUM(key_events) AS key_events_prev_year,
    SUM(revenue) AS revenue_prev_year
  FROM daily
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  COALESCE(d.date, p.date) AS date,
  COALESCE(d.client_id, p.client_id) AS client_id,
  COALESCE(d.client_name, p.client_name) AS client_name,
  COALESCE(d.client_type, p.client_type) AS client_type,
  COALESCE(d.channel, p.channel) AS channel,
  COALESCE(d.sessions, 0) AS sessions,
  COALESCE(d.engaged_sessions, 0) AS engaged_sessions,
  COALESCE(d.new_users, 0) AS new_users,
  COALESCE(d.total_users, 0) AS total_users,
  COALESCE(d.key_events, 0) AS key_events,
  COALESCE(d.revenue, 0) AS revenue,
  COALESCE(p.sessions_prev_year, 0) AS sessions_prev_year,
  COALESCE(p.key_events_prev_year, 0) AS key_events_prev_year,
  COALESCE(p.revenue_prev_year, 0) AS revenue_prev_year
FROM daily AS d
FULL OUTER JOIN prior_year AS p
  ON p.date = d.date
  AND p.client_id = d.client_id
  AND p.channel = d.channel
WHERE COALESCE(d.date, p.date) <= (SELECT MAX(date) FROM daily)
