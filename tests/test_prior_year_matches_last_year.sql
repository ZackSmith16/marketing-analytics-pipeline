-- The prior-year columns must equal what was actually recorded one year
-- earlier. Checked per client for each full month where both years exist.
WITH shifted AS (
  SELECT client_id, DATE_TRUNC(date, MONTH) AS month,
         SUM(conversion_value_prev_year) AS py_value, SUM(cost_prev_year) AS py_cost
  FROM marts.mart_campaign_daily
  GROUP BY 1, 2
),
actual AS (
  SELECT client_id, DATE_TRUNC(DATE_ADD(date, INTERVAL 1 YEAR), MONTH) AS month,
         SUM(conversion_value) AS value, SUM(cost) AS cost
  FROM marts.mart_campaign_daily
  GROUP BY 1, 2
)
SELECT s.client_id, s.month, s.py_value, a.value, s.py_cost, a.cost
FROM shifted AS s
JOIN actual AS a USING (client_id, month)
WHERE s.month > (SELECT DATE_TRUNC(MIN(date), MONTH) FROM marts.mart_campaign_daily WHERE cost > 0)
  AND s.month < (SELECT DATE_TRUNC(MAX(date), MONTH) FROM marts.mart_campaign_daily)
  AND s.month <> DATE '2025-02-01'  -- leap day folds into Feb 28
  AND (ABS(s.py_value - a.value) > 0.01 OR ABS(s.py_cost - a.cost) > 0.01)
