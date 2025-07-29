-- Prepares input data for regression and ARIMA modeling
-- Aggregates utilization per provider/client/calendar, ready for modeling

CREATE OR REPLACE TABLE your_project.analytics.provider_util_features AS
SELECT
  provider_id,
  client_id,
  calendar_week,
  SUM(utilization) AS total_utilization,
  AVG(schedule_count) AS avg_schedule,
  COUNT(DISTINCT session_id) AS unique_sessions,
  -- Add any additional features needed for your use case
FROM
  your_project.raw.provider_sessions
GROUP BY
  provider_id, client_id, calendar_week;
