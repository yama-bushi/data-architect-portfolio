-- Uses ARIMA for time series forecasting + regression coefficients per provider
-- Integrates custom coefficients for each provider in final prediction

CREATE OR REPLACE TABLE your_project.analytics.provider_forecast AS
SELECT
  f.provider_id,
  f.calendar_week,
  forecasted_utilization,
  regression_adj,
  forecasted_utilization + regression_adj AS final_forecast
FROM (
  -- ARIMA model output
  SELECT
    provider_id,
    calendar_week,
    forecasted_utilization
  FROM
    ML.FORECAST(
      MODEL `your_project.analytics.arima_provider_model`,
      STRUCT(12 AS horizon)
    )
) a
LEFT JOIN (
  -- Regression coefficient table (precomputed in Python)
  SELECT
    provider_id,
    calendar_week,
    regression_adj
  FROM
    your_project.analytics.provider_regression_results
) f
USING (provider_id, calendar_week);
