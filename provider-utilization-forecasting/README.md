# Provider Utilization Forecasting

This project demonstrates forecasting provider utilization using BigQuery ML and Python regression.  
It is designed to predict future scheduling or resource needs for healthcare/education provider networks.

## What’s Included

- **bigquery_ml_before.sql**: Prepares and aggregates historic provider and calendar data for modeling.
- **provider_regression_learning.py**: Trains regression models (per provider) to estimate coefficients for custom provider-level effects.
- **bigquery_ml_after.sql**: Runs final forecasting using ARIMA and regression coefficients.

## How To Use

1. Run the `bigquery_ml_before.sql` script in BigQuery to create your feature/modeling tables.
2. Use `provider_regression_learning.py` to fit regression coefficients for each provider.
3. Run `bigquery_ml_after.sql` to generate forecasts with ARIMA models and provider adjustments.
4. Adjust queries and paths as needed for your BigQuery project and dataset names.

**Author:** Justin Lowe
