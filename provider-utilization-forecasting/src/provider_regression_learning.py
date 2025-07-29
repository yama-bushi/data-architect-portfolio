"""
Fits regression models for each provider to estimate provider-specific coefficients.
This enables per-provider modeling in subsequent forecasting.
"""

import pandas as pd
from sklearn.linear_model import LinearRegression

def fit_provider_regressions(df, provider_col='provider_id', target_col='total_utilization', feature_cols=None):
    results = {}
    if feature_cols is None:
        feature_cols = [c for c in df.columns if c not in [provider_col, target_col]]
    for provider, group in df.groupby(provider_col):
        X = group[feature_cols]
        y = group[target_col]
        model = LinearRegression().fit(X, y)
        results[provider] = {
            "coef": model.coef_,
            "intercept": model.intercept_,
            "features": feature_cols
        }
    return results

if __name__ == "__main__":
    # Example usage: Load the features CSV from BigQuery export
    # df = pd.read_csv('provider_util_features.csv')
    # results = fit_provider_regressions(df)
    # Save or print results as needed
    pass
