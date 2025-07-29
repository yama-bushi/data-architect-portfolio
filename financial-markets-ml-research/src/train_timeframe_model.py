import sys
import os
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.feature_selection import RFECV
from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix

FEATURES_DIR = "ohlcv_parquet"
MODEL_DIR = "models"
COVERAGE_CSV = "symbol_timeframe_coverage.csv"
VALIDATION_RESULTS_DIR = "validation_results"
MIN_BARS = 1000
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(VALIDATION_RESULTS_DIR, exist_ok=True)

def compute_validation_metrics(y_true, y_pred, y_prob, test_df):
    metrics = {}
    metrics['accuracy'] = accuracy_score(y_true, y_pred)
    try:
        metrics['auc'] = roc_auc_score(y_true, y_prob)
    except Exception:
        metrics['auc'] = np.nan

    # Compute Sharpe ratio using the returns from correct/incorrect predictions
    # You need a column for return. If not present, fallback to zero.
    if 'target_return_1' in test_df.columns:
        returns = np.where(y_pred == y_true, test_df['target_return_1'], -test_df['target_return_1'])
        if returns.std() > 0:
            metrics['sharpe'] = returns.mean() / returns.std() * (252**0.5)
        else:
            metrics['sharpe'] = np.nan
        metrics['profit'] = returns.sum()
    else:
        metrics['sharpe'] = np.nan
        metrics['profit'] = np.nan

    # Confusion matrix (optional, for more insights)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0,1]).ravel() if len(np.unique(y_true)) == 2 else (np.nan,)*4
    metrics.update({'tp':tp, 'fp':fp, 'tn':tn, 'fn':fn})

    return metrics
#Switching to XGB
def train_xgb_model(X_train, y_train, X_test, y_test):
    tscv = TimeSeriesSplit(n_splits=5)
    rfecv_estimator = xgb.XGBClassifier(
        tree_method="hist",
        device="cuda",
        n_estimators=25,
        random_state=42,
        eval_metric='logloss'
    )
    selector = RFECV(
        rfecv_estimator,
        step=1,
        cv=tscv,
        scoring='accuracy',
        min_features_to_select=5,
        n_jobs=-1
    )
    selector.fit(X_train, y_train)
    selected_features = X_train.columns[selector.support_].tolist()
    final_model = xgb.XGBClassifier(
        tree_method="hist",
        device="cuda",
        n_estimators=100,
        random_state=42,
        eval_metric='logloss'
    )
    final_model.fit(X_train[selected_features], y_train)
    y_pred = final_model.predict(X_test[selected_features])
    y_prob = final_model.predict_proba(X_test[selected_features])[:,1]
    return final_model, selected_features, y_pred, y_prob

def train_model_for_timeframe(timeframe):
    features_path = os.path.join(FEATURES_DIR, f"{timeframe}_features.parquet")
    if not os.path.exists(features_path):
        print(f"Features file not found: {features_path}")
        return

    print(f"[INFO] Loading features from: {features_path}")
    df = pd.read_parquet(features_path)

    # Load coverage info
    if os.path.exists(COVERAGE_CSV):
        coverage_df = pd.read_csv(COVERAGE_CSV)
        covered_symbols = set(coverage_df[
            (coverage_df['timeframe'] == timeframe) & (coverage_df['n_bars'] >= MIN_BARS)
        ]['symbol'])
        print(f"[INFO] {len(covered_symbols)} symbols with sufficient bars for {timeframe}")
    else:
        covered_symbols = set(df['symbol'].unique())
        print(f"[WARN] Coverage file not found, training for all symbols.")

    symbols = df['symbol'].unique()
    print(f"[INFO] Training on {len(symbols)} symbols: {symbols}")

    df = df.sort_values('timestamp')
    drop_cols = ['timestamp', 'symbol', 'timeframe', 'target_return_1', 'target_up']
    feature_cols = [col for col in df.columns if col not in drop_cols and not col.startswith('target_')]

    # --- Train main (multi-symbol) model ---
    X = df[feature_cols]
    y = df['target_up']
    split = int(len(df) * 0.7)
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]
    test_df = df.iloc[split:]  # For Sharpe etc.

    print(f"[INFO] Training main model for timeframe {timeframe}")
    final_model, selected_features, main_y_pred, main_y_prob = train_xgb_model(X_train, y_train, X_test, y_test)
    main_metrics = compute_validation_metrics(y_test, main_y_pred, main_y_prob, test_df)
    print(f"[INFO] [ALL SYMBOLS] Metrics: {main_metrics}")

    # Save main model and validation
    model_path = os.path.join(MODEL_DIR, f"{timeframe}_model_xgboost.joblib")
    joblib.dump({'model': final_model, 'features': selected_features}, model_path)
    print(f"[DONE] Main model saved to: {model_path}")

    validation_records = []
        # Save per-symbol main model performance as well!
    for symbol in symbols:
        sdf = test_df[test_df['symbol'] == symbol]
        if len(sdf) == 0:
            continue

        # Find indices in test set (zero-based for main_y_pred)
        test_indices = sdf.index - split

        # Only keep those within bounds
        valid_mask = (test_indices >= 0) & (test_indices < len(main_y_pred))
        aligned_pred = main_y_pred[test_indices[valid_mask]]
        aligned_prob = main_y_prob[test_indices[valid_mask]]
        aligned_y = sdf['target_up'].iloc[valid_mask]
        aligned_sdf = sdf.iloc[valid_mask]

        if len(aligned_pred) > 0:
            sym_metrics = compute_validation_metrics(aligned_y, aligned_pred, aligned_prob, aligned_sdf)
            validation_records.append({
                'symbol': symbol,
                'timeframe': timeframe,
                'model': 'main',
                **sym_metrics
            })


    # --- Per-symbol models ---
    for symbol in symbols:
        if symbol not in covered_symbols:
            print(f"[SKIP] {symbol} does not have sufficient bars for {timeframe} (skipped)")
            continue
        sdf = df[df['symbol'] == symbol].copy()
        if len(sdf) < MIN_BARS:
            print(f"[SKIP] {symbol} actual bars: {len(sdf)} < {MIN_BARS}")
            continue
        Xs = sdf[feature_cols]
        ys = sdf['target_up']
        split_s = int(len(sdf) * 0.7)
        X_train_s, X_test_s = Xs.iloc[:split_s], Xs.iloc[split_s:]
        y_train_s, y_test_s = ys.iloc[:split_s], ys.iloc[split_s:]
        test_sdf = sdf.iloc[split_s:]
        print(f"[INFO] Training model for symbol: {symbol} ({len(sdf)} bars)")
        sym_model, sym_features, sym_y_pred, sym_y_prob = train_xgb_model(X_train_s, y_train_s, X_test_s, y_test_s)
        sym_model_path = os.path.join(MODEL_DIR, f"{timeframe}_{symbol}_model_xgboost.joblib")
        joblib.dump({'model': sym_model, 'features': sym_features}, sym_model_path)
        print(f"[DONE] {symbol}: Model saved to {sym_model_path}")

        # Per-symbol model validation
        sym_val_metrics = compute_validation_metrics(y_test_s, sym_y_pred, sym_y_prob, test_sdf)
        validation_records.append({
            'symbol': symbol,
            'timeframe': timeframe,
            'model': 'per_symbol',
            **sym_val_metrics
        })

    # --- Save all validation results to CSV ---
    validation_df = pd.DataFrame(validation_records)
    validation_path = os.path.join(VALIDATION_RESULTS_DIR, f"model_validation_{timeframe}.csv")
    validation_df.to_csv(validation_path, index=False)
    print(f"[DONE] Validation metrics saved to {validation_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python train_timeframe_model_xgboost.py [timeframe]")
        sys.exit(1)
    tf = sys.argv[1]
    train_model_for_timeframe(tf)