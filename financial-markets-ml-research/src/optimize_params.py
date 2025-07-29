import json
import numpy as np
from skopt import gp_minimize
from skopt.space import Real
from skopt.utils import use_named_args
import os
from backtest_by_timeframe import run_backtest_for_timeframe

# ---- Import your backtest function here ----
# If your backtest function is in another script, e.g. backtest_module.py:
# from backtest_module import run_backtest_for_timeframe

# For demonstration, I'll define a dummy run_backtest_for_timeframe:
def run_backtest_for_timeframe(timeframe):
    # This should run your actual backtest and write dashboard_data/summary_{timeframe}.json
    # Here is a dummy version for demonstration
    summary = {
        "final_value": 105000,
        "total_return": 0.05,
        "max_drawdown": -0.12,
        "sharpe": np.random.uniform(0.6, 1.4),  # Random Sharpe for demo
        "win_rate": 0.53,
        "avg_hold_hours": 7.4,
        "trade_count": 33
    }
    os.makedirs('dashboard_data', exist_ok=True)
    with open(f'dashboard_data/summary_{timeframe}.json', 'w') as f:
        json.dump(summary, f)

# ---------------

# Timeframes to test
TIMEFRAMES = ['30m', '1h', '4h', '12h', '1d']  # Update with your timeframes

# Space of hyperparameters
space = [
    Real(0.02, 0.10, name='take_profit_pct'),
    Real(-0.15, -0.03, name='stop_loss_pct'),
    Real(0.50, 0.75, name='proba_threshold')
]

def load_trade_params(path="trade_params.json"):
    with open(path, "r") as f:
        return json.load(f)

def save_trade_params(params, path="trade_params.json"):
    with open(path, "w") as f:
        json.dump(params, f, indent=2)

@use_named_args(space)
def robust_objective(take_profit_pct, stop_loss_pct, proba_threshold):
    # Load existing params and update the tuned ones
    params = load_trade_params()
    params['take_profit_pct'] = float(take_profit_pct)
    params['stop_loss_pct'] = float(stop_loss_pct)
    params['proba_threshold'] = float(proba_threshold)
    save_trade_params(params)
    sharpes = []
    for tf in TIMEFRAMES:
        run_backtest_for_timeframe(tf)
        result_path = f'dashboard_data/summary_{tf}.json'
        if os.path.exists(result_path):
            with open(result_path, 'r') as f:
                res = json.load(f)
            sharpe = res.get('sharpe', np.nan)
            if not np.isnan(sharpe):
                sharpes.append(sharpe)
    if not sharpes:
        return 1e6  # Penalize if no sharpes collected (bad params)
    # Robust metric: mean Sharpe minus std Sharpe (can customize)
    robust_score = -(np.mean(sharpes) - np.std(sharpes))
    print(f"Params: TP={take_profit_pct:.3f}, SL={stop_loss_pct:.3f}, PROBA={proba_threshold:.3f} -> Robust Score={-robust_score:.4f}")
    return robust_score  # Minimize negative of robust metric

if __name__ == "__main__":
    print("Starting Bayesian Optimization for trade parameters...")
    result = gp_minimize(
        robust_objective,
        space,
        n_calls=100,         # You can increase for more thorough search
        n_initial_points=10, # More random points to start
        random_state=42,
        verbose=True
    )

    # Save the best found params to file
    best_tp, best_sl, best_proba = result.x
    best_params = load_trade_params()
    best_params['take_profit_pct'] = float(best_tp)
    best_params['stop_loss_pct'] = float(best_sl)
    best_params['proba_threshold'] = float(best_proba)
    save_trade_params(best_params)

    print("\nBest parameters found:")
    print(json.dumps({
        "take_profit_pct": best_tp,
        "stop_loss_pct": best_sl,
        "proba_threshold": best_proba,
        "robust_score": -result.fun
    }, indent=2))
    print("Saved to trade_params.json!")