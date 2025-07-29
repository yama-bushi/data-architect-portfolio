import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt
import json

def load_trade_params(json_path="trade_params.json"):
    with open(json_path, "r") as f:
        params = json.load(f)
    return params
# Load trade parameters from file
trade_params = load_trade_params()  # Uses default 'trade_params.json'

# --- Configurable Parameters ---


MAX_OPEN_TRADES = trade_params['max_open_trades']

FEATURES_DIR = 'ohlcv_parquet'
STARTING_CASH = 100_000
LEVERAGE = trade_params['leverage']               # <--- Set your leverage here
TRADE_PCT = trade_params['trade_pct']
SCALE_OUT_PCT = trade_params['scale_out_pct']
TAKE_PROFIT_PCT = trade_params['take_profit_pct']
STOP_LOSS_PCT = trade_params['stop_loss_pct']
TRADE_COST = 0.001         # 0.1% per side
VOL_SPIKE_PCTL = 0.9
PROBA_THRESHOLD = trade_params['proba_threshold']

def run_backtest_for_timeframe(timeframe):
    features_path = os.path.join(FEATURES_DIR, f"{timeframe}_features.parquet")
    if not os.path.exists(features_path):
        print(f"No aggregated features file: {features_path}")
        return
    df = pd.read_parquet(features_path)
    if df.empty:
        print(f"Aggregated file is empty for {timeframe}")
        return
    df = df.drop_duplicates(subset=['symbol', 'timestamp', 'timeframe']).sort_values(['symbol','timestamp']).reset_index(drop=True)
    needed_cols = ['symbol', 'timestamp', 'timeframe', 'close', 'target_return_1', 'pred_up', 'proba_up', 'drawdown_20', 'rolling_std_14', 'low_vol_liquidity']
    df = df.dropna(subset=needed_cols)
    df['datetime'] = pd.to_datetime(df['timestamp'])
    df = df.set_index(['datetime', 'symbol'])

    # Volatility spike thresholds (per symbol)
    symbol_thresholds = df.groupby('symbol')['rolling_std_14'].quantile(VOL_SPIKE_PCTL).to_dict()
    df['vol_spike'] = df.apply(lambda row: row['rolling_std_14'] > symbol_thresholds[row.name[1]], axis=1)

    # ---- Per-Trade Simulation ----
    trade_log = []
    portfolio_curve = []
    portfolio_value = STARTING_CASH

    symbols = df.index.get_level_values('symbol').unique()
    for symbol in symbols:
        sdf = df.xs(symbol, level='symbol')
        position = 0.0
        entry_idx = None
        entry_price = None
        max_price = None
        scale_out_done = False
        entry_time = None
        entry_fee = 0
        liquidation_triggered = False

        for idx, row in sdf.iterrows():
            # ENTRY LOGIC
            if position == 0.0 and row['pred_up'] == 1 and row['proba_up'] >= PROBA_THRESHOLD:
                position = 1.0
                entry_idx = idx
                entry_price = row['close']
                max_price = entry_price
                scale_out_done = False
                entry_time = idx
                notional = TRADE_PCT * LEVERAGE * portfolio_value
                entry_fee = notional * TRADE_COST
                portfolio_value -= entry_fee  # Deduct entry fee up front
                liquidation_triggered = False
                continue

            if position > 0.0:
                # Update trailing max price
                if row['close'] > max_price:
                    max_price = row['close']

                # Helper: liquidation threshold (if price drops -1/LEVERAGE from entry)
                liquidation_threshold = entry_price * (1 - 1.0 / LEVERAGE)
                # Check for liquidation (only for long, spot-like trades; shorts are more complex)
                if row['close'] <= liquidation_threshold and not liquidation_triggered:
                    exit_time = idx
                    exit_price = row['close']
                    position_size = position
                    trade_value = TRADE_PCT * position_size * LEVERAGE * portfolio_value

                    # On liquidation, you lose your initial margin (the full value for this trade, minus fee already paid)
                    net_pnl = -TRADE_PCT * position_size * portfolio_value  # Lose full position, no need to deduct exit fee again
                    trade_log.append({
                        'symbol': symbol,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'exit_time': exit_time,
                        'exit_price': exit_price,
                        'exit_type': 'Liquidation',
                        'pnl_gross': net_pnl,
                        'fee': entry_fee,  # Only entry fee paid, not exit fee
                        'pnl_net': net_pnl,
                        'position_pct': position_size
                    })
                    portfolio_value += net_pnl  # Reduce portfolio to simulate liquidation
                    position = 0.0
                    scale_out_done = False
                    entry_time = None
                    liquidation_triggered = True
                    continue  # skip further exit logic for this bar

                # SCALE-OUT LOGIC
                if not scale_out_done and (row['close'] / entry_price - 1) >= TAKE_PROFIT_PCT:
                    exit_time = idx
                    exit_price = row['close']
                    position_size = SCALE_OUT_PCT

                    trade_value = TRADE_PCT * position_size * LEVERAGE * portfolio_value
                    gross_pnl = (exit_price - entry_price) / entry_price * trade_value
                    exit_fee = trade_value * TRADE_COST
                    net_pnl = gross_pnl - exit_fee

                    trade_log.append({
                        'symbol': symbol,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'exit_time': exit_time,
                        'exit_price': exit_price,
                        'exit_type': 'TakeProfit70%',
                        'pnl_gross': gross_pnl,
                        'fee': exit_fee,
                        'pnl_net': net_pnl,
                        'position_pct': position_size
                    })
                    portfolio_value += net_pnl
                    position = 1.0 - SCALE_OUT_PCT
                    scale_out_done = True

                # TRAILING STOP / DRAWNDOWN EXIT
                drawdown = row['close'] / max_price - 1
                if drawdown < STOP_LOSS_PCT:
                    exit_time = idx
                    exit_price = row['close']
                    position_size = position

                    trade_value = TRADE_PCT * position_size * LEVERAGE * portfolio_value
                    gross_pnl = (exit_price - entry_price) / entry_price * trade_value
                    exit_fee = trade_value * TRADE_COST
                    net_pnl = gross_pnl - exit_fee

                    trade_log.append({
                        'symbol': symbol,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'exit_time': exit_time,
                        'exit_price': exit_price,
                        'exit_type': 'TrailingStop',
                        'pnl_gross': gross_pnl,
                        'fee': exit_fee,
                        'pnl_net': net_pnl,
                        'position_pct': position_size
                    })
                    portfolio_value += net_pnl
                    position = 0.0
                    scale_out_done = False
                    entry_time = None

                # VOLATILITY EXIT
                elif row['vol_spike']:
                    exit_time = idx
                    exit_price = row['close']
                    position_size = position

                    trade_value = TRADE_PCT * position_size * LEVERAGE * portfolio_value
                    gross_pnl = (exit_price - entry_price) / entry_price * trade_value
                    exit_fee = trade_value * TRADE_COST
                    net_pnl = gross_pnl - exit_fee

                    trade_log.append({
                        'symbol': symbol,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'exit_time': exit_time,
                        'exit_price': exit_price,
                        'exit_type': 'VolSpike',
                        'pnl_gross': gross_pnl,
                        'fee': exit_fee,
                        'pnl_net': net_pnl,
                        'position_pct': position_size
                    })
                    portfolio_value += net_pnl
                    position = 0.0
                    scale_out_done = False
                    entry_time = None

                # SIGNAL FLIP EXIT (when signal turns off)
                elif row['pred_up'] == 0 and row['proba_up'] < (1 - PROBA_THRESHOLD):
                    exit_time = idx
                    exit_price = row['close']
                    position_size = position

                    trade_value = TRADE_PCT * position_size * LEVERAGE * portfolio_value
                    gross_pnl = (exit_price - entry_price) / entry_price * trade_value
                    exit_fee = trade_value * TRADE_COST
                    net_pnl = gross_pnl - exit_fee

                    trade_log.append({
                        'symbol': symbol,
                        'entry_time': entry_time,
                        'entry_price': entry_price,
                        'exit_time': exit_time,
                        'exit_price': exit_price,
                        'exit_type': 'SignalFlip',
                        'pnl_gross': gross_pnl,
                        'fee': exit_fee,
                        'pnl_net': net_pnl,
                        'position_pct': position_size
                    })
                    portfolio_value += net_pnl
                    position = 0.0
                    scale_out_done = False
                    entry_time = None

    # ---- Portfolio Value Simulation (using net pnl for each trade) ----
    trade_log_df = pd.DataFrame(trade_log)
    trade_log_df = trade_log_df.sort_values('exit_time')
    portfolio_curve = [STARTING_CASH]
    for _, trade in trade_log_df.iterrows():
        new_value = portfolio_curve[-1] + trade['pnl_net']
        portfolio_curve.append(new_value)
    portfolio_curve = pd.Series(portfolio_curve[1:], index=trade_log_df['exit_time'])
    
    # ---- Analytics ----
    returns = portfolio_curve.pct_change().fillna(0)
    sharpe = returns.mean() / returns.std() * (252**0.5) if returns.std() > 0 else np.nan
    max_dd = (portfolio_curve / portfolio_curve.cummax() - 1).min()
    win_rate = (trade_log_df['pnl_net'] > 0).sum() / len(trade_log_df) if len(trade_log_df) > 0 else 0
    avg_hold = (trade_log_df['exit_time'] - trade_log_df['entry_time']).dt.total_seconds().mean() / 3600 if len(trade_log_df) > 0 else 0

    print(f"[{timeframe}] Final Value: ${portfolio_curve.iloc[-1]:,.2f}")
    print(f"Total Return: {(portfolio_curve.iloc[-1]/STARTING_CASH-1):.2%}")
    print(f"Max Drawdown: {max_dd:.2%}")
    print(f"Sharpe Ratio: {sharpe:.2f}")
    print(f"Win Rate: {win_rate:.2%}")
    print(f"Avg Holding Time: {avg_hold:.2f} hours/trade")
    print(f"Trade Count: {len(trade_log_df)}")

    plt.figure(figsize=(12,6))
    portfolio_curve.plot(label='Portfolio Value')
    plt.ylabel('Portfolio Value')
    plt.title(f'Portfolio Curve ({timeframe})')
    plt.legend()
    plt.grid()
    #plt.show()
    plt.savefig(f"Portfolio_Curve_{timeframe}_chart.png")

    # Optionally save trade log for live signal monitoring:
    trade_log_df.to_csv(f"trade_log_{timeframe}.csv", index=False)
    print(f"Trade log saved: trade_log_{timeframe}.csv")

    # Store for future dashboarding
    trade_log_df.to_csv(f"dashboard_data/trade_log_{timeframe}.csv", index=False)
    portfolio_curve.to_csv(f"dashboard_data/portfolio_curve_{timeframe}.csv", header=['portfolio_value'])
    summary = {
        'final_value': float(portfolio_curve.iloc[-1]),
        'total_return': float((portfolio_curve.iloc[-1]/STARTING_CASH-1)),
        'max_drawdown': float(max_dd),
        'sharpe': float(sharpe),
        'win_rate': float(win_rate),
        'avg_hold_hours': float(avg_hold),
        'trade_count': int(len(trade_log_df))
    }
    import json
    with open(f"dashboard_data/summary_{timeframe}.json", "w") as f:
        json.dump(summary, f)
    print(f"Dashboard files saved in dashboard_data/ for {timeframe}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python backtest_by_timeframe_tradelog.py [timeframe]")
        sys.exit(1)
    tf = sys.argv[1]
    run_backtest_for_timeframe(tf)
