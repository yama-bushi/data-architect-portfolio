import os
import sys
import pandas as pd
import numpy as np
import json
import uuid
from filelock import FileLock, Timeout

FEATURES_DIR = 'ohlcv_parquet'
TRADE_LOG_DIR = 'trade_ideas_logs'
STATE_DIR = 'trade_ideas_state'
os.makedirs(TRADE_LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)


def load_trade_params(json_path="trade_params.json"):
    with open(json_path, "r") as f:
        params = json.load(f)
    return params
# Load trade parameters from file
trade_params = load_trade_params()  # Uses default 'trade_params.json'

# --- Configurable Parameters ---
TRADE_PCT = trade_params['trade_pct']
SCALE_OUT_PCT = trade_params['scale_out_pct']
TAKE_PROFIT_PCT = trade_params['take_profit_pct']
STOP_LOSS_PCT = trade_params['stop_loss_pct']
MAX_OPEN_TRADES = trade_params['max_open_trades']
PROBA_THRESHOLD = trade_params['proba_threshold']
LEVERAGE = trade_params['leverage']

MAX_DRAWDOWN_PCT = -1 / LEVERAGE  # Liquidation threshold (leverage-adjusted)

TRADE_LOG_COLUMNS = [
    'trade_uuid', 'symbol', 'signal_type', 'signal_time', 'price', 'position_pct', 'extra',
    'entry_time', 'entry_price', 'exit_time', 'exit_price', 'executed'
]

OPEN_POSITIONS_GLOBAL_PATH = os.path.join(STATE_DIR, "open_positions_global.json")
OPEN_POSITIONS_LOCK_PATH = OPEN_POSITIONS_GLOBAL_PATH + ".lock"  # lock file

def load_json_state(filepath, lock_path):
    with FileLock(lock_path, timeout=60):
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return json.load(f)
        return {}

def save_json_state(obj, filepath, lock_path):
    with FileLock(lock_path, timeout=60):
        with open(filepath, "w") as f:
            json.dump(obj, f, indent=2)

def log_trade_ideas_live(timeframe):
    features_path = os.path.join(FEATURES_DIR, f"{timeframe}_features.parquet")
    log_path = os.path.join(TRADE_LOG_DIR, f"trade_ideas_{timeframe}.csv")
    state_filename = f"state_{timeframe}.json"
    positions_filename = f"positions_{timeframe}.json"

    if not os.path.exists(features_path):
        print(f"Features file not found: {features_path}")
        return
    df = pd.read_parquet(features_path)
    if 'pred_up' not in df.columns or 'proba_up' not in df.columns:
        print(f"Missing predictions in: {features_path}")
        return
    df = df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)
    df['datetime'] = pd.to_datetime(df['timestamp'])

    state = load_json_state(os.path.join(STATE_DIR, state_filename), os.path.join(STATE_DIR, state_filename + ".lock"))
    positions = load_json_state(os.path.join(STATE_DIR, positions_filename), os.path.join(STATE_DIR, positions_filename + ".lock"))
    new_trades = []

    # === Global file-locked open positions (across all timeframes) ===
    open_positions_global = load_json_state(OPEN_POSITIONS_GLOBAL_PATH, OPEN_POSITIONS_LOCK_PATH)
    # Structure: { symbol: { 'trade_uuid': uuid, 'timeframe': tf, ... } }

    for symbol in df['symbol'].unique():
        sdf = df[df['symbol'] == symbol].copy()
        if sdf.empty:
            continue
        latest_row = sdf.iloc[[-1]]
        last_processed = state.get(symbol)
        if last_processed and pd.to_datetime(last_processed) >= latest_row.iloc[0]['datetime']:
            continue

        open_trades = positions.get(symbol, [])
        open_trades = [t for t in open_trades if 'entry_time' in t and t['status'] == 'open']

        for i, row in latest_row.iterrows():
            # --- Check global lock: Only one open trade per symbol globally ---
            symbol_locked = symbol in open_positions_global and open_positions_global[symbol].get('status', 'open') == 'open'

            can_enter = (
                len(open_trades) < MAX_OPEN_TRADES
                and row['pred_up'] == 1
                and row['proba_up'] >= PROBA_THRESHOLD
                and not symbol_locked  # Only one open trade globally!
            )

            if can_enter:
                trade_id = str(uuid.uuid4())
                new_trade_state = {
                    'trade_uuid': trade_id,
                    'entry_time': str(row['datetime']),
                    'entry_price': row['close'],
                    'max_price': row['close'],
                    'scale_out_done': False,
                    'status': 'open'
                }
                open_trades.append(new_trade_state)
                new_trades.append({
                    'trade_uuid': trade_id,
                    'symbol': symbol,
                    'signal_type': 'entry',
                    'signal_time': str(row['datetime']),
                    'price': row['close'],
                    'position_pct': 1.0,
                    'extra': '',
                    'entry_time': str(row['datetime']),
                    'entry_price': row['close'],
                    'exit_time': '',
                    'exit_price': '',
                    'executed': 0
                })
                # Update global open_positions
                open_positions_global[symbol] = {
                    'trade_uuid': trade_id,
                    'timeframe': timeframe,
                    'status': 'open'
                }

            closed_trades = []
            for trade in open_trades:
                if row['close'] > trade['max_price']:
                    trade['max_price'] = row['close']

                # --- Scale-out (Take profit) ---
                if not trade['scale_out_done'] and (row['close'] / trade['entry_price'] - 1) >= TAKE_PROFIT_PCT:
                    new_trades.append({
                        'trade_uuid': trade['trade_uuid'],
                        'symbol': symbol,
                        'signal_type': 'exit',
                        'signal_time': str(row['datetime']),
                        'price': row['close'],
                        'position_pct': SCALE_OUT_PCT,
                        'extra': 'TakeProfit70%',
                        'entry_time': trade['entry_time'],
                        'entry_price': trade['entry_price'],
                        'exit_time': str(row['datetime']),
                        'exit_price': row['close'],
                        'executed': 0
                    })
                    trade['scale_out_done'] = True

                # --- Stop loss ---
                drawdown = row['close'] / trade['max_price'] - 1
                if drawdown < STOP_LOSS_PCT:
                    new_trades.append({
                        'trade_uuid': trade['trade_uuid'],
                        'symbol': symbol,
                        'signal_type': 'exit',
                        'signal_time': str(row['datetime']),
                        'price': row['close'],
                        'position_pct': 1.0,
                        'extra': 'TrailingStop',
                        'entry_time': trade['entry_time'],
                        'entry_price': trade['entry_price'],
                        'exit_time': str(row['datetime']),
                        'exit_price': row['close'],
                        'executed': 0
                    })
                    trade['status'] = 'closed'
                    closed_trades.append(trade)
                    # Mark as closed in global file
                    if symbol in open_positions_global:
                        open_positions_global[symbol]['status'] = 'closed'


                # --- Signal flip exit WITH CONFIDENCE THRESHOLD ---
                elif row['pred_up'] == 0 and row['proba_up'] < (1 - PROBA_THRESHOLD):
                    new_trades.append({
                        'trade_uuid': trade['trade_uuid'],
                        'symbol': symbol,
                        'signal_type': 'exit',
                        'signal_time': str(row['datetime']),
                        'price': row['close'],
                        'position_pct': 1.0,
                        'extra': 'SignalFlip',
                        'entry_time': trade['entry_time'],
                        'entry_price': trade['entry_price'],
                        'exit_time': str(row['datetime']),
                        'exit_price': row['close'],
                        'executed': 0
                    })
                    trade['status'] = 'closed'
                    closed_trades.append(trade)
                    if symbol in open_positions_global:
                        open_positions_global[symbol]['status'] = 'closed'

                # --- Max leveraged drawdown liquidation ---
                price_change = (row['close'] - float(trade['entry_price'])) / float(trade['entry_price'])
                leveraged_drawdown = price_change * LEVERAGE
                if leveraged_drawdown <= MAX_DRAWDOWN_PCT and trade['status'] == 'open':
                    new_trades.append({
                        'trade_uuid': trade['trade_uuid'],
                        'symbol': symbol,
                        'signal_type': 'exit',
                        'signal_time': str(row['datetime']),
                        'price': row['close'],
                        'position_pct': 1.0,
                        'extra': 'LeverageLiquidation',
                        'entry_time': trade['entry_time'],
                        'entry_price': trade['entry_price'],
                        'exit_time': str(row['datetime']),
                        'exit_price': row['close'],
                        'executed': 0
                    })
                    trade['status'] = 'closed'
                    closed_trades.append(trade)
                    # Mark as closed in global file
                    if symbol in open_positions_global:
                        open_positions_global[symbol]['status'] = 'closed'

            open_trades = [t for t in open_trades if t['status'] == 'open']

        positions[symbol] = open_trades
        state[symbol] = str(latest_row.iloc[0]['datetime'])

    # --- Save logs and state ---
    if new_trades:
        trades_df = pd.DataFrame(new_trades)
        for col in TRADE_LOG_COLUMNS:
            if col not in trades_df.columns:
                trades_df[col] = ''
        trades_df = trades_df[TRADE_LOG_COLUMNS]

        if os.path.exists(log_path):
            trades_df.to_csv(log_path, mode='a', index=False, header=False)
        else:
            trades_df.to_csv(log_path, index=False)
        print(f"Logged {len(new_trades)} new trade ideas to {log_path}")
    else:
        print("No new trades to log.")

    # Save local and global state with locks
    save_json_state(state, os.path.join(STATE_DIR, state_filename), os.path.join(STATE_DIR, state_filename + ".lock"))
    save_json_state(positions, os.path.join(STATE_DIR, positions_filename), os.path.join(STATE_DIR, positions_filename + ".lock"))
    save_json_state(open_positions_global, OPEN_POSITIONS_GLOBAL_PATH, OPEN_POSITIONS_LOCK_PATH)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python trade_ideas_logger.py [timeframe]")
        sys.exit(1)
    tf = sys.argv[1]
    log_trade_ideas_live(tf)
