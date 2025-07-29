import os
import sys
import time
import pandas as pd
from datetime import datetime
#from utils.notify import send_discord_message
#from utils.notify import send_discord_image

import matplotlib.pyplot as plt

WEBHOOK_URL = "webhook_to_publish_to"

TRADELOG_DIR = "trade_ideas_logs"
TRADELOG_TEMPLATE = "trade_ideas_{tf}.csv"
EQUITY_LOG_TEMPLATE = "paper_equity_log_{tf}.csv"
STARTING_CASH = 100_000

# -- LEVERAGE SUPPORT --
LEVERAGE = 2     # <--- Set this to 1, 2, 3, etc.
TRADE_PCT = 0.05  # Of equity; effective notional = TRADE_PCT * LEVERAGE
SCALE_OUT_PCT = 0.7
FEE_RATE = 0.001  # 0.1% per trade (per side)

def reconstruct_open_positions(tradelog_path):
    trades = pd.read_csv(tradelog_path)
    open_positions = {}

    # Filter to only executed trades for accurate state
    executed = trades[trades['executed'] == 1]

    # Group by trade_uuid
    for uuid, group in executed.groupby('trade_uuid'):
        has_entry = ((group['signal_type'] == 'entry').any())
        exit_rows = group[group['signal_type'] == 'exit']
        total_exited = exit_rows['position_pct'].sum() if not exit_rows.empty else 0
        if has_entry and total_exited < 1.0:
            entry_row = group[group['signal_type'] == 'entry'].iloc[0]
            open_positions[uuid] = {
                'symbol': entry_row['symbol'],
                'entry_time': entry_row['entry_time'],
                'entry_price': entry_row['entry_price'],
                'max_price': entry_row['entry_price'],
                'position': 1.0 - total_exited,
                'scale_out_done': total_exited > 0
            }

    return open_positions

def load_portfolio_value(equity_log_path):
    if os.path.exists(equity_log_path):
        eq_df = pd.read_csv(equity_log_path)
        # Use the last portfolio_value in the file
        last_value = float(eq_df["portfolio_value"].iloc[-1])
        return last_value
    else:
        return STARTING_CASH

def append_equity_log(equity_log_path, log_row):
    df = pd.DataFrame([log_row])
    header = not os.path.exists(equity_log_path)
    df.to_csv(equity_log_path, mode='a', index=False, header=header)

def main():
    if len(sys.argv) != 2:
        print("Usage: python live_paper_trading_bot.py [timeframe]")
        sys.exit(1)
    timeframe = sys.argv[1]
    tradelog_path = os.path.join(TRADELOG_DIR, TRADELOG_TEMPLATE.format(tf=timeframe))
    equity_log_path = EQUITY_LOG_TEMPLATE.format(tf=timeframe)
    if not os.path.exists(tradelog_path):
        print(f"Trade log not found: {tradelog_path}")
        return

    trades = pd.read_csv(tradelog_path)
    if 'trade_uuid' not in trades.columns or 'executed' not in trades.columns:
        print("Trade log missing 'trade_uuid' or 'executed' columns.")
        return

    trades['signal_time'] = pd.to_datetime(trades['signal_time'])
    trades = trades.sort_values('signal_time')

    # Load equity log to get the most recent portfolio value
    portfolio_value = load_portfolio_value(equity_log_path)
    tradelog_path = os.path.join(TRADELOG_DIR, TRADELOG_TEMPLATE.format(tf=timeframe))
    open_positions = reconstruct_open_positions(tradelog_path)

    for idx, row in trades[trades['executed'] == 0].iterrows():
        uuid = row['trade_uuid']
        symbol = row['symbol']
        action = row['signal_type']
        price = float(row['price'])
        position_pct = float(row['position_pct'])
        trade_time = row['signal_time']
        extra = row['extra']

        if action == "entry":
            if uuid not in open_positions:
                open_positions[uuid] = {
                    'symbol': symbol,
                    'entry_time': trade_time,
                    'entry_price': price,
                    'max_price': price,
                    'position': 1.0,
                    'scale_out_done': False
                }
                notional = TRADE_PCT * LEVERAGE * portfolio_value
                entry_fee = notional * FEE_RATE
                portfolio_value -= entry_fee  # Deduct opening fee immediately
                msg = (f"🟢{timeframe} **PAPER BUY** {symbol} (uuid `{uuid[:8]}...`) at ${price:.4f} ({trade_time})\n"
                       f"Alloc: {TRADE_PCT*100:.1f}% x {LEVERAGE}x | Entry Fee: ${entry_fee:.2f} | "
                       f"Portfolio: ${portfolio_value:,.2f}")
                #send_discord_message(WEBHOOK_URL,msg)
                trades.loc[idx, 'executed'] = 1
                # Log portfolio value for entry
                log_row = {
                    'timestamp': trade_time,
                    'portfolio_value': portfolio_value,
                    'action': 'entry',
                    'symbol': symbol,
                    'trade_uuid': uuid,
                    'price': price,
                    'pnl': -entry_fee  # show as negative PnL at entry
                }
                append_equity_log(equity_log_path, log_row)

        elif action == "exit":
            if uuid in open_positions:
                entry = open_positions[uuid]
                entry_price = float(entry['entry_price'])
                # Notional = TRADE_PCT * LEVERAGE * portfolio_value
                # Use original notional at entry, *not* portfolio_value now, to match backtest
                notional = TRADE_PCT * LEVERAGE * portfolio_value
                trade_value = notional * position_pct  # Handles partial exits (scale out)
                pnl_gross = (price - entry_price) / entry_price * trade_value
                exit_fee = trade_value * FEE_RATE
                # Total fee = (entry fee already paid) + exit fee
                pnl = pnl_gross - exit_fee
                portfolio_value += pnl
                msg = (
                    f"🔴{timeframe} **PAPER EXIT** {symbol} (uuid `{uuid[:8]}...`) at ${price:.4f} ({trade_time}) | "
                    f"Gross PnL: ${pnl_gross:.2f}, Exit Fee: ${exit_fee:.2f}, Net PnL: ${pnl:.2f} | "
                    f"Portfolio: ${portfolio_value:,.2f} Reason: {extra}"
                )
                #send_discord_message(WEBHOOK_URL,msg)
                trades.loc[idx, 'executed'] = 1
                # Log portfolio value for exit
                log_row = {
                    'timestamp': trade_time,
                    'portfolio_value': portfolio_value,
                    'action': 'exit',
                    'symbol': symbol,
                    'trade_uuid': uuid,
                    'price': price,
                    'pnl': pnl
                }
                append_equity_log(equity_log_path, log_row)
                if position_pct >= entry['position']:
                    del open_positions[uuid]
                else:
                    open_positions[uuid]['position'] -= position_pct

    trades.to_csv(tradelog_path, index=False)

if __name__ == "__main__":
    main()
