import pandas as pd
import numpy as np
import talib
import os
import sys
import pytz
from datetime import datetime, timezone
DATA_DIR = ''
COVERAGE_LOG_PATH = 'symbol_timeframe_coverage.csv'

def log_symbol_coverage(features_path, timeframe):
    df = pd.read_parquet(features_path)
    df['datetime'] = pd.to_datetime(df['timestamp'])
    summary = []
    for symbol, sdf in df.groupby('symbol'):
        n_bars = len(sdf)
        start = sdf['datetime'].min()
        end = sdf['datetime'].max()
        sufficient = n_bars >= 1000
        summary.append({
            'symbol': symbol,
            'timeframe': timeframe,
            'n_bars': n_bars,
            'start': start,
            'end': end,
            'sufficient': sufficient
        })
    summary_df = pd.DataFrame(summary)
    # Append or overwrite; here we append and deduplicate on ['symbol', 'timeframe']
    if os.path.exists(COVERAGE_LOG_PATH):
        existing = pd.read_csv(COVERAGE_LOG_PATH, parse_dates=['start', 'end'])
        combined = pd.concat([existing, summary_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=['symbol', 'timeframe'], keep='last')
        combined.to_csv(COVERAGE_LOG_PATH, index=False)
    else:
        summary_df.to_csv(COVERAGE_LOG_PATH, index=False)
    print(f"Coverage summary saved to {COVERAGE_LOG_PATH}")


def clean_ohlcv(df):
    # Drop flat bars
    flat = (df['open'] == df['high']) & (df['open'] == df['low']) & (df['open'] == df['close'])
    # Drop zero or missing volume
    no_vol = df['volume'].isna() | (df['volume'] == 0)
    before = len(df)
    df_clean = df[~(flat | no_vol)].reset_index(drop=True)
    after = len(df_clean)
    print(f"  Dropped {before - after} bars (flat or zero/missing volume)")

    now = datetime.now(timezone.utc)

    # Map timeframe string to pandas offset
    timeframe_map = {
        "30m": pd.Timedelta(minutes=30),
        "1h": pd.Timedelta(hours=1),
        "4h": pd.Timedelta(hours=4),
        "12h": pd.Timedelta(hours=12),
        "1d": pd.Timedelta(days=1)
    }
    delta = timeframe_map.get(timeframe)
    if delta is None:
        raise ValueError(f"Unknown timeframe: {timeframe}")

    # What is the latest possible timestamp (rounded down to nearest bar)
    latest_possible = (now - delta)
    latest_possible = latest_possible - (latest_possible - pd.Timestamp("1970-01-01", tz="UTC")) % delta

    # Get the last timestamp in the df
    if df.empty:
        return pd.DataFrame()
    last_ts = df['timestamp'].max()
    if pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        last_ts = last_ts
    else:
        last_ts = pd.to_datetime(last_ts, utc=True)

    # If last_ts is before latest_possible, drop symbol
    if last_ts < latest_possible:
        print(f"Dropping symbol: data too old. Last bar: {last_ts}, expected at least: {latest_possible}")
        return pd.DataFrame()  # Or None, or raise, depending on your script

    # ...rest of cleaning...
    return df_clean



def generate_features_for_timeframe(timeframe):
    for symbol_folder in os.listdir(DATA_DIR):
        folder_path = os.path.join(DATA_DIR, symbol_folder)
        if not os.path.isdir(folder_path):
            continue
        in_path = os.path.join(folder_path, f'{timeframe}.parquet')
        out_path = os.path.join(folder_path, f'{timeframe}_features.parquet')
        if not os.path.exists(in_path):
            print(f"Raw data file missing: {in_path}")
            continue

        # --- Load Data ---
                # --- Load & Clean Data ---
        df = pd.read_parquet(in_path)
        if df.empty or len(df) < 50:
            print(f"Not enough data in {in_path}")
            continue
        df = df.sort_values('timestamp').reset_index(drop=True)
        df = clean_ohlcv(df)
        if len(df) < 50:
            print(f"Not enough data after cleaning in {in_path}")
            continue


        # --- Lagged Returns ---
        for lag in [1, 3, 6, 12]:
            df[f'return_{lag}'] = df['close'].pct_change(lag)

        # --- Rolling Statistics ---
        windows = [6, 12, 24]
        for win in windows:
            df[f'close_mean_{win}'] = df['close'].rolling(win).mean()
            df[f'close_std_{win}'] = df['close'].rolling(win).std()
            df[f'close_z_{win}'] = (df['close'] - df[f'close_mean_{win}']) / df[f'close_std_{win}']
            df[f'hl_range_{win}'] = (df['high'] - df['low']).rolling(win).mean() / df['close']

        # --- Volume Features ---
        for win in windows:
            df[f'volume_mean_{win}'] = df['volume'].rolling(win).mean()
            df[f'volume_std_{win}'] = df['volume'].rolling(win).std()

        # --- TA-Lib Indicators ---
        df['rsi_14'] = talib.RSI(df['close'], timeperiod=14)
        df['ema_12'] = talib.EMA(df['close'], timeperiod=12)
        df['ema_26'] = talib.EMA(df['close'], timeperiod=26)
        macd, macdsignal, macdhist = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        df['macd'] = macd
        df['macd_signal'] = macdsignal
        df['macd_hist'] = macdhist
        upperband, middleband, lowerband = talib.BBANDS(df['close'], timeperiod=20, nbdevup=2, nbdevdn=2)
        df['bb_upper_20'] = upperband
        df['bb_middle_20'] = middleband
        df['bb_lower_20'] = lowerband
        df['bb_width_20'] = (upperband - lowerband) / middleband
        df['atr_14'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
        df['adx_14'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
        slowk, slowd = talib.STOCH(df['high'], df['low'], df['close'])
        df['stoch_k'] = slowk
        df['stoch_d'] = slowd
        df['willr_14'] = talib.WILLR(df['high'], df['low'], df['close'], timeperiod=14)
        df['cci_14'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=14)
        df['mfi_14'] = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=14)
        df['obv'] = talib.OBV(df['close'], df['volume'])

        # --- Time Features ---
        df['hour'] = df['timestamp'].dt.hour
        df['dayofweek'] = df['timestamp'].dt.dayofweek

        # --- Targets ---
        df['target_return_1'] = df['close'].shift(-1) / df['close'] - 1
        df['target_up'] = (df['target_return_1'] > 0).astype(int)

        # --- Risk Features Stefan ---
        # Trailing drawdown (max close over last 20 bars)
        df['drawdown_20'] = df['close'] / df['close'].rolling(20).max() - 1

        # Rolling volatility (14 bars)
        df['rolling_std_14'] = df['close'].pct_change().rolling(14).std()

        # Liquidity risk: Low volume flag (less than 50% of 20-bar mean)
        df['low_vol_liquidity'] = (df['volume'] < df['volume'].rolling(20).mean() * 0.5).astype(int)

        # Rolling min/max for possible mean reversion or breakout stops
        df['rolling_max_20'] = df['close'].rolling(20).max()
        df['rolling_min_20'] = df['close'].rolling(20).min()


        # --- Deduplicate and Clean ---
        dedup_cols = ['symbol', 'timestamp', 'timeframe'] if set(['symbol','timestamp','timeframe']).issubset(df.columns) else ['timestamp']
        df = df.drop_duplicates(subset=dedup_cols).dropna().reset_index(drop=True)

        # --- Save Feature Data ---
        df.to_parquet(out_path, index=False)
        print(f"Features saved to: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python feature_engineering_timeframe.py [timeframe]")
        sys.exit(1)
    timeframe = sys.argv[1]
    generate_features_for_timeframe(timeframe)
    # Usage after feature cleaning/engineering
    features_path = os.path.join(DATA_DIR, f"{timeframe}_features.parquet")
    log_symbol_coverage(features_path, timeframe)
