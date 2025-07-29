import ccxt
import pandas as pd
import os
import sys
import time

LIMIT = 300
OUTPUT_DIR = "ohlcv_parquet"
#Symbols can be updated and grow from exchanges or other data sources, just need to align.
SYMBOLS_FILE = "path_to_your_symbols.txt"

def fetch_missing_bars(exchange, symbol, timeframe, since=None):
    all_data = []
    seen_timestamps = set()
    while True:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=LIMIT)
        except Exception as e:
            print(f"Error for {symbol}-{timeframe}: {e}. Skipping this symbol/timeframe.")
            return pd.DataFrame()
        if not ohlcv:
            break
        ohlcv = [row for row in ohlcv if row[0] not in seen_timestamps]
        if not ohlcv:
            break
        all_data.extend(ohlcv)
        seen_timestamps.update(row[0] for row in ohlcv)
        if ohlcv[-1][0] is not None:
            since = ohlcv[-1][0] + 1
        else:
            break
        if len(ohlcv) < LIMIT:
            break
        time.sleep(exchange.rateLimit / 1000 + 0.1)
    if not all_data:
        return pd.DataFrame()
    df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['symbol'] = symbol
    df['timeframe'] = timeframe
    return df

def main():
    if len(sys.argv) != 2:
        print("Usage: [timeframe]")
        sys.exit(1)
    timeframe = sys.argv[1]

    if not os.path.exists(SYMBOLS_FILE):
        print(f"Symbol file not found: {SYMBOLS_FILE}")
        sys.exit(1)
    with open(SYMBOLS_FILE) as f:
        symbols = [line.strip() for line in f if line.strip()]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    exchange = ccxt.okx({'enableRateLimit': True})

    for symbol in symbols:
        print(f"\nProcessing {symbol} - {timeframe}")
        symbol_dir = os.path.join(OUTPUT_DIR, symbol.replace('/', ''))
        os.makedirs(symbol_dir, exist_ok=True)
        outfile = os.path.join(symbol_dir, f"{timeframe}.parquet")

        # Load existing data if available
        if os.path.exists(outfile):
            df_existing = pd.read_parquet(outfile)
            if not df_existing.empty:
                last_ts = df_existing['timestamp'].max()
                since = int(last_ts.value // 10**6) + 1  # ms since epoch
                print(f"Latest timestamp: {last_ts} | Fetching newer bars...")
                df_new = fetch_missing_bars(exchange, symbol, timeframe, since=since)
                if not df_new.empty:
                    combined = pd.concat([df_existing, df_new]).drop_duplicates(subset='timestamp').sort_values('timestamp').reset_index(drop=True)
                    combined.to_parquet(outfile, index=False)
                    print(f"Updated {symbol} - {timeframe} with {len(df_new)} new bars (Total: {len(combined)})")
                else:
                    print(f"No new bars to add for {symbol} - {timeframe}")
            else:
                print(f"Existing file is empty, downloading full history...")
                df_full = fetch_missing_bars(exchange, symbol, timeframe)
                if not df_full.empty:
                    df_full.to_parquet(outfile, index=False)
                    print(f"Saved {symbol} - {timeframe} ({len(df_full)} bars)")
                else:
                    print(f"No data found for {symbol} - {timeframe}")
        else:
            print(f"No local data found for {symbol} - {timeframe}, downloading full history...")
            df_full = fetch_missing_bars(exchange, symbol, timeframe)
            if not df_full.empty:
                df_full.to_parquet(outfile, index=False)
                print(f"Saved {symbol} - {timeframe} ({len(df_full)} bars)")
            else:
                print(f"No data found for {symbol} - {timeframe}")

if __name__ == "__main__":
    main()
