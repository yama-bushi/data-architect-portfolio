"""
Runs backfill job to populate a crypto data lake.
"""
from fill_historical_data import fill_historical_data

for symbol in ["BTC-USDT", "ETH-USDT"]:
    for interval in ["30m", "1h"]:
        for _ in range(48):
            fill_historical_data(symbol, interval)
