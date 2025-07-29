"""
Orchestrates batch loading of historical data for crypto assets.
"""
from fetch_historical_data import fetch_data

for symbol in ["BTC-USDT", "ETH-USDT"]:
    for interval in ["30m", "1h"]:
        fetch_data(symbol=symbol, interval=interval)
