# Financial Markets ML Research (Portfolio Demo)

This project grew out of my exploration of concepts in Stefan Jansen’s *Machine Learning for Algorithmic Trading*. Inspired by the book’s deep dive into modern quantitative finance, I set out to build my own **cloud-native, ML-driven data pipeline** for financial market research and experimentation.

Here, you’ll find modular Python code demonstrating end-to-end data engineering for market data: ingestion, feature engineering, modeling, parameter optimization, backtesting, and (simulated) paper trading—*without any proprietary trading logic or live signals*.

## What’s Included:
- Modular Python scripts for fetching, cleaning, and feature engineering of time series data
- ML training and inference modules
- Automated parameter optimization workflow
- Backtesting and logging for research/monitoring
- Simulated execution ("paper trading") only

**What’s NOT Included:**
- No live keys, API secrets, or wallet addresses

## Project Structure:
src/
fetch_market_data.py
feature_engineering_timeframe.py
aggregate_features_by_timeframe.py
train_timeframe_model.py
inference_timeframe.py
backtest_by_timeframe.py
trade_ideas_logger.py
live_paper_trading_bot.py
optimize_params.py
orchestration/
30m.bat
.env.example
requirements.txt
.gitignore

## Next Steps:
- Adding in robust alternative data and combining with OHLCV
- Per Symbol models light up at 1000 bars (usually takes 2+ weeks to fill from free source)
    - Parameter tuning for weighting
    - Test which wins, winner take all or weighting
- Heartbeat monitor to close out paper trading
- Live trading

> **For recruiters/hiring managers:**  
> This codebase is intended solely as a demonstration of technical proficiency in data/ML engineering and workflow automation.  
> *It is not a financial product, nor is it intended as investment advice or a trading tool.*

**Author:** Justin Lowe  
