# Financial Markets ML Research (Portfolio Demo)

This project demonstrates my engineering skills in building a **cloud-native, ML-driven data pipeline** for financial market research. It includes data ingestion, feature engineering, modeling, parameter optimization, backtesting, and (simulated) paper trading—*without any proprietary trading logic or live strategies*.

## What’s Included:
- Modular Python scripts for fetching, cleaning, and feature engineering of time series data
- ML training and inference modules
- Automated parameter optimization workflow
- Backtesting and logging for research/monitoring
- Simulated execution ("paper trading") only

**What’s NOT Included:**
- No proprietary trading logic, alpha signals, or production secrets
- No live keys, API secrets, or wallet addresses

## Project Structure:
src/
fetch_okx_data.py
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


> **For recruiters/hiring managers:**  
> This codebase is intended solely as a demonstration of technical proficiency in data/ML engineering and workflow automation.  
> *It is not a financial product, nor is it intended as investment advice or a trading tool.*

**Author:** Justin Lowe  
