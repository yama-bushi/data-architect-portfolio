REM Sample orchestration script for pipeline demo
REM Calls all core pipeline scripts every 30 minutes (for local/demo use)
python fetch_market_data.py
python feature_engineering_timeframe.py
python aggregate_features_by_timeframe.py
python train_timeframe_model.py
python inference_timeframe.py
python backtest_by_timeframe.py
python trade_ideas_logger.py
python live_paper_trading_bot.py
