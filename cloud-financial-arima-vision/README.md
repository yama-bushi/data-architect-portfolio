# Cloud Financial ARIMA & Vision ML Exploration

See the main portfolio README for details.
# Cloud Financial ARIMA & Vision ML Exploration

This project demonstrates a full pipeline for ingesting, modeling, and analyzing financial time series data (cryptocurrency) using cloud-native tools and basic ML/vision techniques.

## What’s Included

- **Ingestion Pipelines:** Python scripts to fetch crypto data via API, push to Google Cloud Storage, and load to BigQuery.
    Currently running on a free compute engine, keeping data up to date at no cost. 
    Cronjobs to schedule and cleaning out any storage to keep costs low.
- **Feature Engineering:** RSI, Bollinger Bands, and other features created on the fly.
- **Image Generation:** Generates chart images for further image recognition/ML experiments.
- **ML Research:** Jupyter notebook with preliminary experiments on pattern recognition and error rate tracking.
- **ARIMA Modeling:** Leverages BigQuery’s built-in ARIMA time series modeling for forecasting and dashboarding.
    dashboard.png shows sample dashboard created and able to poll every 30 min.
## Structure

- `/src/` – All ingestion, processing, and data pipeline scripts (Python)
- `/notebooks/` – Research and experimentation (Jupyter)
- `requirements.txt` – Dependencies to run locally or in cloud
- `.gitignore` – Keeps secrets and system files out of version control

## Usage

1. Install dependencies:
    pip install -r requirements.txt
2. Set your Google credentials:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service_account_key.json
3. Update any project/dataset/table variables in the scripts for your own GCP environment.

**Author:** Justin Lowe
