import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup Authentication from GitHub Secrets
gcp_json = os.environ.get('GCP_SA_KEY')
gcp_info = json.loads(gcp_json)
credentials = service_account.Credentials.from_service_account_info(gcp_info)

# 2. Configuration
API_KEY = os.environ.get('ALPHA_VANTAGE_KEY')
STOCKS = ['NVDA', 'AAPL', 'GOOGL', 'TSLA', 'MSFT']
PROJECT_ID = gcp_info['project_id']
DATASET_ID = 'market_raw'
TABLE_ID = 'daily_stock_data'

def run_ingestion():
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    all_data = []
    for symbol in STOCKS:
        # Added &outputsize=full to get 20+ years of history (or the last 100 days)
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={API_KEY}'
        r = requests.get(url).json()
        
        if "Time Series (Daily)" in r:
            time_series = r["Time Series (Daily)"]
            # Loop through ALL dates returned by the API (usually the last 100)
            for date, row in time_series.items():
                all_data.append({
                    "symbol": symbol,
                    "date": date,
                    "close": float(row["4. close"]),
                    "volume": int(row["5. volume"])
                })
        
        # IMPORTANT: Alpha Vantage Free Tier only allows 5 calls per minute.
        # We wait 15 seconds between stocks so we don't get blocked.
        import time
        time.sleep(15)

    if all_data:
        df = pd.DataFrame(all_data)
        # We use WRITE_TRUNCATE just for this first "backfill" to clean out the 1-row test
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        print(f"✅ Success! Backfilled {len(all_data)} rows.")


if __name__ == "__main__":
    run_ingestion()
