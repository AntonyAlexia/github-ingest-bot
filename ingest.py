import os
import json
import requests
import pandas as pd
import time
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup
gcp_json = os.environ.get('GCP_SA_KEY')
gcp_info = json.loads(gcp_json)
credentials = service_account.Credentials.from_service_account_info(gcp_info)

API_KEY = os.environ.get('ALPHA_VANTAGE_KEY')
# Expanded list to reach high row counts
STOCKS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'AVGO', 'PEP', 'COST',
    'ADBE', 'CSCO', 'AMD', 'NFLX', 'INTC', 'TMUS', 'TXN', 'AMAT', 'QCOM', 'ISRG',
    'HON', 'INTU', 'AMGN', 'VRTX', 'SBUX'
]

PROJECT_ID = gcp_info['project_id']
DATASET_ID = 'market_raw'
TABLE_ID = 'daily_stock_data'

def run_ingestion():
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    all_data = []

    print(f"🚀 Starting ingestion for {len(STOCKS)} symbols...")

    for i, symbol in enumerate(STOCKS):
        # We use 'full' to get 20 years of history per stock
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={API_KEY}'
        
        try:
            r = requests.get(url).json()
            if "Time Series (Daily)" in r:
                ts = r["Time Series (Daily)"]
                for date, values in ts.items():
                    all_data.append({
                        "symbol": symbol,
                        "date": date,
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"])
                    })
                print(f"✅ Loaded {symbol} ({len(ts)} rows)")
            else:
                print(f"⚠️ Skip {symbol}: API limit or No Data.")
        except Exception as e:
            print(f"❌ Error on {symbol}: {e}")

        # ALPHA VANTAGE PROTECTION: 5 calls per minute. 
        # We wait 15 seconds between every call to ensure we don't get blocked.
        if i < len(STOCKS) - 1:
            time.sleep(15)

    if all_data:
        df = pd.DataFrame(all_data)
        # Use WRITE_TRUNCATE to replace the old 1-row data with this massive historical set
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        print(f"🔥 FINAL SUCCESS: Total Rows in BigQuery: {len(all_data)}")

if __name__ == "__main__":
    run_ingestion()
