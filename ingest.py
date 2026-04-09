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
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}'
        r = requests.get(url).json()
        
        if "Time Series (Daily)" in r:
            latest_date = list(r["Time Series (Daily)"].keys())[0]
            row = r["Time Series (Daily)"][latest_date]
            all_data.append({
                "symbol": symbol,
                "date": latest_date,
                "close": float(row["4. close"]),
                "volume": int(row["5. volume"])
            })

    if all_data:
        df = pd.DataFrame(all_data)
        # Load to BigQuery (Append mode)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        print(f"✅ Successfully ingested {len(all_data)} rows.")

if __name__ == "__main__":
    run_ingestion()
