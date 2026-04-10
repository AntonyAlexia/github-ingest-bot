import os
import json
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup GCP (Keep your secret key for Google, but none needed for yfinance!)
gcp_json = os.environ.get('GCP_SA_KEY')
gcp_info = json.loads(gcp_json)
credentials = service_account.Credentials.from_service_account_info(gcp_info)

PROJECT_ID = gcp_info['project_id']
DATASET_ID = 'market_raw'
TABLE_ID = 'daily_stock_data'

# 2. Your Stock Universe
STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'NFLX', 'AMD', 'INTC', 'PYPL', 'ADBE', 'CSCO', 'PEP', 'COST']

def run_ingestion():
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    print(f"🚀 Fetching total historical data for {len(STOCKS)} symbols...")
    
    # yfinance pulls data in one giant block - super fast
    data = yf.download(STOCKS, period="max", interval="1d", group_by='ticker')
    
    all_rows = []
    for symbol in STOCKS:
        if symbol in data:
            # Drop empty rows and loop through history
            symbol_data = data[symbol].dropna()
            for date, row in symbol_data.iterrows():
                all_rows.append({
                    "symbol": symbol,
                    "date": date.strftime('%Y-%m-%d'),
                    "close": float(row['Close']),
                    "volume": int(row['Volume'])
                })
            print(f"✅ Processed {symbol}: {len(symbol_data)} rows found.")

    if all_rows:
        df = pd.DataFrame(all_rows)
        # WRITE_TRUNCATE clears out the old 500 rows and puts in the 50,000+ new ones
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        print(f"🔥 MISSION ACCOMPLISHED: {len(all_rows)} rows are now in BigQuery.")

if __name__ == "__main__":
    run_ingestion()
