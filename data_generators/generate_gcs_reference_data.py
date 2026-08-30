"""
GCS Reference Data Generator - FX Rates and Symbol Master

DATA DOMAIN EXPLANATION:
========================
This generates REFERENCE DATA (slowly changing, supporting data)

1. FX RATES (Currency Exchange Rates):
   - Daily exchange rates between currencies
   - Example: 1 USD = 0.92 EUR, 1 USD = 149.5 JPY
   - Generated for LAST 90 DAYS (historical backfill)
   - In production, new files would be added daily
   
   FILE STRUCTURE:
   gs://bucket/reference/fx_rates/ds=2025-01-10/fx_rates.csv
   gs://bucket/reference/fx_rates/ds=2025-01-11/fx_rates.csv
   gs://bucket/reference/fx_rates/ds=2025-01-12/fx_rates.csv
   ... (90 days worth)
   
   Each file contains ~100 rows (10 currencies × 10 currencies)

2. SYMBOLS (Stock Metadata):
   - Static reference: Which exchange, which sector
   - ONE-TIME load (doesn't change daily)
   - Updates only when new stocks are added to platform
   
   FILE STRUCTURE:
   gs://bucket/reference/symbols/symbols_master.csv
   
   Contains 30 stocks with their metadata

WHEN TO RUN:
- FIRST TIME: Run once to create 90 days of historical FX data
- ONGOING: In production, would run daily to add today's FX rates
- SYMBOLS: Run only when adding new stocks (rare)

FOR THIS PROJECT:
- Run ONCE during initial setup
- Gives you 90 days of FX history to work with
- Enough for testing and demos
"""

# Install the following packages
# Make sure gcloud sdk is setup in your local and you have connected your
# terminal with gcp account through gcloud init
# pip install pandas
# pip install google-cloud-storage
# pip install google-api-core
# pip install google-cloud-storage
# Before running the code in terminal run this command to authenticate with the google using cli
# gcloud auth application-default login

import os
import random
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-2eef9167-38f3-4223-ac1")
GCS_BUCKET = os.getenv("GCS_BUCKET", "fintech-trading-lakehouse-gds1")
FX_RATES_PREFIX = "reference/fx_rates"
SYMBOLS_PREFIX = "reference/symbols"

# Generate 90 days of historical FX data
# In production, you'd run this daily to add just today's rates
NUM_DAYS_HISTORY = 90

# 10 major currencies
BASE_CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'CNY', 'HKD', 'SGD']

# 30 stocks available on the platform
STOCK_SYMBOLS = [
    {'symbol': 'AAPL', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'GOOGL', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'MSFT', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'AMZN', 'exchange': 'NASDAQ', 'sector': 'Consumer Cyclical', 'tick_size': 0.01},
    {'symbol': 'TSLA', 'exchange': 'NASDAQ', 'sector': 'Consumer Cyclical', 'tick_size': 0.01},
    {'symbol': 'META', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'NVDA', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'JPM', 'exchange': 'NYSE', 'sector': 'Financial Services', 'tick_size': 0.01},
    {'symbol': 'V', 'exchange': 'NYSE', 'sector': 'Financial Services', 'tick_size': 0.01},
    {'symbol': 'WMT', 'exchange': 'NYSE', 'sector': 'Consumer Defensive', 'tick_size': 0.01},
    {'symbol': 'PG', 'exchange': 'NYSE', 'sector': 'Consumer Defensive', 'tick_size': 0.01},
    {'symbol': 'JNJ', 'exchange': 'NYSE', 'sector': 'Healthcare', 'tick_size': 0.01},
    {'symbol': 'UNH', 'exchange': 'NYSE', 'sector': 'Healthcare', 'tick_size': 0.01},
    {'symbol': 'HD', 'exchange': 'NYSE', 'sector': 'Consumer Cyclical', 'tick_size': 0.01},
    {'symbol': 'BAC', 'exchange': 'NYSE', 'sector': 'Financial Services', 'tick_size': 0.01},
    {'symbol': 'MA', 'exchange': 'NYSE', 'sector': 'Financial Services', 'tick_size': 0.01},
    {'symbol': 'DIS', 'exchange': 'NYSE', 'sector': 'Communication Services', 'tick_size': 0.01},
    {'symbol': 'ADBE', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'CRM', 'exchange': 'NYSE', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'NFLX', 'exchange': 'NASDAQ', 'sector': 'Communication Services', 'tick_size': 0.01},
    {'symbol': 'CSCO', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'INTC', 'exchange': 'NASDAQ', 'sector': 'Technology', 'tick_size': 0.01},
    {'symbol': 'PFE', 'exchange': 'NYSE', 'sector': 'Healthcare', 'tick_size': 0.01},
    {'symbol': 'KO', 'exchange': 'NYSE', 'sector': 'Consumer Defensive', 'tick_size': 0.01},
    {'symbol': 'PEP', 'exchange': 'NASDAQ', 'sector': 'Consumer Defensive', 'tick_size': 0.01},
    {'symbol': 'TMO', 'exchange': 'NYSE', 'sector': 'Healthcare', 'tick_size': 0.01},
    {'symbol': 'ABBV', 'exchange': 'NYSE', 'sector': 'Healthcare', 'tick_size': 0.01},
    {'symbol': 'NKE', 'exchange': 'NYSE', 'sector': 'Consumer Cyclical', 'tick_size': 0.01},
    {'symbol': 'MRK', 'exchange': 'NYSE', 'sector': 'Healthcare', 'tick_size': 0.01},
    {'symbol': 'ORCL', 'exchange': 'NYSE', 'sector': 'Technology', 'tick_size': 0.01}
]

# Base FX rates (approximate real values as of 2024-2025)
FX_BASE_RATES = {
    'USD': 1.0,
    'EUR': 0.92,
    'GBP': 0.79,
    'JPY': 149.5,
    'AUD': 1.54,
    'CAD': 1.36,
    'CHF': 0.88,
    'CNY': 7.24,
    'HKD': 7.83,
    'SGD': 1.35
}


def connect_gcs():
    """Establish GCS connection"""
    try:
        client = storage.Client(project=PROJECT_ID)
        print(f"Connected to GCS project: {PROJECT_ID}")
        return client
    except GoogleAPIError as e:
        print(f"Failed to connect to GCS: {e}")
        print("Using mock mode - data will be saved to local files")
        return None


def create_bucket(client):
    """Create GCS bucket if it doesn't exist"""
    if client is None:
        return
    
    try:
        bucket = client.bucket(GCS_BUCKET)
        if not bucket.exists():
            bucket = client.create_bucket(GCS_BUCKET, location="US")
            print(f"Created bucket: {GCS_BUCKET}")
        else:
            print(f"Bucket {GCS_BUCKET} already exists")
    except GoogleAPIError as e:
        print(f"Warning with bucket: {e}")


def generate_fx_rates(num_days=NUM_DAYS_HISTORY):
    """
    Generate daily FX rates consecutively from current date to 90 days back
    
    GENERATION ORDER:
    - Starts from TODAY and goes backward 90 days
    - Creates files in consecutive date order
    - Includes ALL days (weekends too) for complete coverage
    
    WHAT THIS CREATES:
    - 90 separate CSV files (one per day, including weekends)
    - Each file contains 100 currency pair rates (10×10 matrix)
    - Files stored in date-partitioned structure
    
    EXAMPLE GENERATION:
    Day 0:  2025-01-10 (Friday) → reference/fx_rates/ds=2025-01-10/fx_rates.csv
    Day -1: 2025-01-09 (Thursday) → reference/fx_rates/ds=2025-01-09/fx_rates.csv
    Day -2: 2025-01-08 (Wednesday) → reference/fx_rates/ds=2025-01-08/fx_rates.csv
    Day -3: 2025-01-07 (Tuesday) → reference/fx_rates/ds=2025-01-07/fx_rates.csv
    Day -4: 2025-01-06 (Monday) → reference/fx_rates/ds=2025-01-06/fx_rates.csv
    Day -5: 2025-01-05 (Sunday) → reference/fx_rates/ds=2025-01-05/fx_rates.csv
    Day -6: 2025-01-04 (Saturday) → reference/fx_rates/ds=2025-01-04/fx_rates.csv
    ...
    Day -90: 2024-10-12 → reference/fx_rates/ds=2024-10-12/fx_rates.csv
    
    WHEN TO RUN:
    - INITIAL SETUP: Run once to create 90 days history
    - ONGOING: In production, run daily to add just today's rates
    """
    fx_rates_by_date = {}
    
    # Start from current date and go backwards
    current_date = datetime.utcnow().date()
    
    print(f"\nGenerating FX rates from {current_date} back to {current_date - timedelta(days=num_days-1)}")
    print("Processing ALL dates consecutively (including weekends)...\n")
    
    # Generate dates in reverse chronological order (today → 90 days ago)
    for day_offset in range(num_days):
        ds = current_date - timedelta(days=day_offset)
        
        print(f"  Day -{day_offset}: {ds} ({ds.strftime('%A')}) → Generating rates...")
        
        rates = []
        
        # Generate rates for all currency pairs (10 × 10 = 100 pairs)
        for base_ccy in BASE_CURRENCIES:
            for quote_ccy in BASE_CURRENCIES:
                if base_ccy == quote_ccy:
                    rate = 1.0  # 1 USD = 1 USD, 1 EUR = 1 EUR, etc.
                else:
                    # Calculate cross rate with daily volatility
                    base_rate = FX_BASE_RATES[base_ccy]
                    quote_rate = FX_BASE_RATES[quote_ccy]
                    cross_rate = quote_rate / base_rate
                    
                    # Add daily volatility (±0.5% to simulate real market)
                    volatility = random.uniform(0.995, 1.005)
                    rate = round(cross_rate * volatility, 6)
                
                rates.append({
                    'base_ccy': base_ccy,
                    'quote_ccy': quote_ccy,
                    'rate': rate,
                    'ds': ds
                })
        
        fx_rates_by_date[ds] = pd.DataFrame(rates)
    
    print(f"\nGenerated rates for {len(fx_rates_by_date)} days (including weekends)")
    return fx_rates_by_date


def generate_symbols():
    """
    Generate symbol master data (static reference)
    
    WHAT THIS CREATES:
    - ONE CSV file with 30 stocks
    - Stock metadata that rarely changes
    
    REAL-WORLD MEANING:
    - List of all stocks available on your platform
    - Which exchange they trade on
    - Which industry sector they belong to
    
    FILE STRUCTURE:
    reference/symbols/symbols_master.csv
    
    WHEN TO RUN:
    - INITIAL SETUP: Run once
    - UPDATES: Only when adding new stocks to platform (rare)
    
    FOR THIS PROJECT:
    - Run ONCE during setup
    - 30 stocks is enough for demos
    """
    return pd.DataFrame(STOCK_SYMBOLS)


def save_fx_rates_local(fx_rates_by_date):
    """Save FX rates to local CSV files (mock mode)"""
    local_dir = "/Users/shashankmishra/Desktop/DE 5.0 With Azure/Trino Class 2/data_generators/fx_rates"
    os.makedirs(local_dir, exist_ok=True)
    
    files_created = []
    for ds, df in fx_rates_by_date.items():
        date_dir = os.path.join(local_dir, f"ds={ds}")
        os.makedirs(date_dir, exist_ok=True)
        
        file_path = os.path.join(date_dir, "fx_rates.csv")
        df.to_csv(file_path, index=False)
        files_created.append(file_path)
    
    return files_created


def save_symbols_local(symbols_df):
    """Save symbol master to local CSV (mock mode)"""
    local_dir = "/Users/shashankmishra/Desktop/DE 5.0 With Azure/Trino Class 2/data_generators/symbols"
    os.makedirs(local_dir, exist_ok=True)
    
    file_path = os.path.join(local_dir, "symbols_master.csv")
    symbols_df.to_csv(file_path, index=False)
    
    return file_path


def upload_fx_rates_gcs(client, fx_rates_by_date):
    """Upload FX rates to GCS"""
    if client is None:
        return save_fx_rates_local(fx_rates_by_date)
    
    bucket = client.bucket(GCS_BUCKET)
    files_uploaded = []
    
    for ds, df in fx_rates_by_date.items():
        blob_path = f"{FX_RATES_PREFIX}/ds={ds}/fx_rates.csv"
        blob = bucket.blob(blob_path)
        
        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
        files_uploaded.append(f"gs://{GCS_BUCKET}/{blob_path}")
    
    return files_uploaded


def upload_symbols_gcs(client, symbols_df):
    """Upload symbol master to GCS"""
    if client is None:
        return save_symbols_local(symbols_df)
    
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"{SYMBOLS_PREFIX}/symbols_master.csv"
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(symbols_df.to_csv(index=False), content_type='text/csv')
    
    return f"gs://{GCS_BUCKET}/{blob_path}"


def main():
    """Main execution"""
    print("=" * 80)
    print("GCS Reference Data Generator")
    print("=" * 80)
    print("\nWHAT THIS GENERATES:")
    print("1. FX Rates: 90 DAYS of historical currency exchange rates")
    print("   - ~60 files (weekdays only)")
    print("   - Each file: 100 currency pairs")
    print("   - Total: ~6,000 rows")
    print("\n2. Symbols: ONE file with 30 stocks metadata")
    print("   - Static reference data")
    print("   - Rarely updated")
    print("=" * 80)
    print()
    
    client = connect_gcs()
    
    if client:
        print("\nChecking/creating GCS bucket...")
        create_bucket(client)
    
    print(f"\nGenerating FX rates for {NUM_DAYS_HISTORY} days...")
    print("(This creates historical data - run ONCE during setup)")
    fx_rates_by_date = generate_fx_rates(NUM_DAYS_HISTORY)
    print(f"Generated FX rates for {len(fx_rates_by_date)} trading days")
    
    print(f"\nGenerating symbol master data...")
    symbols_df = generate_symbols()
    print(f"Generated metadata for {len(symbols_df)} symbols")
    
    print(f"\nUploading FX rates...")
    fx_files = upload_fx_rates_gcs(client, fx_rates_by_date)
    print(f"Uploaded/Saved {len(fx_files)} FX rate files")
    
    if client is None:
        print(f"Sample local files: {fx_files[:3]}")
    else:
        print(f"Sample GCS paths: {fx_files[:3]}")
    
    print(f"\nUploading symbol master...")
    symbols_file = upload_symbols_gcs(client, symbols_df)
    print(f"Uploaded/Saved symbol master")
    print(f"Location: {symbols_file}")
    
    print("\nSample FX Rates (Latest Day):")
    latest_date = max(fx_rates_by_date.keys())
    print(fx_rates_by_date[latest_date].head(10))
    
    print("\nSymbol Master Data:")
    print(symbols_df)
    
    print("\n" + "=" * 80)
    print("Data generation complete!")
    print("\nIN PRODUCTION:")
    print("- Run this DAILY to add just today's FX rates")
    print("- Symbols: Update only when adding new stocks")
    print("\nFOR THIS PROJECT:")
    print("- Run ONCE during setup")
    print("- You now have 90 days of data to work with")
    print("=" * 80)


if __name__ == "__main__":
    main()
