"""
BigQuery Data Generator - Trade Events and Market Quotes

DATA DOMAIN EXPLANATION:
========================
This simulates TRADING ACTIVITY on the stock trading platform

TRADES_STREAM Table:
- Represents every BUY/SELL order executed by users
- trade_id: Unique transaction identifier
- portfolio_id: Which trading account made this trade (PORT_XXXXXX)
  → THIS LINKS TO MONGODB PORTFOLIOS - CRITICAL FOR FEDERATED QUERIES!
- symbol: Which stock (AAPL, GOOGL, etc.)
- side: BUY or SELL
- price: Price per share at execution
- qty: Number of shares traded
- ts: Exact timestamp of trade
- venue: Which stock exchange executed it (NYSE, NASDAQ)
- event_date: Date (used for partitioning in BigQuery)

QUOTES_DAILY Table:
- Represents daily STOCK MARKET PRICES
- OHLC data: Open, High, Low, Close prices for each day
- volume: Total shares of that stock traded in the market
- Used to calculate current portfolio values

REAL-WORLD ANALOGY:
- trades_stream = Your bank transaction history (every debit/credit)
- quotes_daily = Daily stock market closing prices from financial news

IMPORTANT: portfolio_id here MUST match MongoDB portfolios.portfolio_id
           This is how we connect user demographics to trading activity!
"""

# Install the following packages
# Make sure gcloud sdk is setup in your local and you have connected your
# terminal with gcp account through gcloud init
# pip install faker
# pip install pandas
# pip install google-cloud-bigquery
# pip install google-api-core

# Before running the code in terminal run this command to authenticate with the google using cli
# gcloud auth application-default login

import os
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

fake = Faker()

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-2eef9167-38f3-4223-ac1")
DATASET_ID = "fintech_trading"
TRADES_TABLE = "trades_stream"
QUOTES_TABLE = "quotes_daily"

NUM_TRADES = 50000
NUM_DAYS_HISTORY = 90

# Stock symbols pool (must match MongoDB and GCS)
STOCK_SYMBOLS = [
    'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT',
    'PG', 'JNJ', 'UNH', 'HD', 'BAC', 'MA', 'DIS', 'ADBE', 'CRM', 'NFLX',
    'CSCO', 'INTC', 'PFE', 'KO', 'PEP', 'TMO', 'ABBV', 'NKE', 'MRK', 'ORCL'
]

VENUES = ['NYSE', 'NASDAQ', 'BATS', 'IEX', 'ARCA']  # Stock exchanges
SIDES = ['BUY', 'SELL']

# Realistic price ranges for each stock (approximate current market values)
SYMBOL_PRICES = {
    'AAPL': (150, 200), 'GOOGL': (120, 160), 'MSFT': (300, 400),
    'AMZN': (130, 180), 'TSLA': (180, 300), 'META': (250, 350),
    'NVDA': (400, 600), 'JPM': (140, 180), 'V': (230, 280),
    'WMT': (50, 70), 'PG': (140, 160), 'JNJ': (160, 180),
    'UNH': (450, 550), 'HD': (300, 350), 'BAC': (28, 38),
    'MA': (360, 420), 'DIS': (85, 115), 'ADBE': (480, 580),
    'CRM': (200, 260), 'NFLX': (380, 480), 'CSCO': (48, 58),
    'INTC': (38, 48), 'PFE': (28, 36), 'KO': (58, 65),
    'PEP': (165, 185), 'TMO': (520, 600), 'ABBV': (145, 165),
    'NKE': (100, 120), 'MRK': (105, 125), 'ORCL': (95, 115)
}


def connect_bigquery():
    """Establish BigQuery connection"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print(f"Connected to BigQuery project: {PROJECT_ID}")
        return client
    except GoogleAPIError as e:
        print(f"Failed to connect to BigQuery: {e}")
        print("Using mock mode - data will be saved to CSV files")
        return None


def create_dataset_and_tables(client):
    """Create BigQuery dataset and tables"""
    if client is None:
        return
    
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    
    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {DATASET_ID} ready")
    except GoogleAPIError as e:
        print(f"Warning creating dataset: {e}")
    
    # Define trades_stream schema
    trades_schema = [
        bigquery.SchemaField("trade_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("portfolio_id", "STRING", mode="REQUIRED"),  # Links to MongoDB!
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("side", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("qty", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("venue", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
    ]
    
    # Define quotes_daily schema
    quotes_schema = [
        bigquery.SchemaField("ds", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("open", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("high", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("low", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("volume", "INTEGER", mode="REQUIRED"),
    ]
    
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    trades_table_id = f"{dataset_id}.{TRADES_TABLE}"
    quotes_table_id = f"{dataset_id}.{QUOTES_TABLE}"
    
    # Partition by event_date for efficient querying
    trades_table = bigquery.Table(trades_table_id, schema=trades_schema)
    trades_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="event_date"
    )
    
    quotes_table = bigquery.Table(quotes_table_id, schema=quotes_schema)
    quotes_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ds"
    )
    
    try:
        client.create_table(trades_table, exists_ok=True)
        print(f"Table {TRADES_TABLE} ready")
        
        client.create_table(quotes_table, exists_ok=True)
        print(f"Table {QUOTES_TABLE} ready")
    except GoogleAPIError as e:
        print(f"Warning creating tables: {e}")


def generate_trades(num_trades=NUM_TRADES):
    """
    Generate trade events
    
    Each trade represents a BUY or SELL order:
    - portfolio_id: PORT_000001 to PORT_001500 (matches MongoDB portfolios!)
    - Realistic prices within each stock's typical range
    - Random timestamps over last 90 days
    - Mix of BUY and SELL orders
    
    BUSINESS LOGIC:
    - Each portfolio makes multiple trades
    - Trades distributed across different stocks
    - Some portfolios are more active than others (realistic)
    """
    trades = []
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=NUM_DAYS_HISTORY)
    
    for i in range(num_trades):
        trade_id = f"TRD_{datetime.utcnow().strftime('%Y%m%d')}_{str(i+1).zfill(8)}"
        
        # CRITICAL: portfolio_id must be PORT_000001 to PORT_001500 (matching MongoDB)
        portfolio_id = f"PORT_{str(random.randint(1, 1500)).zfill(6)}"
        
        symbol = random.choice(STOCK_SYMBOLS)
        
        # Generate realistic price within symbol's typical range
        price_min, price_max = SYMBOL_PRICES[symbol]
        price = round(random.uniform(price_min, price_max), 2)
        
        ts = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        trade = {
            'trade_id': trade_id,
            'portfolio_id': portfolio_id,
            'symbol': symbol,
            'side': random.choice(SIDES),
            'price': price,
            'qty': random.randint(1, 500),
            'ts': ts,
            'venue': random.choice(VENUES),
            'event_date': ts.date()
        }
        trades.append(trade)
    
    return pd.DataFrame(trades)


def generate_quotes(num_days=NUM_DAYS_HISTORY):
    """
    Generate daily stock prices (OHLC data)
    
    For each stock, each trading day:
    - open: Opening price
    - high: Highest price of the day
    - low: Lowest price of the day
    - close: Closing price
    - volume: Total shares traded in the market (not just our platform)
    
    BUSINESS LOGIC:
    - Skip weekends (stock market closed)
    - Daily volatility ~2-3% (realistic)
    - High >= Open >= Low
    - Close is between High and Low
    """
    quotes = []
    
    end_date = datetime.utcnow().date()
    
    for symbol in STOCK_SYMBOLS:
        price_min, price_max = SYMBOL_PRICES[symbol]
        base_price = (price_min + price_max) / 2
        
        for day_offset in range(num_days):
            ds = end_date - timedelta(days=day_offset)
            
            # Skip weekends (stock market closed Saturday/Sunday)
            if ds.weekday() >= 5:
                continue
            
            # Generate OHLC with realistic intraday volatility
            daily_volatility = random.uniform(0.98, 1.02)
            open_price = round(base_price * daily_volatility, 2)
            
            high_price = round(open_price * random.uniform(1.00, 1.03), 2)
            low_price = round(open_price * random.uniform(0.97, 1.00), 2)
            close_price = round(random.uniform(low_price, high_price), 2)
            
            volume = random.randint(1000000, 50000000)
            
            quote = {
                'ds': ds,
                'symbol': symbol,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            }
            quotes.append(quote)
    
    return pd.DataFrame(quotes)


def insert_data(client, trades_df, quotes_df):
    """Insert generated data into BigQuery"""
    
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    trades_table_id = f"{dataset_id}.{TRADES_TABLE}"
    quotes_table_id = f"{dataset_id}.{QUOTES_TABLE}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    
    print(f"\nLoading {len(trades_df)} trades into BigQuery...")
    trades_job = client.load_table_from_dataframe(
        trades_df, trades_table_id, job_config=job_config
    )
    trades_job.result()
    print(f"Loaded {len(trades_df)} trades")
    
    print(f"Loading {len(quotes_df)} quotes into BigQuery...")
    quotes_job = client.load_table_from_dataframe(
        quotes_df, quotes_table_id, job_config=job_config
    )
    quotes_job.result()
    print(f"Loaded {len(quotes_df)} quotes")
    
    print("\nBigQuery Statistics:")
    trades_count_query = f"SELECT COUNT(*) as cnt FROM `{trades_table_id}`"
    trades_count = list(client.query(trades_count_query).result())[0].cnt
    print(f"  Total Trades: {trades_count}")
    
    quotes_count_query = f"SELECT COUNT(*) as cnt FROM `{quotes_table_id}`"
    quotes_count = list(client.query(quotes_count_query).result())[0].cnt
    print(f"  Total Quotes: {quotes_count}")


def main():
    """Main execution"""
    print("=" * 80)
    print("BigQuery Data Generator - Fintech Trading Platform")
    print("=" * 80)
    
    client = connect_bigquery()
    
    if client:
        print("\nCreating dataset and tables...")
        create_dataset_and_tables(client)
    
    print(f"\nGenerating {NUM_TRADES} trade events...")
    trades_df = generate_trades(NUM_TRADES)
    
    print(f"Generating daily quotes for {NUM_DAYS_HISTORY} days...")
    quotes_df = generate_quotes(NUM_DAYS_HISTORY)
    
    print(f"\nInserting data into BigQuery...")
    insert_data(client, trades_df, quotes_df)
    
    print("\n" + "=" * 80)
    print("Data generation complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
