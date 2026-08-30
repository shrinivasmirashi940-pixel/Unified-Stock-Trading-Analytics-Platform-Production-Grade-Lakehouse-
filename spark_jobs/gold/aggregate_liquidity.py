"""
Gold Layer: Aggregate Daily Liquidity Metrics by Symbol

DATA DOMAIN EXPLANATION:
========================
This job calculates MARKET LIQUIDITY for each stock

WHAT IS LIQUIDITY:
- How actively a stock is being traded
- High liquidity = easy to buy/sell (many trades, high volume)
- Low liquidity = hard to buy/sell (few trades, low volume)

METRICS CALCULATED:
1. total_volume: Total shares traded (e.g., 2.5 million shares of AAPL)
2. total_turnover_usd: Total dollar value traded (e.g., $438 million)
3. num_trades: Count of transactions (e.g., 15,234 trades)
4. unique_portfolios: How many different users traded this stock
5. avg_trade_size: Average dollar amount per trade
6. price_volatility: How much price fluctuated (stddev of prices)

BUSINESS VALUE:
- Identify most popular stocks (high volume)
- Detect illiquid stocks (low volume, risky to trade)
- Monitor market activity trends
- Optimize trading fees based on liquidity

EXAMPLE OUTPUT:
Symbol AAPL on 2025-01-10:
  - total_volume: 2,500,000 shares
  - total_turnover_usd: $438,750,000
  - num_trades: 15,234
  - unique_portfolios: 4,567 users traded this
  - price_volatility: $2.35 (price swings)

FEDERATED QUERY USE:
- Can join with MongoDB to see "which region trades AAPL most?"
- NO direct portfolio_id link (this is symbol-level, not portfolio-level)
"""

import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(app_name):
    """Initialize Spark with Hive support using Dataproc Hive metastore"""
    gcs_bucket = os.getenv("GCS_BUCKET", "fintech-trading-lakehouse-gds1")
    warehouse_path = f"gs://{gcs_bucket}/warehouse"

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver_trades(spark, start_date, end_date):
    """
    Read validated trades from Silver
    
    Includes portfolio_id to count unique_portfolios (how many users traded each symbol)
    """
    print(f"Reading enriched trades from Silver, date range: {start_date} to {end_date}")
    
    df = spark.sql(f"""
        SELECT event_date as ds, symbol, exchange, sector, price_usd, qty, notional_usd, portfolio_id, is_valid
        FROM silver_v1.trades_enriched
        WHERE event_date >= '{start_date}' AND event_date <= '{end_date}' AND is_valid = true
    """)
    
    print(f"Read {df.count()} valid trades")
    return df


def calculate_liquidity_metrics(trades_df):
    """
    Aggregate trading activity by symbol
    
    AGGREGATION LOGIC:
    - Group by: date, symbol, exchange, sector
    - Sum quantities → total volume
    - Sum notional → total turnover
    - Count trades → num_trades
    - Count distinct portfolios → how many users
    - Stddev of prices → volatility measure
    
    BUSINESS INTERPRETATION:
    - High volume + low volatility = stable, liquid stock
    - Low volume + high volatility = risky, illiquid stock
    - High unique_portfolios = broadly popular stock
    """
    print("Calculating liquidity metrics")
    
    liquidity_df = trades_df.groupBy("ds", "symbol", "exchange", "sector").agg(
        F.sum("qty").cast("long").alias("total_volume"),
        F.sum("notional_usd").alias("total_turnover_usd"),
        F.count("*").cast("int").alias("num_trades"),
        F.countDistinct("portfolio_id").cast("int").alias("unique_portfolios"),
        F.avg("notional_usd").alias("avg_trade_size"),
        F.stddev("price_usd").alias("price_volatility")
    )
    
    # Handle nulls (single trade = no stddev)
    liquidity_df = liquidity_df.fillna(0.0, subset=["price_volatility"])
    
    return liquidity_df


def main():
    """Main execution"""
    end_date = os.getenv("END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start_date = os.getenv("START_DATE", end_date)
    
    print(f"Configuration: Date Range={start_date} to {end_date}")
    
    spark = create_spark_session("Gold_Liquidity_Aggregation")
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS gold_v1")
        
        trades_df = read_silver_trades(spark, start_date, end_date)
        
        if trades_df.count() == 0:
            print("No valid trades found in Silver")
            return
        
        liquidity_df = calculate_liquidity_metrics(trades_df)
        
        print("Sample liquidity records (top by turnover):")
        liquidity_df.orderBy(F.desc("total_turnover_usd")).show(10, truncate=False)
        
        # Write to Gold layer
        liquidity_df.write \
            .mode("overwrite") \
            .partitionBy("ds") \
            .saveAsTable("gold_v1.fact_liquidity")
        
        print(f"Written {liquidity_df.count()} records to gold_v1.fact_liquidity")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
