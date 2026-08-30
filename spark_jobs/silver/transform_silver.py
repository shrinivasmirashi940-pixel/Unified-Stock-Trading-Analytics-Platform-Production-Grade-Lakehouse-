"""
Silver Layer: Transform and Enrich Trades

DATA DOMAIN EXPLANATION:
========================
This job creates ENRICHED TRADE RECORDS ready for analytics

INPUT:
- BigQuery trades_stream: Raw buy/sell orders (NO BRONZE COPY NEEDED!)
- Bronze symbols: Stock metadata (exchange, sector)

OUTPUT:
- silver.trades_enriched: Trades with additional context

WHAT IT DOES:
1. Reads trades DIRECTLY from BigQuery (incremental by date)
2. Joins with symbol master to add exchange, sector info
3. Validates data quality (price > 0, qty > 0, valid symbol)
4. Calculates notional value (price × quantity in USD)
5. Assigns quality score (100 = perfect, lower = issues)

BUSINESS VALUE:
- Clean, validated trades ready for aggregation
- Enriched with business context (which exchange, which sector)
- Quality flags help identify data issues

EXAMPLE TRANSFORMATION:
Input (BigQuery):
  portfolio_id: PORT_000001, symbol: AAPL, side: BUY, price: 175.50, qty: 50

Output (Iceberg):
  portfolio_id: PORT_000001, symbol: AAPL, side: BUY, price: 175.50, qty: 50
  + exchange: NASDAQ (enriched from symbols)
  + sector: Technology (enriched from symbols)
  + price_usd: 175.50 (converted to USD)
  + notional_usd: 8775.00 (calculated: 175.50 × 50)
  + is_valid: true (passed validation)
  + quality_score: 100 (perfect data)
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


def read_bigquery_trades(spark, project_id, dataset, table, start_date, end_date):
    """
    Read trades DIRECTLY from BigQuery (no bronze copy needed)
    
    WHY: BigQuery is already a good data store for raw events
    - Fast columnar storage
    - Partitioned by date
    - Can query incrementally
    
    We read only new data (by date range) to process incrementally
    """
    print(f"Reading trades from BigQuery: {project_id}.{dataset}.{table}")
    print(f"Date range: {start_date} to {end_date}")
    
    bq_table = f"{project_id}.{dataset}.{table}"
    
    df = spark.read \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("filter", f"event_date >= '{start_date}' AND event_date <= '{end_date}'") \
        .load()
    
    print(f"Read {df.count()} trades from BigQuery")
    return df


def read_bronze_symbols(spark):
    """
    Read symbol metadata from Bronze
    
    DOMAIN: Stock reference data
    - symbol: Ticker symbol (AAPL, GOOGL)
    - exchange: Where it trades (NYSE, NASDAQ)
    - sector: Industry classification (Technology, Healthcare, etc.)
    """
    print("Reading symbols from Bronze")
    
    df = spark.sql("SELECT symbol, exchange, sector, tick_size FROM bronze_v1.br_symbols")
    
    print(f"Read {df.count()} symbols")
    return df


def enrich_trades(trades_df, symbols_df):
    """
    Enrich trades with symbol metadata and calculate derived fields
    
    ENRICHMENT LOGIC:
    1. Join with symbols to add exchange, sector
    2. Calculate notional_usd = price × qty (total dollar value of trade)
    3. Validate: price > 0, qty > 0, symbol exists
    4. Assign quality_score:
       - 100 = perfect data
       - 90 = missing sector (minor issue)
       - 50 = validation failed (critical issue)
    
    BUSINESS VALUE:
    - Know which exchange a trade executed on (for venue analysis)
    - Know which sector (for sector performance analysis)
    - Flag bad data before it reaches analytics layer
    """
    print("Enriching trades with symbol metadata")
    
    # Left join to keep trades even if symbol metadata is missing
    enriched = trades_df.join(symbols_df, on="symbol", how="left")
    
    # For simplicity, assume all trades are in USD (price_usd = price)
    # In real system, would join with FX rates for currency conversion
    enriched = enriched.withColumn("price_usd", F.col("price"))
    
    # Calculate notional value (total dollar amount of trade)
    # Example: 50 shares × $175.50/share = $8,775 notional
    enriched = enriched.withColumn(
        "notional_usd",
        F.col("price_usd") * F.col("qty")
    )
    
    # Data validation checks
    enriched = enriched.withColumn(
        "is_valid",
        (F.col("price") > 0) &  # Price must be positive
        (F.col("qty") > 0) &    # Quantity must be positive
        (F.col("symbol").isNotNull()) &  # Symbol must exist
        (F.col("exchange").isNotNull())  # Must have matched with symbols master
    )
    
    # Quality scoring (0-100)
    enriched = enriched.withColumn("quality_score", F.lit(100))
    
    # Deduct 50 points if core validation failed
    enriched = enriched.withColumn(
        "quality_score",
        F.when(~F.col("is_valid"), 50).otherwise(F.col("quality_score"))
    )
    
    # Deduct 10 points if sector is missing (minor issue)
    enriched = enriched.withColumn(
        "quality_score",
        F.when(F.col("sector").isNull(), F.col("quality_score") - 10).otherwise(F.col("quality_score"))
    )
    
    enriched = enriched.withColumn("processed_ts", F.current_timestamp())
    
    return enriched.select(
        "trade_id", "portfolio_id", "symbol", "side", "price", "qty", "ts", "venue",
        "event_date", "exchange", "sector", "price_usd", "notional_usd",
        "is_valid", "quality_score", "processed_ts"
    )


def main():
    """Main execution"""
    # Configuration
    project_id = os.getenv("GCP_PROJECT_ID", "project-2eef9167-38f3-4223-ac1")
    dataset = "fintech_trading"
    table = "trades_stream"
    
    end_date = os.getenv("END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start_date = os.getenv("START_DATE", end_date)
    
    print(f"Configuration: Date Range={start_date} to {end_date}")
    
    spark = create_spark_session("Silver_Trades_Transformation")
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS silver_v1")
        
        # Read trades directly from BigQuery (no bronze copy needed)
        trades_df = read_bigquery_trades(spark, project_id, dataset, table, start_date, end_date)
        
        if trades_df.count() == 0:
            print("No trades found in BigQuery for specified date range")
            return
        
        # Read symbols from bronze
        symbols_df = read_bronze_symbols(spark)
        
        # Enrich trades
        silver_df = enrich_trades(trades_df, symbols_df)
        
        print("Sample enriched records:")
        silver_df.select(
            "trade_id", "symbol", "side", "price", "price_usd", 
            "notional_usd", "exchange", "sector", "is_valid", "quality_score"
        ).show(5, truncate=False)
        
        # Write to Silver layer
        silver_df.write \
            .mode("overwrite") \
            .partitionBy("event_date", "symbol") \
            .saveAsTable("silver_v1.trades_enriched")
        
        print(f"Written {silver_df.count()} records to silver_v1.trades_enriched")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
