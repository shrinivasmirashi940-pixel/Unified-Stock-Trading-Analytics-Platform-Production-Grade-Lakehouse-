"""
Bronze Layer: Ingest FX Rates from GCS to Iceberg

DATA DOMAIN EXPLANATION:
========================
This job loads CURRENCY EXCHANGE RATES (Foreign Exchange rates)

WHAT ARE FX RATES:
- Exchange rates between currencies (e.g., 1 USD = 0.92 EUR)
- Updated daily based on forex market
- Needed when user's account is in EUR but stock prices are in USD

EXAMPLE USE CASE:
- User has account in EUR (base_ccy = EUR in portfolios)
- User buys AAPL at $175 per share
- Need to convert: $175 × 0.92 = €161 per share for reporting

DATA STRUCTURE:
- base_ccy: Currency you have (USD, EUR, GBP, etc.)
- quote_ccy: Currency you want (EUR, JPY, etc.)
- rate: Conversion rate (1 base_ccy = X quote_ccy)
- ds: Date (FX rates change daily)

BUSINESS LOGIC:
- Stored in GCS as daily files: fx_rates/ds=2025-01-10/fx_rates.csv
- Contains all currency pairs: USD/EUR, EUR/USD, USD/JPY, etc.
- Skip weekends (forex market closed)

WHY BRONZE LAYER:
- Historical record of exchange rates
- Can time-travel to see "what was EUR/USD rate on Jan 10?"
- Audit trail for compliance
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


def read_gcs_fx_rates(spark, gcs_bucket, start_date, end_date):
    """
    Read FX rate files from GCS for a date range
    
    Files are partitioned by date: gs://bucket/reference/fx_rates/ds=2025-01-10/fx_rates.csv
    Each file contains ~100 currency pairs (10 currencies × 10 currencies)
    """
    print(f"Reading FX rates from GCS bucket: {gcs_bucket}")
    print(f"Date range: {start_date} to {end_date}")
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    paths = []
    current_dt = start_dt
    while current_dt <= end_dt:
        # Include all days (weekends too)
        path = f"gs://{gcs_bucket}/reference/fx_rates/ds={current_dt}/fx_rates.csv"
        paths.append(path)
        current_dt += timedelta(days=1)
    
    if not paths:
        print("No FX rate files found")
        return None
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(paths)
    
    print(f"Read {df.count()} FX rates")
    return df


def transform_bronze_fx_rates(df):
    """
    Add ingestion metadata and validate data types
    
    Ensures:
    - Currencies are strings (USD, EUR, etc.)
    - Rates are doubles (0.92, 149.5, etc.)
    - Dates are proper date type
    - No duplicate currency pairs on same date
    """
    print("Adding ingestion metadata")
    
    df = df \
        .withColumn("base_ccy", F.col("base_ccy").cast("string")) \
        .withColumn("quote_ccy", F.col("quote_ccy").cast("string")) \
        .withColumn("rate", F.col("rate").cast("double")) \
        .withColumn("ds", F.to_date(F.col("ds"))) \
        .withColumn("ingestion_ts", F.current_timestamp())
    
    # Remove duplicates (if any)
    df = df.dropDuplicates(["base_ccy", "quote_ccy", "ds"])
    
    return df


def main():
    """Main execution"""
    gcs_bucket = os.getenv("GCS_BUCKET", "fintech-trading-lakehouse-gds1")
    end_date = os.getenv("END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start_date = os.getenv("START_DATE", end_date)
    
    print(f"Configuration: Bucket={gcs_bucket}, Date Range={start_date} to {end_date}")
    
    spark = create_spark_session("Bronze_FX_Rates_Ingestion")
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze_v1")
        
        fx_df = read_gcs_fx_rates(spark, gcs_bucket, start_date, end_date)
        
        if fx_df is None or fx_df.count() == 0:
            print("No FX rates found")
            return
        
        bronze_df = transform_bronze_fx_rates(fx_df)
        
        print("Sample records:")
        bronze_df.select("base_ccy", "quote_ccy", "rate", "ds").show(10, truncate=False)
        
        bronze_df.write \
            .mode("overwrite") \
            .partitionBy("ds") \
            .saveAsTable("bronze_v1.br_fx_rates")
        
        print(f"Written {bronze_df.count()} records to bronze_v1.br_fx_rates")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
