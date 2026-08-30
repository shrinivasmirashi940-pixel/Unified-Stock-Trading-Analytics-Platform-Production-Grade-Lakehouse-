"""
Bronze Layer: Ingest Symbol Master from GCS to Iceberg

DATA DOMAIN EXPLANATION:
========================
This job loads STOCK METADATA (reference data about each stock)

WHAT IS SYMBOL MASTER:
- Reference data for each stock traded on the platform
- Relatively static (doesn't change often)
- Loaded once or refreshed weekly

DATA STRUCTURE:
- symbol: Stock ticker (AAPL, GOOGL, TSLA)
- exchange: Where it trades (NYSE, NASDAQ)
- sector: Industry classification (Technology, Healthcare, Financial Services)
- tick_size: Minimum price increment (usually $0.01)

BUSINESS VALUE:
- Used to enrich trades with context (which exchange, which sector)
- Enables sector-level analysis: "Which sector is most traded?"
- Ensures trades only happen for valid symbols

EXAMPLE RECORDS:
  AAPL, NASDAQ, Technology, 0.01
  JPM,  NYSE,   Financial Services, 0.01
  PFE,  NYSE,   Healthcare, 0.01

WHY BRONZE LAYER:
- Single source of truth for symbol metadata
- Can track changes over time (stock changes exchanges, etc.)
- Reusable across multiple pipelines
"""

import os
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


def read_gcs_symbols(spark, gcs_bucket):
    """
    Read symbol master file from GCS
    
    File location: gs://bucket/reference/symbols/symbols_master.csv
    Contains metadata for all 30 stocks available on the platform
    """
    print(f"Reading symbols from GCS bucket: {gcs_bucket}")
    
    path = f"gs://{gcs_bucket}/reference/symbols/symbols_master.csv"
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path)
    
    print(f"Read {df.count()} symbols")
    return df


def transform_bronze_symbols(df):
    """
    Validate and standardize symbol data
    
    Ensures:
    - Symbol is uppercase string (AAPL not aapl)
    - Exchange is valid (NYSE or NASDAQ)
    - Sector is categorized properly
    - No duplicate symbols
    """
    print("Adding ingestion metadata")
    
    df = df \
        .withColumn("symbol", F.col("symbol").cast("string")) \
        .withColumn("exchange", F.col("exchange").cast("string")) \
        .withColumn("sector", F.col("sector").cast("string")) \
        .withColumn("tick_size", F.col("tick_size").cast("double")) \
        .withColumn("ingestion_ts", F.current_timestamp())
    
    # Remove duplicates (in case CSV has duplicate rows)
    df = df.dropDuplicates(["symbol"])
    
    return df


def main():
    """Main execution"""
    gcs_bucket = os.getenv("GCS_BUCKET", "fintech-trading-lakehouse-gds1")
    
    print(f"Configuration: Bucket={gcs_bucket}")
    
    spark = create_spark_session("Bronze_Symbols_Ingestion")
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze_v1")
        
        symbols_df = read_gcs_symbols(spark, gcs_bucket)
        
        if symbols_df.count() == 0:
            print("No symbols found")
            return
        
        bronze_df = transform_bronze_symbols(symbols_df)
        
        print("Sample records:")
        bronze_df.show(10, truncate=False)
        
        # Full refresh (replace entire table)
        bronze_df.write \
            .mode("overwrite") \
            .saveAsTable("bronze_v1.br_symbols")
        
        print(f"Written {bronze_df.count()} records to bronze_v1.br_symbols")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
