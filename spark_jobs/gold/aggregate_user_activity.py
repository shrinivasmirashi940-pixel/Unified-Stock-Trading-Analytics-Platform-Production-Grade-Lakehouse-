"""
Gold Layer: Aggregate Daily User Activity Metrics

DATA DOMAIN EXPLANATION:
========================
This job calculates USER ENGAGEMENT for each portfolio

WHAT IS USER ACTIVITY:
- How actively each portfolio is trading
- Used to identify power users vs inactive users
- Helps in user retention and engagement strategies

METRICS CALCULATED:
1. num_trades: How many trades this portfolio made today
2. unique_symbols: How many different stocks traded (diversification)
3. total_notional_usd: Total dollar volume traded
4. buy_sell_ratio: Ratio of buys to sells (>1 = more buying, <1 = more selling)
5. activity_score: Engagement score 0-100 (higher = more active)

BUSINESS VALUE:
- Identify most active traders (for VIP treatment)
- Detect inactive users (for re-engagement campaigns)
- Measure platform engagement trends
- Predict user churn (declining activity scores)

EXAMPLE OUTPUT:
Portfolio PORT_000001 on 2025-01-10:
  - num_trades: 12 (made 12 buy/sell orders)
  - unique_symbols: 5 (traded 5 different stocks - diversified)
  - total_notional_usd: $35,000 (total trading volume)
  - buy_sell_ratio: 2.0 (8 buys, 4 sells - net buying)
  - activity_score: 85 (highly engaged user)

FEDERATED QUERY USE:
- portfolio_id links to MongoDB
- Can answer: "Are younger users more active?"
- Can answer: "Which region has most engaged users?"
"""

import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
    
    Includes:
    - portfolio_id: WHO is trading (links to MongoDB)
    - symbol: WHAT they're trading
    - side: BUY or SELL (to calculate buy/sell ratio)
    - notional_usd: Trade size (for volume calculation)
    """
    print(f"Reading enriched trades from Silver, date range: {start_date} to {end_date}")
    
    df = spark.sql(f"""
        SELECT event_date as ds, portfolio_id, symbol, side, notional_usd, is_valid
        FROM silver_v1.trades_enriched
        WHERE event_date >= '{start_date}' AND event_date <= '{end_date}' AND is_valid = true
    """)
    
    print(f"Read {df.count()} valid trades")
    return df


def calculate_user_activity_metrics(trades_df):
    """
    Aggregate activity per portfolio per day
    
    CALCULATION LOGIC:
    1. Group by portfolio and date
    2. Count total trades
    3. Count unique symbols (shows diversification)
    4. Sum total dollar volume
    5. Calculate buy/sell ratio (trading direction bias)
    6. Assign activity score based on engagement level
    
    ACTIVITY SCORE INTERPRETATION:
    - 80-100: Power user (very active)
    - 60-79: Active trader
    - 40-59: Moderate activity
    - 20-39: Low activity
    - 0-19: Barely active (churn risk)
    """
    print("Calculating user activity metrics")
    
    # Basic aggregations by portfolio and date
    activity_df = trades_df.groupBy("ds", "portfolio_id").agg(
        F.count("*").cast("int").alias("num_trades"),
        F.countDistinct("symbol").cast("int").alias("unique_symbols"),
        F.sum("notional_usd").alias("total_notional_usd"),
        (F.sum(F.when(F.col("side") == "BUY", 1).otherwise(0)).cast("double") / 
         F.sum(F.when(F.col("side") == "SELL", 1).otherwise(0)).cast("double")).alias("buy_sell_ratio"),
        F.first(F.col("symbol")).alias("most_traded_symbol")
    )
    
    # Find most traded symbol using window function
    symbol_counts = trades_df.groupBy("ds", "portfolio_id", "symbol").agg(
        F.count("*").alias("symbol_trade_count")
    )
    
    window_spec = Window.partitionBy("ds", "portfolio_id").orderBy(F.desc("symbol_trade_count"))
    most_traded = symbol_counts.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .select("ds", "portfolio_id", F.col("symbol").alias("most_traded_symbol"))
    
    activity_df = activity_df.drop("most_traded_symbol").join(most_traded, on=["ds", "portfolio_id"], how="left")
    
    # Handle null buy_sell_ratio (when no sells, ratio = infinity)
    # Use coalesce to handle both null and NaN values
    activity_df = activity_df.withColumn(
        "buy_sell_ratio",
        F.coalesce(F.col("buy_sell_ratio"), F.lit(999.0))
    )
    
    # Simple activity score (in real system, would be more sophisticated)
    # For now, based on num_trades: 10+ trades = score 80, 5-9 = 60, etc.
    activity_df = activity_df.withColumn(
        "activity_score",
        F.when(F.col("num_trades") >= 10, 80)
        .when(F.col("num_trades") >= 5, 60)
        .when(F.col("num_trades") >= 2, 40)
        .otherwise(20)
        .cast("int")
    )
    
    return activity_df


def main():
    """Main execution"""
    end_date = os.getenv("END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start_date = os.getenv("START_DATE", end_date)
    
    print(f"Configuration: Date Range={start_date} to {end_date}")
    
    spark = create_spark_session("Gold_User_Activity_Aggregation")
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS gold_v1")
        
        trades_df = read_silver_trades(spark, start_date, end_date)
        
        if trades_df.count() == 0:
            print("No valid trades found in Silver")
            return
        
        activity_df = calculate_user_activity_metrics(trades_df)
        
        print("Sample activity records (most active):")
        activity_df.orderBy(F.desc("activity_score")).show(10, truncate=False)
        
        # Write to Gold layer
        activity_df.write \
            .mode("overwrite") \
            .partitionBy("ds") \
            .saveAsTable("gold_v1.fact_user_activity")
        
        print(f"Written {activity_df.count()} records to gold_v1.fact_user_activity")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
