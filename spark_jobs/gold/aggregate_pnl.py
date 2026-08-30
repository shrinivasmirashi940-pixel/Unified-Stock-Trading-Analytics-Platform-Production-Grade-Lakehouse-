"""
Gold Layer: Aggregate Daily PnL by Portfolio and Symbol

DATA DOMAIN EXPLANATION:
========================
This job calculates PROFIT & LOSS (PnL) for each portfolio

WHAT IS PnL:
- Profit or Loss from buying and selling stocks
- Example: Buy 100 shares at $150, sell 100 shares at $160 = $10/share profit = $1000 total PnL

INPUT:
- silver.trades_enriched: All validated buy/sell trades

OUTPUT:
- gold.fact_pnl_daily: Daily PnL summary per portfolio per symbol

METRICS CALCULATED:
1. buy_qty: Total shares bought (e.g., 100 shares)
2. sell_qty: Total shares sold (e.g., 50 shares)
3. net_qty: Net position = buy - sell (e.g., 100-50 = 50 shares still held)
4. avg_buy_price: Average price paid when buying (e.g., $150)
5. avg_sell_price: Average price received when selling (e.g., $160)
6. realized_pnl_usd: Actual profit/loss = (sell_price - buy_price) × min(buy_qty, sell_qty)
   Example: (160-150) × 50 = $500 profit
7. notional_traded_usd: Total dollar volume = sum of all buy+sell dollar amounts
8. num_trades: Count of transactions

BUSINESS VALUE:
- Track which portfolios are profitable
- Identify winning vs losing traders
- Calculate total platform revenue (from trading fees)

FEDERATED QUERY USE:
- portfolio_id links to MongoDB to see WHO made this profit
- Can answer: "Which region's users are most profitable?"
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
    Read validated trades from Silver layer
    
    Only reads trades that passed quality validation (is_valid = true)
    This ensures PnL calculations are based on good data
    """
    print(f"Reading enriched trades from Silver, date range: {start_date} to {end_date}")
    
    df = spark.sql(f"""
        SELECT event_date as ds, portfolio_id, symbol, side, price_usd, qty, notional_usd, is_valid
        FROM silver_v1.trades_enriched
        WHERE event_date >= '{start_date}' AND event_date <= '{end_date}' AND is_valid = true
    """)
    
    print(f"Read {df.count()} valid trades")
    return df


def calculate_daily_pnl(trades_df):
    """
    Calculate PnL metrics per portfolio per symbol per day
    
    CALCULATION LOGIC:
    1. Separate BUY and SELL trades
    2. For each portfolio-symbol-date:
       - Sum all buy quantities and calculate average buy price
       - Sum all sell quantities and calculate average sell price
    3. Realized PnL = (avg_sell_price - avg_buy_price) × shares_sold
       - Only count profit/loss on shares actually sold
       - Shares still held are "unrealized" (not counted here)
    
    EXAMPLE:
    Portfolio PORT_000001, Symbol AAPL, Date 2025-01-10:
      Buys: 100 shares @ $150, 50 shares @ $160 → avg buy price = $153.33, total 150 shares
      Sells: 80 shares @ $170 → avg sell price = $170, total 80 shares
      Realized PnL: (170 - 153.33) × 80 = $1,333.60 profit
      Net position: 150 - 80 = 70 shares still held
    """
    print("Calculating daily PnL metrics")
    
    # Separate buy and sell trades
    buy_trades = trades_df.filter(F.col("side") == "BUY")
    sell_trades = trades_df.filter(F.col("side") == "SELL")
    
    # Aggregate buy metrics
    buy_agg = buy_trades.groupBy("ds", "portfolio_id", "symbol").agg(
        F.sum("qty").alias("buy_qty"),
        F.avg("price_usd").alias("avg_buy_price"),
        F.sum("notional_usd").alias("buy_notional"),
        F.count("*").alias("buy_count")
    )
    
    # Aggregate sell metrics
    sell_agg = sell_trades.groupBy("ds", "portfolio_id", "symbol").agg(
        F.sum("qty").alias("sell_qty"),
        F.avg("price_usd").alias("avg_sell_price"),
        F.sum("notional_usd").alias("sell_notional"),
        F.count("*").alias("sell_count")
    )
    
    # Full outer join (include portfolios that only bought OR only sold)
    pnl_df = buy_agg.join(sell_agg, on=["ds", "portfolio_id", "symbol"], how="outer")
    
    # Fill nulls with zeros
    pnl_df = pnl_df \
        .fillna(0, subset=["buy_qty", "sell_qty", "buy_notional", "sell_notional", "buy_count", "sell_count"]) \
        .fillna(0.0, subset=["avg_buy_price", "avg_sell_price"])
    
    # Calculate net position (shares still held)
    pnl_df = pnl_df.withColumn("net_qty", F.col("buy_qty") - F.col("sell_qty"))
    
    # Calculate realized PnL
    # Formula: (avg_sell_price - avg_buy_price) × min(buy_qty, sell_qty)
    # We use min() because you can only realize profit on shares you actually sold
    pnl_df = pnl_df.withColumn(
        "realized_pnl_usd",
        F.when(
            (F.col("buy_qty") > 0) & (F.col("sell_qty") > 0),
            (F.col("avg_sell_price") - F.col("avg_buy_price")) * F.least(F.col("buy_qty"), F.col("sell_qty"))
        ).otherwise(0.0)
    )
    
    # Total notional traded (buy + sell volume)
    pnl_df = pnl_df.withColumn("notional_traded_usd", F.col("buy_notional") + F.col("sell_notional"))
    
    # Total number of trades
    pnl_df = pnl_df.withColumn("num_trades", (F.col("buy_count") + F.col("sell_count")).cast("int"))
    
    return pnl_df.select(
        "ds", "portfolio_id", "symbol",
        F.col("buy_qty").cast("int").alias("buy_qty"),
        F.col("sell_qty").cast("int").alias("sell_qty"),
        F.col("net_qty").cast("int").alias("net_qty"),
        "avg_buy_price", "avg_sell_price",
        "realized_pnl_usd", "notional_traded_usd", "num_trades"
    )


def main():
    """Main execution"""
    end_date = os.getenv("END_DATE", (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y-%m-%d"))
    start_date = os.getenv("START_DATE", end_date)
    
    print(f"Configuration: Date Range={start_date} to {end_date}")
    
    spark = create_spark_session("Gold_PnL_Aggregation")
    
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS gold_v1")
        
        trades_df = read_silver_trades(spark, start_date, end_date)
        
        if trades_df.count() == 0:
            print("No valid trades found in Silver")
            return
        
        pnl_df = calculate_daily_pnl(trades_df)
        
        print("Sample PnL records (top profits):")
        pnl_df.orderBy(F.desc("realized_pnl_usd")).show(10, truncate=False)
        
        # Write to Gold layer (partitioned by date for efficient queries)
        pnl_df.write \
            .mode("overwrite") \
            .partitionBy("ds") \
            .saveAsTable("gold_v1.fact_pnl_daily")
        
        print(f"Written {pnl_df.count()} records to gold_v1.fact_pnl_daily")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
