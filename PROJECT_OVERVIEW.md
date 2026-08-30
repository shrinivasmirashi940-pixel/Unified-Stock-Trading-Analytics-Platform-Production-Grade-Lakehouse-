# Project Overview - Stock Trading Analytics

## What You're Building

A **real-world stock trading analytics platform** where you can analyze:
- User behavior (who trades what)
- Trading performance (who's profitable)
- Market activity (which stocks are hot)
- Risk patterns (who's taking big risks)

## Complete Data Flow

```
Step 1: GENERATE DATA (Run locally on your laptop)
────────────────────────────────────────────────────
generate_mongodb_data.py     → MongoDB Atlas
  ├─ 1,000 users (with demographics)
  └─ 1,500 portfolios (trading accounts)

generate_bigquery_data.py    → BigQuery
  ├─ 50,000 trades (buy/sell orders)
  └─ 2,000+ daily quotes (stock prices)

generate_gcs_reference_data.py → GCS
  ├─ FX rates (currency conversions)
  └─ Symbols (stock metadata)


Step 2: PROCESS DATA (PySpark on Dataproc)
────────────────────────────────────────────────────
BRONZE LAYER (Raw copies):
  ingest_trades_bronze.py    → bronze.br_trades
  ingest_fx_bronze.py        → bronze.br_fx_rates
  ingest_symbols_bronze.py   → bronze.br_symbols

SILVER LAYER (Enriched):
  transform_silver.py        → silver.trades_enriched
    - Joins trades with symbol metadata
    - Validates data quality
    - Calculates notional values

GOLD LAYER (Aggregated):
  aggregate_pnl.py           → gold.fact_pnl_daily
    - Daily profit/loss per portfolio
  
  aggregate_liquidity.py     → gold.fact_liquidity
    - Trading volume per stock
  
  aggregate_user_activity.py → gold.fact_user_activity
    - User engagement metrics


Step 3: QUERY DATA (Trino Federated Queries)
────────────────────────────────────────────────────
MongoDB (operational) + Iceberg (lakehouse) = Powerful Analytics

Example: "Top profitable users by region"
  SELECT u.region, SUM(f.realized_pnl_usd)
  FROM lakehouse.gold.fact_pnl_daily f      ← Iceberg
  JOIN operational.portfolios p              ← MongoDB
  JOIN operational.users u                   ← MongoDB
  GROUP BY u.region
```

## How Data Connects (The Magic!)

```
MongoDB:
  users
    ├─ user_id: USER_000001
    ├─ name: "John Smith"
    ├─ region: "US"
    └─ risk_profile: "aggressive"

  portfolios
    ├─ portfolio_id: PORT_000001  ←─┐
    ├─ user_id: USER_000001          │
    └─ base_ccy: "USD"               │
                                     │
BigQuery:                            │
  trades_stream                      │
    ├─ portfolio_id: PORT_000001  ←─┤  SAME ID!
    ├─ symbol: "AAPL"                │
    ├─ side: "BUY"                   │
    └─ price: 175.50                 │
                                     │
Iceberg Gold:                        │
  fact_pnl_daily                     │
    ├─ portfolio_id: PORT_000001  ←─┘
    ├─ realized_pnl_usd: 125.50
    └─ num_trades: 8
```

**Because portfolio_id is consistent**, you can join:
- User demographics (MongoDB) 
- Trading metrics (Iceberg)

## Real-World Business Questions

### User Analytics
1. "Which age group trades most?" → Join users.age + fact_user_activity
2. "Do high-income users trade more?" → Join users.income + fact_user_activity
3. "Which users are profitable?" → Join users + fact_pnl_daily

### Regional Analysis
1. "Which region generates most revenue?" → Join users.region + fact_pnl_daily
2. "Regional trading preferences?" → Join users.region + fact_liquidity
3. "Geographic profitability?" → Join users.region + fact_pnl_daily

### Risk Management
1. "Who has concentrated positions?" → Join fact_pnl_daily + portfolios + users
2. "Conservative users taking big risks?" → Join users.risk_profile + fact_pnl_daily
3. "Consistent loss-makers?" → Join users + fact_pnl_daily (negative PnL)

## Why Federated Queries Matter

**Without Federated Queries (Iceberg only):**
- You know portfolio PORT_000001 made $500 profit
- But you DON'T know: Who is this user? Where are they from? What's their age?

**With Federated Queries (MongoDB + Iceberg):**
- Portfolio PORT_000001 made $500 profit
- AND it's owned by John Smith, 35 years old, from US, aggressive trader
- Now you can segment: "US aggressive traders averaged $500 profit"

## Data Volumes

**MongoDB**: 
- 1,000 users (< 1 MB)
- 1,500 portfolios (< 2 MB)
- **Total**: ~3 MB (fits in free tier 512 MB)

**BigQuery**: 
- 50,000 trades (~5 MB)
- 2,000 quotes (~200 KB)
- **Total**: ~5 MB (well within 10 GB free tier)

**Iceberg (on GCS)**:
- Bronze: ~10 MB
- Silver: ~15 MB  
- Gold: ~5 MB
- **Total**: ~30 MB (well within 5 GB free tier)

## Tech Stack Summary

| Component | Technology | Purpose | Cost |
|-----------|------------|---------|------|
| Customer DB | MongoDB Atlas (Free) | User profiles, portfolios | $0 |
| Event Store | BigQuery (Free tier) | Trade history | $0 |
| Reference Files | GCS (Free tier) | FX rates, symbols | $0 |
| Processing | Dataproc | ETL pipeline | ~$2/month |
| Storage | Iceberg on GCS | Analytics tables | $0 |
| Query Engine | Trino (self-hosted) | Federated SQL | $0 |

**Total**: ~$2/month (from $300 GCP credit)

## Files You Need to Run

**Locally (your laptop):**
1. `data_generators/generate_mongodb_data.py`
2. `data_generators/generate_bigquery_data.py`
3. `data_generators/generate_gcs_reference_data.py`

**On Dataproc (submit jobs):**
1. All 7 files in `spark_jobs/` folder

**In your dashboard tool:**
1. SQL files from `trino_queries/` folder

That's it! Simple, logical, and production-ready.

