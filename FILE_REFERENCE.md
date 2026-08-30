# File Reference Guide

## Quick Overview

**Total files**: 17
- 3 documentation files
- 3 data generators (run locally)
- 7 Spark jobs (run on Dataproc)
- 3 Airflow DAGs
- 3 Trino query files

---

## Documentation (Read First)

| File | Purpose |
|------|---------|
| `README.md` | Main setup guide and project overview |
| `DATA_UNDERSTANDING.md` | Explains what each data point means (READ THIS FIRST!) |
| `PROJECT_OVERVIEW.md` | Complete data flow and business questions |

---

## Data Generators (Run on Your Laptop)

| File | What It Creates | Where | Size |
|------|-----------------|-------|------|
| `data_generators/generate_mongodb_data.py` | 1,000 users<br>1,500 portfolios | MongoDB Atlas | ~3 MB |
| `data_generators/generate_bigquery_data.py` | 50,000 trades<br>2,000+ quotes | BigQuery | ~5 MB |
| `data_generators/generate_gcs_reference_data.py` | FX rates (90 days)<br>30 symbols | GCS | ~500 KB |

**Run order**: MongoDB → BigQuery → GCS (any order works)

---

## Spark Jobs (Submit to Dataproc)

### Bronze Layer (3 jobs - Raw Ingestion)

| File | Reads From | Writes To | Partitioned By |
|------|------------|-----------|----------------|
| `spark_jobs/bronze/ingest_trades_bronze.py` | BigQuery: trades_stream | bronze.br_trades | event_date |
| `spark_jobs/bronze/ingest_fx_bronze.py` | GCS: fx_rates/*.csv | bronze.br_fx_rates | ds |
| `spark_jobs/bronze/ingest_symbols_bronze.py` | GCS: symbols/*.csv | bronze.br_symbols | none |

### Silver Layer (1 job - Enrichment)

| File | Reads From | Writes To | What It Does |
|------|------------|-----------|--------------|
| `spark_jobs/silver/transform_silver.py` | bronze.br_trades<br>bronze.br_symbols | silver.trades_enriched | Joins trades with symbol metadata<br>Validates data<br>Calculates USD values |

### Gold Layer (3 jobs - Aggregation)

| File | Reads From | Writes To | Metrics Calculated |
|------|------------|-----------|-------------------|
| `spark_jobs/gold/aggregate_pnl.py` | silver.trades_enriched | gold.fact_pnl_daily | Daily profit/loss<br>Buy/sell volumes<br>Net positions |
| `spark_jobs/gold/aggregate_liquidity.py` | silver.trades_enriched | gold.fact_liquidity | Trading volume<br>Price volatility<br>Market turnover |
| `spark_jobs/gold/aggregate_user_activity.py` | silver.trades_enriched | gold.fact_user_activity | Trade frequency<br>Symbol diversity<br>Activity scores |

---

## Airflow DAGs (Upload to Composer)

| File | Runs | Dependencies |
|------|------|--------------|
| `airflow_dags/dag_ingest_bronze.py` | Hourly | Waits for GCS file sensor |
| `airflow_dags/dag_transform_silver.py` | 15 min after bronze | Waits for bronze DAG |
| `airflow_dags/dag_aggregate_gold.py` | 30 min after silver | Waits for silver DAG |

---

## Trino Queries (Run from Dashboard)

| File | Joins | Business Questions |
|------|-------|--------------------|
| `trino_queries/federated_user_analytics.sql` | MongoDB users + Iceberg gold | Top profitable users<br>Risk profile performance<br>Age group analysis |
| `trino_queries/federated_regional_analysis.sql` | MongoDB users + Iceberg gold | Regional volume<br>Regional profitability<br>Geographic preferences |
| `trino_queries/federated_risk_analysis.sql` | MongoDB users + Iceberg gold | Portfolio concentration<br>High-risk users<br>Loss-makers |

---

## Configuration

| File | Purpose |
|------|---------|
| `requirements.txt` | Python packages (8 packages for local data generation) |
| `.gitignore` | Files to exclude from git |
| `trino_setup/README_TRINO_SETUP.md` | Trino catalog configuration guide |

---

## Execution Order

### First Time Setup:
1. Install: `pip install -r requirements.txt`
2. Configure: Set environment variables
3. Generate data: Run 3 data generator scripts
4. Create cluster: `gcloud dataproc clusters create...`
5. Upload jobs: `gsutil cp -r spark_jobs/ gs://bucket/`
6. Run Bronze jobs (3)
7. Run Silver job (1)
8. Run Gold jobs (3)
9. Setup Trino catalogs
10. Run federated queries

### Regular Updates (Weekly):
1. Generate new data (local)
2. Create Dataproc cluster
3. Run all Spark jobs (or use Airflow)
4. Delete cluster
5. Query from dashboard

---

## Table Schema Quick Reference

### MongoDB Collections

**users**: user_id, name, email, region, risk_profile, kyc_status, metadata{age, income, ...}

**portfolios**: portfolio_id, user_id, base_ccy, holdings[{symbol, qty}], cash_balance, is_active

### Iceberg Tables

**bronze.br_trades**: trade_id, portfolio_id, symbol, side, price, qty, ts, venue, event_date

**bronze.br_fx_rates**: base_ccy, quote_ccy, rate, ds

**bronze.br_symbols**: symbol, exchange, sector, tick_size

**silver.trades_enriched**: (all bronze.br_trades columns) + exchange, sector, price_usd, notional_usd, is_valid, quality_score

**gold.fact_pnl_daily**: ds, portfolio_id, symbol, buy_qty, sell_qty, net_qty, avg_buy_price, avg_sell_price, realized_pnl_usd, notional_traded_usd, num_trades

**gold.fact_liquidity**: ds, symbol, exchange, sector, total_volume, total_turnover_usd, num_trades, unique_portfolios, avg_trade_size, price_volatility

**gold.fact_user_activity**: ds, portfolio_id, num_trades, unique_symbols, total_notional_usd, buy_sell_ratio, activity_score

---

## Join Keys for Federated Queries

```
users.user_id ←→ portfolios.user_id
portfolios.portfolio_id ←→ fact_pnl_daily.portfolio_id
portfolios.portfolio_id ←→ fact_user_activity.portfolio_id
```

This linking enables all federated analytics!

