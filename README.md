# Unified-Stock-Trading-Analytics-Platform-Production-Grade-Lakehouse

# Stock Trading Analytics - Federated Data Lakehouse

Fintech data lakehouse with federated queries across MongoDB (users) and Iceberg (trading metrics).

## Architecture

```
Data Sources              Processing           Analytics             Query Layer
─────────────────        ─────────────────    ─────────────────     ─────────────────
MongoDB                                        
├─ users              ─┐                                             Trino Federated
└─ portfolios         ─┼──────────────────────────────────────────► Queries
                       │                                             (Join MongoDB
BigQuery               │                                              + Iceberg)
└─ trades ────────────►│  PySpark/Dataproc   Silver/Gold
                       │  (No Bronze copy)   (Iceberg/Hive)
GCS                    │
├─ fx_rates ───────────┼─► Bronze ──────────► 
└─ symbols ────────────┘
```

## Key Design: Portfolio ID Linking

```
MongoDB portfolios.portfolio_id = "PORT_000001"
                                      ↓
BigQuery trades.portfolio_id = "PORT_000001" 
                                      ↓
Silver trades_enriched.portfolio_id = "PORT_000001"
                                      ↓
Gold fact_pnl_daily.portfolio_id = "PORT_000001"
Gold fact_user_activity.portfolio_id = "PORT_000001"
```

This enables federated queries like:
```sql
SELECT u.region, SUM(f.realized_pnl_usd)
FROM lakehouse.gold.fact_pnl_daily f
JOIN operational.portfolios p ON f.portfolio_id = p.portfolio_id
JOIN operational.users u ON p.user_id = u.user_id
GROUP BY u.region;
```

## Project Structure

```
Trino Class 2/
├── README.md
├── requirements.txt
├── data_generators/          # Run locally on your laptop
│   ├── generate_mongodb_data.py      # 1K users, 1.5K portfolios
│   ├── generate_bigquery_data.py     # 50K trades (with PORT_IDs)
│   └── generate_gcs_reference_data.py # FX rates, symbols
├── spark_jobs/               # Submit to Dataproc
│   ├── bronze/
│   │   ├── ingest_fx_bronze.py       # GCS → Iceberg
│   │   └── ingest_symbols_bronze.py  # GCS → Iceberg
│   ├── silver/
│   │   └── transform_silver.py       # BigQuery → Iceberg (reads trades directly!)
│   └── gold/
│       ├── aggregate_pnl.py          # Daily PnL per portfolio
│       ├── aggregate_liquidity.py    # Volume per symbol
│       └── aggregate_user_activity.py # Engagement per portfolio
├── airflow_dags/             # Upload to Composer
│   ├── dag_ingest_bronze.py          # FX + Symbols (NO trades!)
│   ├── dag_transform_silver.py
│   └── dag_aggregate_gold.py
├── trino_setup/              # Trino catalog configs
│   └── README_TRINO_SETUP.md
└── trino_queries/            # Federated analytics queries
    ├── federated_user_analytics.sql
    ├── federated_regional_analysis.sql
    └── federated_risk_analysis.sql
```

## Setup Steps

### 1. Generate Mock Data Locally

```bash
pip install -r requirements.txt

export GCP_PROJECT_ID="your-project-id"
export MONGODB_URI="mongodb+srv://user:pass@cluster.mongodb.net/"
export GCS_BUCKET="your-bucket-name"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

python data_generators/generate_mongodb_data.py
python data_generators/generate_bigquery_data.py
python data_generators/generate_gcs_reference_data.py
```

### 2. Create Dataproc Cluster

```bash
gcloud dataproc clusters create fintech-cluster \
    --region=us-central1 \
    --master-machine-type=n1-standard-2 \
    --num-workers=2 \
    --worker-machine-type=n1-standard-2 \
    --image-version=2.1-debian11 \
    --enable-component-gateway \
    --properties=hive:hive.metastore.warehouse.dir=gs://YOUR_BUCKET/warehouse,spark:spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2
```

### 3. Run Spark Jobs

```bash
gsutil -m cp -r spark_jobs/ gs://$GCS_BUCKET/

# Bronze (only reference data)
gcloud dataproc jobs submit pyspark --cluster=fintech-cluster --region=us-central1 \
    gs://$GCS_BUCKET/spark_jobs/bronze/ingest_fx_bronze.py
gcloud dataproc jobs submit pyspark --cluster=fintech-cluster --region=us-central1 \
    gs://$GCS_BUCKET/spark_jobs/bronze/ingest_symbols_bronze.py

# Silver (reads trades directly from BigQuery)
gcloud dataproc jobs submit pyspark --cluster=fintech-cluster --region=us-central1 \
    gs://$GCS_BUCKET/spark_jobs/silver/transform_silver.py

# Gold
gcloud dataproc jobs submit pyspark --cluster=fintech-cluster --region=us-central1 \
    gs://$GCS_BUCKET/spark_jobs/gold/aggregate_pnl.py
gcloud dataproc jobs submit pyspark --cluster=fintech-cluster --region=us-central1 \
    gs://$GCS_BUCKET/spark_jobs/gold/aggregate_liquidity.py
gcloud dataproc jobs submit pyspark --cluster=fintech-cluster --region=us-central1 \
    gs://$GCS_BUCKET/spark_jobs/gold/aggregate_user_activity.py
```

### 4. Setup Trino Catalogs

See `trino_setup/README_TRINO_SETUP.md` for detailed instructions.

**Quick setup:**
- Iceberg catalog: Points to Dataproc Hive metastore
- MongoDB catalog: Points to MongoDB Atlas

### 5. Run Federated Queries

Use SQL files in `trino_queries/`:
- User demographics + trading performance
- Regional profitability analysis
- Risk assessment queries

## Data Model

### MongoDB (Operational)
- **users**: user_id, name, region, risk_profile, age, income
- **portfolios**: portfolio_id, user_id, holdings, cash_balance

### BigQuery (Raw Transactions)
- **trades_stream**: trade_id, portfolio_id, symbol, side, price, qty, ts

### Iceberg Bronze (Reference Data)
- **bronze.br_fx_rates**: Currency exchange rates
- **bronze.br_symbols**: Stock metadata

### Iceberg Silver (Enriched)
- **silver.trades_enriched**: Validated trades with exchange, sector, quality_score

### Iceberg Gold (Analytics)
- **gold.fact_pnl_daily**: portfolio_id, symbol, realized_pnl_usd, ...
- **gold.fact_liquidity**: symbol, total_volume, price_volatility, ...
- **gold.fact_user_activity**: portfolio_id, num_trades, activity_score, ...

## Federated Query Examples

**Top profitable users by region:**
```sql
SELECT u.region, SUM(f.realized_pnl_usd) as total_pnl
FROM lakehouse.gold.fact_pnl_daily f
JOIN operational.fintech_trading.portfolios p ON f.portfolio_id = p.portfolio_id
JOIN operational.fintech_trading.users u ON p.user_id = u.user_id
GROUP BY u.region;
```

**Trading activity by age group:**
```sql
SELECT 
    CASE WHEN u.metadata.age < 30 THEN 'Young' ELSE 'Senior' END as age_group,
    AVG(a.activity_score)
FROM lakehouse.gold.fact_user_activity a
JOIN operational.fintech_trading.portfolios p ON a.portfolio_id = p.portfolio_id
JOIN operational.fintech_trading.users u ON p.user_id = u.user_id
GROUP BY age_group;
```

## Documentation

- **DATA_UNDERSTANDING.md**: Explains each data point and business context
- **PROJECT_OVERVIEW.md**: Complete data flow and use cases
- **FILE_REFERENCE.md**: Quick reference for all files
- **trino_setup/README_TRINO_SETUP.md**: Trino configuration guide

## Notes

- Trades are NOT copied to Bronze - read directly from BigQuery for efficiency
- All code files have inline domain explanations
- portfolio_id is the key that enables MongoDB + Iceberg joins
- Designed for GCP free tier (~$2-5/month)
