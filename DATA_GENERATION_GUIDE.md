# Data Generation Guide

## What Gets Generated When You Run the Scripts

### 1. MongoDB Data (generate_mongodb_data.py)

**Run frequency**: ONCE (during initial setup)

**What it creates**:
- 1,000 users (customer profiles)
- 1,500 portfolios (trading accounts)

**Data timeframe**: 
- Users created over last 2 years (random signup dates)
- Portfolios created after user signup

**Example**:
```
User created: 2023-05-15
Portfolio created: 2023-06-20 (after user signup)
```

**Size**: ~3 MB in MongoDB

---

### 2. BigQuery Data (generate_bigquery_data.py)

**Run frequency**: ONCE (during initial setup)

**What it creates**:

#### trades_stream
- 50,000 trades (buy/sell orders)
- Spread over LAST 90 DAYS
- Random timestamps between now and 90 days ago

**Example**:
```
Today: 2025-01-10
Trades generated from: 2024-10-12 to 2025-01-10 (90 days)

Trade 1: 2024-10-15 14:30:25 - BUY 50 AAPL @ $175
Trade 2: 2024-11-03 09:15:42 - SELL 30 GOOGL @ $142
...
Trade 50000: 2025-01-09 16:45:10 - BUY 100 TSLA @ $245
```

#### quotes_daily
- Daily stock prices for all 30 symbols
- For last 90 DAYS (weekdays only, ~60 days)
- Open, High, Low, Close prices

**Example**:
```
AAPL on 2024-10-15: open=$174, high=$177, low=$173, close=$175
AAPL on 2024-10-16: open=$175, high=$178, low=$174, close=$176
...
AAPL on 2025-01-10: open=$195, high=$198, low=$194, close=$196
```

**Size**: ~5 MB in BigQuery

---

### 3. GCS Data (generate_gcs_reference_data.py)

**Run frequency**: ONCE (during initial setup)

**What it creates**:

#### FX Rates
- 90 DAYS of historical currency exchange rates
- One file per day (weekdays only)
- ~60 files total

**File structure**:
```
reference/fx_rates/
  ├── ds=2024-10-14/fx_rates.csv  (100 rows: 10 currencies × 10 currencies)
  ├── ds=2024-10-15/fx_rates.csv  (100 rows)
  ├── ds=2024-10-16/fx_rates.csv  (100 rows)
  ...
  └── ds=2025-01-10/fx_rates.csv  (100 rows)

Total: ~60 files × 100 rows = 6,000 rows
```

**Example content** (fx_rates.csv for 2025-01-10):
```csv
base_ccy,quote_ccy,rate,ds
USD,EUR,0.92,2025-01-10
USD,JPY,149.5,2025-01-10
EUR,USD,1.087,2025-01-10
...
```

#### Symbols
- ONE static file
- 30 stocks

**File structure**:
```
reference/symbols/symbols_master.csv  (30 rows)
```

**Content**:
```csv
symbol,exchange,sector,tick_size
AAPL,NASDAQ,Technology,0.01
GOOGL,NASDAQ,Technology,0.01
JPM,NYSE,Financial Services,0.01
...
```

**Size**: ~500 KB in GCS

---

## Summary

| Data Source | Run Frequency | Time Period | Volume |
|-------------|---------------|-------------|--------|
| **MongoDB** | ONCE | Users over 2 years, Portfolios active | 1K users, 1.5K portfolios |
| **BigQuery Trades** | ONCE | Last 90 days | 50K trades |
| **BigQuery Quotes** | ONCE | Last 90 days (weekdays) | ~2K quotes |
| **GCS FX Rates** | ONCE | Last 90 days (weekdays) | ~6K rates (60 files) |
| **GCS Symbols** | ONCE | Static | 30 symbols (1 file) |

---

## In Production vs This Project

### This Project (Learning/Demo):
- Run all generators ONCE
- Creates 90 days of historical data
- Enough to test all pipelines and queries
- Total data: ~10 MB across all sources

### Production System:
- **Daily**: Run BigQuery + GCS FX generators for just today
- **Weekly**: Refresh symbols if new stocks added
- **Continuous**: MongoDB updated by application (users signup, portfolios created)
- **Incremental**: Process only new data each day

---

## When to Re-run Generators

**Re-run if**:
- You want fresh data (new random trades)
- Testing different scenarios
- Deleted your databases and starting over

**No need to re-run if**:
- Just testing queries (existing data is fine)
- Making code changes (data doesn't need refresh)
- Learning the pipeline flow

---

## Data Timeline Visualization

```
Time: ←──────────────────────────────────────────────→
      90 days ago                           Today

Users: [Created randomly over 2 years]─────────────────►
Portfolios: [Created after user signup]────────────────►
Trades: [──────── 50,000 trades spread across 90 days ──────]
Quotes: [──────── Daily prices for 90 days ──────────────]
FX Rates: [──────── Daily rates for 90 days ───────────────]
Symbols: [Static - no time dimension]
```

When you run the pipeline, it processes all this historical data and creates:
- Bronze: Historical FX rates + symbols
- Silver: All 50K trades enriched
- Gold: Daily aggregations for each of the 90 days

This gives you a complete 90-day dataset to query and analyze!

