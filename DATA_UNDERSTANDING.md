# Data Understanding - Stock Trading Platform

## What This System Represents

Think of this as a **stock trading app** like Robinhood, Zerodha, or E*TRADE where people buy and sell stocks.

## Real-World Scenario

**Example Flow:**
1. John Smith signs up → Creates user account
2. John opens a trading account → Creates portfolio
3. John deposits $10,000 cash
4. John buys 50 shares of Apple (AAPL) at $175/share → Trade executed
5. Next day, John sells 20 shares of AAPL at $180/share → Another trade
6. John made profit: (180-175) × 20 = $100

## Data Sources Explained

### 1. MongoDB - Customer Database (Who)

**Collection: users**
```
Example User:
{
  user_id: "USER_000001",
  name: "John Smith",
  email: "john@example.com",
  region: "US",                    ← Where is this person from?
  risk_profile: "aggressive",      ← What's their investment style?
  kyc_status: "verified",          ← Are they verified to trade?
  metadata: {
    age: 35,                       ← Demographics
    annual_income_usd: 120000,     ← How much do they earn?
    investment_experience_years: 5  ← How experienced are they?
  }
}
```

**Real meaning**: Customer master data - who are your platform users

**Collection: portfolios**
```
Example Portfolio:
{
  portfolio_id: "PORT_000001",     ← Trading account ID
  user_id: "USER_000001",          ← Who owns this account
  base_ccy: "USD",                 ← Account currency
  holdings: [                       ← What stocks they currently own
    {symbol: "AAPL", qty: 50},
    {symbol: "GOOGL", qty: 30}
  ],
  cash_balance_usd: 5000,          ← Available cash
  is_active: true                   ← Is account active?
}
```

**Real meaning**: Trading accounts - one user can have multiple portfolios

### 2. BigQuery - Transaction History (What)

**Table: trades_stream**
```
Example Trade:
{
  trade_id: "TRD_20250110_00012345",
  portfolio_id: "PORT_000001",     ← Which account made this trade
  symbol: "AAPL",                  ← Which stock
  side: "BUY",                     ← Buying or selling?
  price: 175.50,                   ← Price per share
  qty: 50,                         ← Number of shares
  ts: "2025-01-10 14:30:25",      ← Exact time
  venue: "NASDAQ",                 ← Which exchange
  event_date: "2025-01-10"         ← Date (for partitioning)
}
```

**Real meaning**: Every buy/sell order - complete trading history

**Table: quotes_daily**
```
Example Quote:
{
  ds: "2025-01-10",               ← Date
  symbol: "AAPL",                 ← Stock
  open: 174.20,                   ← Opening price
  high: 177.50,                   ← Highest price of day
  low: 173.80,                    ← Lowest price of day
  close: 175.90,                  ← Closing price
  volume: 45000000                ← Total shares traded
}
```

**Real meaning**: Daily stock market prices

### 3. GCS - Reference Data (Supporting Info)

**File: fx_rates/ds=2025-01-10/fx_rates.csv**
```
base_ccy, quote_ccy, rate, ds
USD,      EUR,       0.92, 2025-01-10
USD,      JPY,       149.5, 2025-01-10
EUR,      USD,       1.087, 2025-01-10
```

**Real meaning**: Currency exchange rates - needed when user's account is in EUR but stock prices in USD

**File: symbols/symbols_master.csv**
```
symbol, exchange, sector,           tick_size
AAPL,   NASDAQ,   Technology,       0.01
JPM,    NYSE,     Financial Services, 0.01
PFE,    NYSE,     Healthcare,        0.01
```

**Real meaning**: Stock metadata - which exchange, what industry

## Data Pipeline Transformation

### Bronze Layer (Raw Storage)

**What it does**: Copies data exactly as-is from sources

**Tables created:**
- `bronze.br_trades` - Copy of BigQuery trades
- `bronze.br_fx_rates` - Copy of GCS FX rates
- `bronze.br_symbols` - Copy of GCS symbols

**Why**: Historical record, data audit trail

### Silver Layer (Cleaned & Enriched)

**What it does**: Joins and validates data

**Table created:**
- `silver.trades_enriched` - Trades + symbol info + validation

**Enrichment example:**
```
Original trade:
  symbol: "AAPL", price: 175.50, qty: 50

After enrichment:
  symbol: "AAPL", 
  price: 175.50, 
  qty: 50,
  exchange: "NASDAQ",      ← Added from symbols master
  sector: "Technology",    ← Added from symbols master
  price_usd: 175.50,       ← Converted to USD
  notional_usd: 8775,      ← Calculated: 175.50 × 50
  is_valid: true,          ← Quality check passed
  quality_score: 100       ← Data quality score
```

**Why**: Clean data ready for analytics

### Gold Layer (Business Metrics)

**Table 1: fact_pnl_daily** (Profit & Loss)

**What it calculates:**
```
Portfolio PORT_000001 traded AAPL on 2025-01-10:
  - Bought 50 shares at avg $175
  - Sold 20 shares at avg $180
  - Realized PnL: (180-175) × 20 = $100 profit
  - Net position: 50-20 = 30 shares still held
  - Total notional: (175×50) + (180×20) = $12,350 traded
```

**Columns:**
- `ds`: Date
- `portfolio_id`: Which account (links to MongoDB!)
- `symbol`: Which stock
- `buy_qty`: Total shares bought
- `sell_qty`: Total shares sold
- `net_qty`: Net position (buy - sell)
- `realized_pnl_usd`: Actual profit/loss
- `notional_traded_usd`: Total dollar volume
- `num_trades`: Number of transactions

**Real meaning**: Daily profit/loss scorecard per portfolio

**Table 2: fact_liquidity** (Market Activity)

**What it calculates:**
```
Symbol AAPL on 2025-01-10:
  - Total shares traded: 2,500,000
  - Total dollar volume: $438,750,000
  - Number of trades: 15,234
  - Unique traders: 4,567
  - Price volatility: $2.35
```

**Columns:**
- `ds`: Date
- `symbol`: Stock
- `exchange`, `sector`: From symbols master
- `total_volume`: Total shares traded
- `total_turnover_usd`: Total dollar value
- `num_trades`: Transaction count
- `unique_portfolios`: How many different accounts traded this
- `price_volatility`: Price swings

**Real meaning**: How actively is each stock being traded?

**Table 3: fact_user_activity** (User Engagement)

**What it calculates:**
```
Portfolio PORT_000001 on 2025-01-10:
  - Made 8 trades
  - Traded 4 different symbols
  - Total volume: $25,000
  - Buy/Sell ratio: 3.0 (more buying than selling)
  - Most traded symbol: AAPL
  - Activity score: 75 (out of 100)
```

**Columns:**
- `ds`: Date
- `portfolio_id`: Which account (links to MongoDB!)
- `num_trades`: How many trades
- `unique_symbols`: How many different stocks
- `total_notional_usd`: Total dollar volume
- `buy_sell_ratio`: Buying vs selling tendency
- `activity_score`: Engagement metric

**Real meaning**: How active is each user?

## How Federated Queries Work

### Example 1: Top Profitable Users by Region

**Question**: "Which country's users made the most money last week?"

**Data needed:**
- User region → MongoDB users
- User's portfolios → MongoDB portfolios
- Trading PnL → Iceberg gold.fact_pnl_daily

**Query:**
```sql
SELECT 
    u.region,                        -- From MongoDB
    SUM(f.realized_pnl_usd) as pnl  -- From Iceberg
FROM lakehouse.gold.fact_pnl_daily f
JOIN operational.fintech_trading.portfolios p 
    ON f.portfolio_id = p.portfolio_id    ← Join key!
JOIN operational.fintech_trading.users u 
    ON p.user_id = u.user_id
WHERE f.ds >= CURRENT_DATE - 7
GROUP BY u.region
ORDER BY pnl DESC;
```

**Answer example:**
```
region | pnl
-------|----------
US     | $2,450,000
EU     | $1,200,000
APAC   | $850,000
```

### Example 2: Do Aggressive Traders Perform Better?

**Question**: "Does risk_profile (conservative vs aggressive) correlate with profits?"

**Data needed:**
- Risk profile → MongoDB users
- Trading performance → Iceberg gold.fact_pnl_daily

**Query:**
```sql
SELECT 
    u.risk_profile,                  -- From MongoDB
    AVG(f.realized_pnl_usd) as avg_pnl  -- From Iceberg
FROM lakehouse.gold.fact_pnl_daily f
JOIN operational.fintech_trading.portfolios p 
    ON f.portfolio_id = p.portfolio_id
JOIN operational.fintech_trading.users u 
    ON p.user_id = u.user_id
GROUP BY u.risk_profile
ORDER BY avg_pnl DESC;
```

**Answer example:**
```
risk_profile     | avg_pnl
-----------------|--------
very_aggressive  | $125.50
aggressive       | $98.30
moderate         | $65.20
conservative     | $45.10
```

### Example 3: Young vs Old Traders

**Question**: "Do younger users trade more actively?"

**Data needed:**
- User age → MongoDB users.metadata
- Trading activity → Iceberg gold.fact_user_activity

**Query:**
```sql
SELECT 
    CASE 
        WHEN u.metadata.age < 30 THEN 'Young (18-29)'
        WHEN u.metadata.age < 50 THEN 'Middle (30-49)'
        ELSE 'Senior (50+)'
    END as age_group,
    AVG(a.num_trades) as avg_trades,
    AVG(a.activity_score) as avg_activity
FROM operational.fintech_trading.users u
JOIN operational.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
JOIN lakehouse.gold.fact_user_activity a 
    ON p.portfolio_id = a.portfolio_id
GROUP BY age_group;
```

**Answer example:**
```
age_group        | avg_trades | avg_activity
-----------------|------------|-------------
Young (18-29)    | 45.3       | 78
Middle (30-49)   | 32.1       | 65
Senior (50+)     | 18.5       | 42
```

## Why This Design Works

### Data Lifecycle:
1. **User signs up** → MongoDB (users table)
2. **User creates trading account** → MongoDB (portfolios table)
3. **User executes trades** → BigQuery (trades_stream)
4. **Spark processes trades** → Iceberg (Bronze → Silver → Gold)
5. **Business questions** → Trino (federated queries across MongoDB + Iceberg)

### Linking Keys:
- `user_id` links users to portfolios
- `portfolio_id` links portfolios to trading activity
- `symbol` links trades to market data
- `ds/event_date` filters time periods

### Separation of Concerns:
- **MongoDB**: Slow-changing data (users don't change region often)
- **BigQuery**: High-volume transactional data (millions of trades)
- **Iceberg**: Pre-aggregated analytics (faster queries)

## Sample Business Insights

With this setup, you can build dashboards showing:

1. **Executive Dashboard**
   - Total users by region
   - Total trading volume by region
   - Top profitable regions

2. **User Segmentation Dashboard**
   - Trading activity by age group
   - Performance by income bracket
   - Risk profile distribution

3. **Risk Management Dashboard**
   - Users with concentrated positions
   - Conservative users taking big risks
   - Consistent loss-makers needing intervention

4. **Regional Performance**
   - Which regions trade which stocks
   - Regional profitability comparison
   - Market share by geography

All powered by joining MongoDB (who the users are) with Iceberg (what they're doing)!

