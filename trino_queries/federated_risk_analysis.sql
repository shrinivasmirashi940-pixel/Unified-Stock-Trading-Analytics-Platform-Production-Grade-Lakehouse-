-- Federated Query: Portfolio Risk Analysis
-- Identifies risky trading patterns and concentration issues

-- ============================================================================
-- 1. PORTFOLIO CONCENTRATION RISK
-- Question: Which users have too much exposure in a single stock?
-- ============================================================================

WITH portfolio_concentration AS (
    SELECT 
        f.portfolio_id,
        f.symbol,
        SUM(f.notional_traded_usd) as symbol_notional,
        SUM(SUM(f.notional_traded_usd)) OVER (PARTITION BY f.portfolio_id) as total_notional
    FROM hive.gold_v1.fact_pnl_daily f
    WHERE f.ds >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY f.portfolio_id, f.symbol
)
SELECT 
    u.user_id,
    u.name,
    u.region,
    u.risk_profile,
    pc.portfolio_id,
    pc.symbol,
    ROUND(pc.symbol_notional, 2) as symbol_volume_usd,
    ROUND(pc.total_notional, 2) as portfolio_total_usd,
    ROUND(pc.symbol_notional / pc.total_notional * 100, 2) as concentration_pct,
    CASE 
        WHEN pc.symbol_notional / pc.total_notional > 0.50 THEN 'CRITICAL'
        WHEN pc.symbol_notional / pc.total_notional > 0.30 THEN 'HIGH'
        ELSE 'MODERATE'
    END as risk_level
FROM portfolio_concentration pc
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON pc.portfolio_id = p.portfolio_id
JOIN mongodb_dev.fintech_trading.users u 
    ON p.user_id = u.user_id
WHERE pc.symbol_notional / pc.total_notional > 0.30  -- Over 30% in one stock
  AND u.kyc_status = 'verified'
ORDER BY concentration_pct DESC
LIMIT 50;


-- ============================================================================
-- 2. HIGH-RISK USERS WITH LARGE POSITIONS
-- Question: Which aggressive traders have the largest positions?
-- ============================================================================

SELECT 
    u.user_id,
    u.name,
    u.risk_profile,
    COUNT(DISTINCT p.portfolio_id) as num_portfolios,
    SUM(f.notional_traded_usd) as total_volume_30d,
    COUNT(DISTINCT f.symbol) as unique_symbols,
    ROUND(AVG(ABS(f.realized_pnl_usd)), 2) as avg_absolute_pnl
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_pnl_daily f 
    ON p.portfolio_id = f.portfolio_id
    AND f.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.risk_profile IN ('very_aggressive', 'aggressive')
  AND u.kyc_status = 'verified'
GROUP BY u.user_id, u.name, u.risk_profile
HAVING SUM(f.notional_traded_usd) > 100000  -- Over $100K traded
ORDER BY total_volume_30d DESC
LIMIT 30;


-- ============================================================================
-- 3. LOSS-MAKING USERS
-- Question: Which users consistently lose money?
-- ============================================================================

SELECT 
    u.user_id,
    u.name,
    u.region,
    u.risk_profile,
    COUNT(DISTINCT f.ds) as trading_days,
    SUM(f.num_trades) as total_trades,
    ROUND(SUM(f.realized_pnl_usd), 2) as total_pnl_usd,
    ROUND(AVG(f.realized_pnl_usd), 2) as avg_pnl_per_day
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
JOIN hive.gold_v1.fact_pnl_daily f 
    ON p.portfolio_id = f.portfolio_id
    AND f.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.kyc_status = 'verified'
GROUP BY u.user_id, u.name, u.region, u.risk_profile
HAVING SUM(f.realized_pnl_usd) < -1000  -- Lost more than $1000
   AND SUM(f.num_trades) > 10  -- Active traders
ORDER BY total_pnl_usd ASC
LIMIT 30;


-- ============================================================================
-- 4. RISK MISMATCH ALERT
-- Question: Are conservative users taking aggressive positions?
-- ============================================================================

WITH user_trading_behavior AS (
    SELECT 
        p.portfolio_id,
        p.user_id,
        COUNT(DISTINCT f.symbol) as symbols_traded,
        SUM(f.notional_traded_usd) as total_volume,
        AVG(ABS(f.realized_pnl_usd)) as avg_absolute_pnl,
        SUM(f.num_trades) as total_trades
    FROM mongodb_dev.fintech_trading.portfolios p
    JOIN hive.gold_v1.fact_pnl_daily f 
        ON p.portfolio_id = f.portfolio_id
        AND f.ds >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY p.portfolio_id, p.user_id
)
SELECT 
    u.user_id,
    u.name,
    u.risk_profile as declared_profile,
    CASE 
        WHEN tb.symbols_traded > 15 AND tb.total_volume > 50000 THEN 'Very Aggressive'
        WHEN tb.symbols_traded > 10 AND tb.total_volume > 30000 THEN 'Aggressive'
        WHEN tb.symbols_traded > 5 AND tb.total_volume > 10000 THEN 'Moderate'
        ELSE 'Conservative'
    END as actual_behavior,
    tb.symbols_traded,
    ROUND(tb.total_volume, 2) as total_volume_usd,
    tb.total_trades
FROM mongodb_dev.fintech_trading.users u
JOIN user_trading_behavior tb 
    ON u.user_id = tb.user_id
WHERE u.kyc_status = 'verified'
  AND (
      (u.risk_profile = 'conservative' AND tb.total_volume > 30000) OR
      (u.risk_profile = 'moderate' AND tb.total_volume > 50000)
  )
ORDER BY tb.total_volume DESC;

