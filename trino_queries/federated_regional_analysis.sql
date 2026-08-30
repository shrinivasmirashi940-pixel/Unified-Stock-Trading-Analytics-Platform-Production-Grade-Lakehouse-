-- Federated Query: Regional Trading Patterns
-- Analyzes trading behavior across geographic regions

-- ============================================================================
-- 1. REGIONAL TRADING VOLUME
-- Question: Which region generates the most trading activity?
-- ============================================================================

SELECT 
    u.region,
    COUNT(DISTINCT u.user_id) as num_users,
    COUNT(DISTINCT p.portfolio_id) as num_portfolios,
    SUM(a.num_trades) as total_trades,
    ROUND(SUM(a.total_notional_usd), 2) as total_volume_usd,
    ROUND(AVG(a.activity_score), 2) as avg_activity_score
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_user_activity a 
    ON p.portfolio_id = a.portfolio_id
    AND a.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.kyc_status = 'verified'
GROUP BY u.region
ORDER BY total_volume_usd DESC;


-- ============================================================================
-- 2. REGIONAL PROFITABILITY
-- Question: Which region's users are most profitable?
-- ============================================================================

SELECT 
    u.region,
    COUNT(DISTINCT u.user_id) as num_users,
    ROUND(SUM(f.realized_pnl_usd), 2) as total_pnl_usd,
    ROUND(AVG(f.realized_pnl_usd), 2) as avg_pnl_per_position,
    ROUND(
        SUM(f.realized_pnl_usd) / NULLIF(SUM(f.notional_traded_usd), 0) * 100, 
        4
    ) as roi_percentage
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_pnl_daily f 
    ON p.portfolio_id = f.portfolio_id
    AND f.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.kyc_status = 'verified'
GROUP BY u.region
ORDER BY total_pnl_usd DESC;


-- ============================================================================
-- 3. REGIONAL RISK PROFILE DISTRIBUTION
-- Question: How do risk preferences vary by region?
-- ============================================================================

SELECT 
    u.region,
    u.risk_profile,
    COUNT(DISTINCT u.user_id) as num_users,
    SUM(a.num_trades) as total_trades
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_user_activity a 
    ON p.portfolio_id = a.portfolio_id
    AND a.ds >= CURRENT_DATE - INTERVAL '7' DAY
WHERE u.kyc_status = 'verified'
GROUP BY u.region, u.risk_profile
ORDER BY u.region, 
    CASE u.risk_profile 
        WHEN 'very_aggressive' THEN 1
        WHEN 'aggressive' THEN 2
        WHEN 'moderate' THEN 3
        WHEN 'conservative' THEN 4
    END;


-- ============================================================================
-- 4. REGIONAL SYMBOL PREFERENCES
-- Question: Do different regions prefer different stocks?
-- ============================================================================

SELECT 
    u.region,
    f.symbol,
    COUNT(DISTINCT p.portfolio_id) as num_portfolios_traded,
    SUM(f.num_trades) as total_trades,
    ROUND(SUM(f.notional_traded_usd), 2) as total_volume_usd
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
JOIN hive.gold_v1.fact_pnl_daily f 
    ON p.portfolio_id = f.portfolio_id
    AND f.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.kyc_status = 'verified'
GROUP BY u.region, f.symbol
HAVING SUM(f.num_trades) > 10  -- Only significant trading activity
ORDER BY u.region, total_volume_usd DESC;

