-- Federated Query: User Demographics & Trading Performance
-- Joins MongoDB (users, portfolios) with Iceberg Gold tables

-- ============================================================================
-- 1. TOP PROFITABLE USERS (Last 7 Days)
-- Question: Which users made the most profit and where are they from?
-- ============================================================================

SELECT 
    u.user_id,
    u.name,
    u.region,
    u.risk_profile,
    COUNT(DISTINCT p.portfolio_id) as num_portfolios,
    SUM(f.realized_pnl_usd) as total_pnl_usd,
    SUM(f.notional_traded_usd) as total_volume_usd,
    COUNT(DISTINCT f.symbol) as unique_symbols_traded
FROM hive.gold_v1.fact_pnl_daily f
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON f.portfolio_id = p.portfolio_id
JOIN mongodb_dev.fintech_trading.users u 
    ON p.user_id = u.user_id
WHERE f.ds >= CURRENT_DATE - INTERVAL '7' DAY
  AND u.kyc_status = 'verified'
GROUP BY u.user_id, u.name, u.region, u.risk_profile
ORDER BY total_pnl_usd DESC
LIMIT 20;


-- ============================================================================
-- 2. TRADING PERFORMANCE BY RISK PROFILE
-- Question: Do aggressive traders perform better than conservative ones?
-- ============================================================================

SELECT 
    u.risk_profile,
    COUNT(DISTINCT u.user_id) as num_users,
    SUM(f.num_trades) as total_trades,
    ROUND(AVG(f.num_trades), 2) as avg_trades_per_portfolio,
    ROUND(SUM(f.realized_pnl_usd), 2) as total_pnl_usd,
    ROUND(AVG(f.realized_pnl_usd), 2) as avg_pnl_per_position
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_pnl_daily f 
    ON p.portfolio_id = f.portfolio_id
    AND f.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.kyc_status = 'verified'
GROUP BY u.risk_profile
ORDER BY 
    CASE u.risk_profile 
        WHEN 'very_aggressive' THEN 1
        WHEN 'aggressive' THEN 2
        WHEN 'moderate' THEN 3
        WHEN 'conservative' THEN 4
    END;


-- ============================================================================
-- 3. USER AGE GROUP ANALYSIS
-- Question: Which age group trades most actively?
-- ============================================================================

SELECT 
    CASE 
        WHEN u.metadata.age < 30 THEN '18-29'
        WHEN u.metadata.age < 40 THEN '30-39'
        WHEN u.metadata.age < 50 THEN '40-49'
        WHEN u.metadata.age < 60 THEN '50-59'
        ELSE '60+'
    END as age_group,
    COUNT(DISTINCT u.user_id) as num_users,
    SUM(a.num_trades) as total_trades,
    ROUND(AVG(a.activity_score), 2) as avg_activity_score,
    ROUND(SUM(a.total_notional_usd), 2) as total_traded_usd
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_user_activity a 
    ON p.portfolio_id = a.portfolio_id
    AND a.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.kyc_status = 'verified'
GROUP BY age_group
ORDER BY age_group;


-- ============================================================================
-- 4. INCOME BRACKET vs TRADING BEHAVIOR
-- Question: Do higher income users trade larger volumes?
-- ============================================================================

SELECT 
    CASE 
        WHEN u.metadata.annual_income_usd < 50000 THEN '< $50K'
        WHEN u.metadata.annual_income_usd < 100000 THEN '$50K-$100K'
        WHEN u.metadata.annual_income_usd < 200000 THEN '$100K-$200K'
        ELSE '$200K+'
    END as income_bracket,
    COUNT(DISTINCT u.user_id) as num_users,
    SUM(a.num_trades) as total_trades,
    ROUND(AVG(a.total_notional_usd), 2) as avg_traded_per_user
FROM mongodb_dev.fintech_trading.users u
JOIN mongodb_dev.fintech_trading.portfolios p 
    ON u.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_user_activity a 
    ON p.portfolio_id = a.portfolio_id
    AND a.ds >= CURRENT_DATE - INTERVAL '30' DAY
WHERE u.kyc_status = 'verified'
GROUP BY income_bracket
ORDER BY 
    CASE income_bracket
        WHEN '< $50K' THEN 1
        WHEN '$50K-$100K' THEN 2
        WHEN '$100K-$200K' THEN 3
        ELSE 4
    END;


-- ============================================================================
-- 5. USER ACTIVATION RATE
-- Question: Are new users actively trading?
-- ============================================================================

WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', created_at) as signup_month
    FROM mongodb_dev.fintech_trading.users
    WHERE created_at >= CURRENT_DATE - INTERVAL '12' MONTH
      AND kyc_status = 'verified'
)
SELECT 
    uc.signup_month,
    COUNT(DISTINCT uc.user_id) as new_users,
    COUNT(DISTINCT CASE WHEN a.num_trades > 0 THEN uc.user_id END) as active_users,
    ROUND(
        COUNT(DISTINCT CASE WHEN a.num_trades > 0 THEN uc.user_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT uc.user_id), 0), 
        2
    ) as activation_rate_pct
FROM user_cohorts uc
LEFT JOIN mongodb_dev.fintech_trading.portfolios p 
    ON uc.user_id = p.user_id
LEFT JOIN hive.gold_v1.fact_user_activity a 
    ON p.portfolio_id = a.portfolio_id
    AND a.ds >= uc.signup_month
GROUP BY uc.signup_month
ORDER BY uc.signup_month DESC;

