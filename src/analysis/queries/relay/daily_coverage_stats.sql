-- Daily Relay Coverage Statistics
-- Detect gaps in relay data by analyzing daily block counts and identifying outliers
--
-- This query groups relay payloads by relay and day, calculates statistics,
-- and identifies days with abnormally low coverage (potential gaps).
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter (optional)
--
-- Returns:
-- - relay: Relay domain name
-- - date: Date (YYYY-MM-DD)
-- - block_count: Number of blocks from this relay on this day
-- - relay_avg: Average blocks per day for this relay (overall)
-- - relay_stddev: Standard deviation of blocks per day
-- - pct_of_avg: Percentage of average (block_count / relay_avg * 100)
-- - is_outlier: True if block_count is <50% of average or <2 stddev below mean
-- - missing_estimate: Estimated missing blocks (relay_avg - block_count)
--
-- Usage:
-- - Identify days where specific relays have abnormally low data
-- - Useful for detecting titanrelay.xyz gaps or other relay inconsistencies

WITH daily_counts AS (
    SELECT
        rp.relay,
        DATE(b.timestamp) as date,
        COUNT(*) as block_count,
        MIN(rp.slot) as min_slot,
        MAX(rp.slot) as max_slot
    FROM relays_payloads rp
    INNER JOIN blocks b ON rp.block_number = b.number
    WHERE TRUE
        -- Optional time filter for Grafana
        -- AND $__timeFilter(b.timestamp)
    GROUP BY rp.relay, DATE(b.timestamp)
),
relay_stats AS (
    SELECT
        relay,
        AVG(block_count) as relay_avg,
        STDDEV(block_count) as relay_stddev,
        COUNT(*) as total_days
    FROM daily_counts
    GROUP BY relay
)
SELECT
    dc.relay,
    dc.date,
    dc.block_count,
    dc.min_slot,
    dc.max_slot,
    ROUND(rs.relay_avg::numeric, 2) as relay_avg,
    ROUND(rs.relay_stddev::numeric, 2) as relay_stddev,
    rs.total_days,
    ROUND((dc.block_count::numeric / NULLIF(rs.relay_avg, 0) * 100), 2) as pct_of_avg,
    -- Mark as outlier if:
    -- 1. Less than 50% of average
    -- 2. More than 2 standard deviations below mean
    CASE
        WHEN dc.block_count < (rs.relay_avg * 0.5) THEN true
        WHEN dc.block_count < (rs.relay_avg - 2 * rs.relay_stddev) THEN true
        ELSE false
    END as is_outlier,
    -- Estimate missing blocks (could be negative if above average)
    ROUND(GREATEST(0, rs.relay_avg - dc.block_count)::numeric, 0) as missing_estimate
FROM daily_counts dc
INNER JOIN relay_stats rs ON dc.relay = rs.relay
ORDER BY dc.relay, dc.date DESC;
