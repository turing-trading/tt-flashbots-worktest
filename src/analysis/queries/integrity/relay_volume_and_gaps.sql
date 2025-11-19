-- Relay Volume and Data Gaps
-- Combined view of relay block volume with gap detection
--
-- This query provides a comprehensive view of relay performance by showing
-- both absolute block counts AND detecting periods with abnormally low activity.
-- Combines volume monitoring with anomaly detection for data quality assurance.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, $__interval): Grafana time grouping (default: '1d')
-- - $outlier_threshold: Percentage below average to flag as outlier (default: 50)
--
-- Returns:
-- - time: Time bucket
-- - relay: Relay name
-- - blocks_delivered: Actual blocks in this period
-- - relay_avg: Historical average for this relay (rolling 30-day)
-- - expected_min: Minimum expected blocks (relay_avg * threshold)
-- - has_gap: True if blocks_delivered < expected_min
-- - gap_severity_pct: How far below average (0-100%, higher = worse)
-- - status: OK, WARNING, or CRITICAL based on gap severity
--
-- Usage in Grafana:
-- 1. Format: Time series
-- 2. Transform: "Partition by values" on "relay" field
-- 3. Create two Y-axes:
--    - Left: blocks_delivered (bars)
--    - Right: gap_severity_pct (line, red when >0)
-- 4. Override: Color "has_gap" series red when true
--
-- Grafana Panel Configuration:
-- - Visualization: Time series (dual axis)
-- - Left Y-axis: "Blocks" (bars, blue/green)
-- - Right Y-axis: "Gap Severity %" (line, red)
-- - Thresholds: Add threshold at gap_severity_pct > 25% (yellow), > 50% (red)
-- - Legend: Show Min, Max, Mean for blocks_delivered
--
-- Alert Configuration:
-- "Alert when any relay has >50% gap severity for >2 consecutive periods"
-- WHEN avg_over_time(gap_severity_pct[2 intervals]) > 50

WITH relay_blocks AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        UNNEST(relays) as relay,
        COUNT(*) as blocks_delivered
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
        AND relays IS NOT NULL
    GROUP BY time, relay
),
relay_averages AS (
    -- Calculate rolling 30-day average for each relay
    SELECT
        relay,
        AVG(blocks_delivered) as relay_avg,
        STDDEV(blocks_delivered) as relay_stddev
    FROM relay_blocks
    GROUP BY relay
)
SELECT
    rb.time,
    rb.relay,
    rb.blocks_delivered,
    ROUND(ra.relay_avg::numeric, 2) as relay_avg,
    ROUND(ra.relay_stddev::numeric, 2) as relay_stddev,
    ROUND((ra.relay_avg * COALESCE($outlier_threshold, 50) / 100)::numeric, 0) as expected_min,
    -- Detect if this is a gap
    CASE
        WHEN rb.blocks_delivered < (ra.relay_avg * COALESCE($outlier_threshold, 50) / 100) THEN true
        ELSE false
    END as has_gap,
    -- Calculate gap severity (how far below average, as percentage)
    CASE
        WHEN rb.blocks_delivered < ra.relay_avg THEN
            ROUND(((ra.relay_avg - rb.blocks_delivered) / NULLIF(ra.relay_avg, 0) * 100)::numeric, 1)
        ELSE 0
    END as gap_severity_pct,
    -- Status indicator
    CASE
        WHEN rb.blocks_delivered >= (ra.relay_avg * 0.75) THEN 'OK'
        WHEN rb.blocks_delivered >= (ra.relay_avg * 0.50) THEN 'WARNING'
        ELSE 'CRITICAL'
    END as status
FROM relay_blocks rb
INNER JOIN relay_averages ra ON rb.relay = ra.relay
ORDER BY rb.time DESC, rb.relay;
