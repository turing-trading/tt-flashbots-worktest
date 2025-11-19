-- Relay Availability (Time Series)
-- Track which relays are actively delivering blocks over time
--
-- This query monitors relay uptime by checking whether each relay
-- delivered at least one block in each time window. Useful for detecting
-- relay outages, data gaps, or onboarding/offboarding of relays.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, $__interval): Grafana time grouping (e.g., '1h', '6h', '1d')
--
-- Returns:
-- - time: Time bucket for aggregation
-- - relay: Relay name
-- - is_active: 1 if relay delivered blocks, 0 if missing/inactive
-- - blocks_delivered: Number of blocks (for reference)
-- - first_block: First block number in this window
-- - last_block: Last block number in this window
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or fixed like '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "relay"
--    - Keep fields: Select "time" and "is_active"
-- 5. Each relay appears as a separate series (1 = active, 0 = inactive)
--
-- Grafana Panel Configuration:
-- - Visualization: State timeline or Status history
-- - Y-axis: Shows each relay as a separate row
-- - Color scheme: Green (1 = active), Red (0 = inactive)
-- - Tooltip: Show blocks_delivered for context
-- - Legend: Hide (relay names shown in Y-axis)
--
-- Alternative: Heatmap Visualization
-- - Transform: Convert field type of is_active to numeric
-- - Visualization: Heatmap
-- - Y-axis: relay names
-- - X-axis: time
-- - Color: Green (active) to Red (inactive)
--
-- Example Alert:
-- "Alert if relay has been inactive for >24 hours"
-- WHEN last_over_time(is_active[24h]) = 0

WITH all_relays AS (
    -- Get list of all known relays from the constants
    -- This ensures we show gaps even when relay has 0 blocks
    SELECT DISTINCT UNNEST(relays) as relay
    FROM analysis_pbs_v2
    WHERE $__timeFilter(block_timestamp)
        AND relays IS NOT NULL
),
time_buckets AS (
    -- Generate time series based on Grafana interval
    SELECT DISTINCT $__timeGroup(block_timestamp, $__interval) as time
    FROM analysis_pbs_v2
    WHERE $__timeFilter(block_timestamp)
),
relay_activity AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        UNNEST(relays) as relay,
        COUNT(*) as blocks_delivered,
        MIN(block_number) as first_block,
        MAX(block_number) as last_block
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
        AND relays IS NOT NULL
    GROUP BY time, relay
),
complete_grid AS (
    -- Create full grid of time x relay combinations
    SELECT
        tb.time,
        ar.relay
    FROM time_buckets tb
    CROSS JOIN all_relays ar
)
SELECT
    cg.time,
    cg.relay,
    COALESCE(ra.blocks_delivered, 0) as blocks_delivered,
    CASE
        WHEN ra.blocks_delivered IS NOT NULL THEN 1
        ELSE 0
    END as is_active,
    ra.first_block,
    ra.last_block
FROM complete_grid cg
LEFT JOIN relay_activity ra ON cg.time = ra.time AND cg.relay = ra.relay
ORDER BY cg.relay, cg.time;
