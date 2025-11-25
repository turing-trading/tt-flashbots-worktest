-- MEV-Boost Market Share
-- Row: General
-- Tracks how the share of MEV-Boost blocks changes over time.
-- Useful for spotting shifts in relay adoption, outages, or network-wide MEV dynamics.
--

-- MEV-Boost Market Share (Rolling Window)
-- Show the fraction of blocks that use MEV-Boost vs vanilla blocks over time
--
-- Vanilla blocks are blocks where proposers build themselves without using MEV-Boost relays.
-- These are identified by blocks with NULL or empty relays array.
--
-- This query shows how MEV-Boost adoption changes over time with configurable time buckets.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, '1h'): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for the rolling window
-- - block_type: 'mev_boost' or 'vanilla'
-- - block_count: Number of blocks of this type in this time bucket
-- - market_share_pct: Percentage of blocks of this type in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or a fixed value like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "block_type" (this is the column to split by)
--    - Keep fields: Select "time" and "market_share_pct"
-- 5. You will get two lines: one for 'mev_boost' and one for 'vanilla'
--
-- Grafana Panel Configuration:
-- - Visualization: Time series (or Area chart for stacked view)
-- - Y-axis: Label as "Market Share (%)", set unit to "percent (0-100)"
-- - Legend: Show values to see adoption percentages
-- - For stacked view: In panel options, set "Stack series" to "Normal"
--
-- Alternative (Grafana 10+): Use "Multi-frame time series" transformation
-- This automatically groups by the block_type column without additional configuration

WITH block_types AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        CASE
            WHEN is_block_vanilla THEN 'vanilla'
            ELSE 'mev_boost'
        END as block_type
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
),
block_counts AS (
    SELECT
        time,
        block_type,
        COUNT(*) as block_count
    FROM block_types
    GROUP BY time, block_type
),
total_per_time AS (
    SELECT
        time,
        SUM(block_count) as total
    FROM block_counts
    GROUP BY time
)
SELECT
    bc.time,
    bc.block_type,
    --bc.block_count,
    ROUND((bc.block_count::numeric / tpt.total * 100), 2) as market_share_pct
FROM block_counts bc
JOIN total_per_time tpt ON bc.time = tpt.time
ORDER BY bc.time, bc.block_type;
