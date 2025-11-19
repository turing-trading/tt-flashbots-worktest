-- Block Type Breakdown (Time Series)
-- Shows total blocks, vanilla blocks, and MEV-Boost blocks over time
--
-- This query aggregates blocks by type within configurable time buckets
-- to visualize the split between vanilla and MEV-Boost blocks over time.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, $__interval): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for aggregation
-- - block_type: Type of blocks ("Total Blocks", "Vanilla Blocks", "MEV-Boost Blocks")
-- - block_count: Number of blocks of this type in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval (auto) or fixed like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "block_type" (this creates separate series per type)
--    - Keep fields: Select "time" and "block_count"
-- 5. Three lines will appear: Total, Vanilla, MEV-Boost
--
-- Grafana Panel Configuration:
-- - Visualization: Time series (line chart) or Bar chart (stacked)
-- - Y-axis: Label as "Block Count", no unit transformation needed
-- - Legend: Show values (Min, Max, Mean, Last)
-- - Legend mode: Table for detailed breakdown
-- - Stack series: Enable for stacked view (Vanilla + MEV-Boost = Total)
-- - Fill opacity: 30-50% for stacked view
--
-- Color Suggestions:
-- - Total Blocks: Blue
-- - MEV-Boost Blocks: Green
-- - Vanilla Blocks: Orange
--
-- Alternative: Stacked Bar Chart
-- - Shows composition clearly
-- - Visualization: Bar chart
-- - Bar chart mode: Stacked
-- - Only show Vanilla + MEV-Boost (they stack to Total)

WITH block_counts AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        COUNT(*) as total_blocks,
        COUNT(*) FILTER (WHERE is_block_vanilla = true) as vanilla_blocks,
        COUNT(*) FILTER (WHERE is_block_vanilla = false) as mev_boost_blocks
    FROM analysis_pbs_v2
    WHERE $__timeFilter(block_timestamp)
    GROUP BY time
)
SELECT time, 'Total Blocks' as block_type, total_blocks as block_count FROM block_counts
UNION ALL
SELECT time, 'Vanilla Blocks' as block_type, vanilla_blocks as block_count FROM block_counts
UNION ALL
SELECT time, 'MEV-Boost Blocks' as block_type, mev_boost_blocks as block_count FROM block_counts
ORDER BY time, block_type;
