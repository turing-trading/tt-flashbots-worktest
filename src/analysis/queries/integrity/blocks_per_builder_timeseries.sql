-- Blocks Per Builder (Time Series)
-- Count total blocks produced by each builder over rolling time windows
--
-- This query aggregates block counts by builder within configurable time buckets
-- (e.g., hourly, daily) over the selected time range. Shows absolute block counts
-- rather than market share percentages, useful for monitoring builder activity levels.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, $__interval): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for aggregation
-- - builder: Builder name
-- - blocks_produced: Total number of blocks produced by this builder in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval (auto) or fixed like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "builder" (this creates separate series per builder)
--    - Keep fields: Select "time" and "blocks_produced"
-- 5. Each builder will appear as a separate line on the graph
--
-- Grafana Panel Configuration:
-- - Visualization: Time series (line chart) or Bar chart (stacked)
-- - Y-axis: Label as "Blocks Produced", no unit transformation needed
-- - Legend: Show values (Min, Max, Mean, Last) for quick stats
-- - Legend mode: Table for detailed breakdown
-- - Stack series: Enable for stacked area chart (shows total block production)
-- - Fill opacity: 30-50% for stacked view, 0% for line view
--
-- Color Suggestions (Top Builders):
-- - Titan Builder: Purple
-- - rsync-builder: Blue
-- - beaverbuild.org: Orange
-- - Flashbots: Green
-- - unknown: Gray
--
-- Alternative Visualization (Grafana 10+):
-- - Use "Multi-frame time series" transformation for automatic series splitting
-- - Or use "Group to nested tables" to create hierarchical views
--
-- Example Stacked Bar Chart:
-- - Visualization: Bar chart
-- - Bar chart mode: Stacked
-- - Shows total builder capacity over time with builder breakdown
--
-- Notes:
-- - Only includes MEV-Boost blocks (excludes vanilla blocks)
-- - Builder names are normalized (e.g., "geth" variants → "unknown")
-- - Time buckets with no blocks will not appear (sparse data)

SELECT
    $__timeGroup(block_timestamp, $__interval) as time,
    builder_name as builder,
    COUNT(*) as blocks_produced
FROM analysis_pbs_v2
WHERE
    $__timeFilter(block_timestamp)
    AND NOT is_block_vanilla
    AND builder_name IS NOT NULL
GROUP BY time, builder_name
ORDER BY time, builder;
