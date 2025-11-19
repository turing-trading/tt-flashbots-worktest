-- Blocks Per Relay (Time Series)
-- Count total blocks delivered by each relay over rolling time windows
--
-- This query aggregates block counts by relay within configurable time buckets
-- (e.g., hourly, daily) over the selected time range. Shows absolute block counts
-- rather than market share percentages, useful for monitoring relay activity levels.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, $__interval): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for aggregation
-- - relay: Relay name
-- - blocks_delivered: Total number of blocks delivered by this relay in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval (auto) or fixed like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "relay" (this creates separate series per relay)
--    - Keep fields: Select "time" and "blocks_delivered"
-- 5. Each relay will appear as a separate line on the graph
--
-- Grafana Panel Configuration:
-- - Visualization: Time series (line chart) or Bar chart (stacked)
-- - Y-axis: Label as "Blocks Delivered", no unit transformation needed
-- - Legend: Show values (Min, Max, Mean, Last) for quick stats
-- - Legend mode: Table for detailed breakdown
-- - Stack series: Enable for stacked area chart (shows total capacity)
-- - Fill opacity: 30-50% for stacked view, 0% for line view
--
-- Alternative Visualization (Grafana 10+):
-- - Use "Multi-frame time series" transformation for automatic series splitting
-- - Or use "Group to nested tables" to create hierarchical views
--
-- Example Stacked Bar Chart:
-- - Visualization: Bar chart
-- - Bar chart mode: Stacked
-- - Shows total relay capacity over time with relay breakdown

WITH relay_blocks AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        UNNEST(relays) as relay,
        block_number
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
        AND relays IS NOT NULL
)
SELECT
    time,
    relay,
    COUNT(*) as blocks_delivered
FROM relay_blocks
GROUP BY time, relay
ORDER BY time, relay;
