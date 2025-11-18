-- Builder Market Share (Rolling Window)
-- Calculate the fraction of blocks produced by each builder over time
--
-- This query shows the percentage of blocks built by each builder
-- within configurable time buckets, only counting MEV-Boost blocks.
-- Vanilla blocks (self-built by proposers) are excluded.
-- Top 9 builders are shown individually, rest grouped as "Others"
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, '1h'): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for the rolling window
-- - builder_name: Name of the builder (or "Others")
-- - blocks_built: Number of blocks built by this builder in this time bucket
-- - market_share_pct: Percentage of MEV-Boost blocks built by this builder in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or a fixed value like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "builder_name" (this is the column to split by)
--    - Keep fields: Select "time" and "market_share_pct"
-- 5. Each builder will become a separate line on the graph
--
-- Grafana Panel Configuration:
-- - Visualization: Time series
-- - Y-axis: Label as "Market Share (%)", set unit to "percent (0-100)"
-- - Legend: Show values (Avg, Max, Last) to see market share percentages
-- - Legend mode: Table for better readability with many builders
--
-- Alternative (Grafana 10+): Use "Multi-frame time series" transformation
-- This automatically groups by the builder_name column without additional configuration

WITH builder_counts AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        COALESCE(builder_name, 'unknown') as builder_name,
        COUNT(*) as blocks_built
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND relays IS NOT NULL
        AND array_length(relays, 1) IS NOT NULL
    GROUP BY time, builder_name
),
-- Identify top 9 builders across the entire time range
top_builders AS (
    SELECT builder_name
    FROM (
        SELECT
            builder_name,
            SUM(blocks_built) as total_blocks
        FROM builder_counts
        WHERE builder_name != 'unknown'
        GROUP BY builder_name
        ORDER BY total_blocks DESC
        LIMIT 9
    ) t
),
categorized_counts AS (
    SELECT
        bc.time,
        CASE
            WHEN bc.builder_name IN (SELECT builder_name FROM top_builders) THEN bc.builder_name
            ELSE 'Others'
        END as builder_name,
        bc.blocks_built
    FROM builder_counts bc
),
aggregated_counts AS (
    SELECT
        time,
        builder_name,
        SUM(blocks_built) as blocks_built
    FROM categorized_counts
    GROUP BY time, builder_name
),
total_per_time AS (
    SELECT
        time,
        SUM(blocks_built) as total
    FROM aggregated_counts
    GROUP BY time
)
SELECT
    ac.time,
    ac.builder_name,
    ac.blocks_built,
    ROUND((ac.blocks_built::numeric / tpt.total * 100), 2) as market_share_pct
FROM aggregated_counts ac
JOIN total_per_time tpt ON ac.time = tpt.time
ORDER BY ac.time, ac.blocks_built DESC;
