-- Builder Market Share
-- Calculate the fraction of blocks produced by each builder
--
-- This query shows the percentage of blocks built by each builder,
-- only counting MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
-- Top 9 builders are shown individually, rest grouped as "Others"
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder (or "Others")
-- - blocks_built: Number of blocks built by this builder
-- - market_share_pct: Percentage of total MEV-Boost blocks built

WITH builder_counts AS (
    SELECT
        COALESCE(builder_name, 'unknown') as builder_name,
        COUNT(*) as blocks_built
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND relays IS NOT NULL
        AND array_length(relays, 1) IS NOT NULL
    GROUP BY builder_name
),
top_builders AS (
    SELECT builder_name
    FROM builder_counts
    WHERE builder_name != 'unknown'
    ORDER BY blocks_built DESC
    LIMIT 9
),
categorized_counts AS (
    SELECT
        CASE
            WHEN bc.builder_name IN (SELECT builder_name FROM top_builders) THEN bc.builder_name
            ELSE 'Others'
        END as builder_name,
        bc.blocks_built
    FROM builder_counts bc
),
aggregated_counts AS (
    SELECT
        builder_name,
        SUM(blocks_built) as blocks_built
    FROM categorized_counts
    GROUP BY builder_name
),
total_blocks AS (
    SELECT SUM(blocks_built) as total
    FROM aggregated_counts
)
SELECT
    ac.builder_name,
    ac.blocks_built,
    ROUND((ac.blocks_built::numeric / tb.total * 100), 2) as market_share_pct
FROM aggregated_counts ac
CROSS JOIN total_blocks tb
ORDER BY ac.blocks_built DESC;
