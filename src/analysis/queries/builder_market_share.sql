-- Builder Market Share
-- Calculate the fraction of blocks produced by each builder
--
-- This query shows the percentage of blocks built by each builder,
-- only counting MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder
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
total_blocks AS (
    SELECT SUM(blocks_built) as total
    FROM builder_counts
)
SELECT
    bc.builder_name,
    bc.blocks_built,
    ROUND((bc.blocks_built::numeric / tb.total * 100), 2) as market_share_pct
FROM builder_counts bc
CROSS JOIN total_blocks tb
ORDER BY bc.blocks_built DESC;
