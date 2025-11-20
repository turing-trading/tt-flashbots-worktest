-- Builder Share of Negative Blocks
-- Show each builder's share of the total negative value blocks market
--
-- This query calculates what percentage of ALL negative blocks
-- each builder is responsible for.
--
-- Only counts MEV-Boost blocks.
-- Vanilla blocks are excluded.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder
-- - negative_block_count: Number of blocks with negative total value
-- - pct_of_negative: Builder's share of all negative blocks (%)
--
-- Usage in Grafana:
-- - Visualization: Table or Pie chart
-- - Shows which builders contribute most to negative value blocks

WITH builder_negative_counts AS (
    SELECT
        builder_name,
        COUNT(*) as negative_block_count
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
        AND total_value < 0
    GROUP BY builder_name
),
total_negative AS (
    SELECT SUM(negative_block_count) as total
    FROM builder_negative_counts
)
SELECT
    bnc.builder_name,
    bnc.negative_block_count,
    ROUND((bnc.negative_block_count::numeric / tn.total * 100), 2) as pct_of_negative
FROM builder_negative_counts bnc
CROSS JOIN total_negative tn
ORDER BY pct_of_negative DESC;
