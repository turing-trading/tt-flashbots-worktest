-- Negative Blocks Percentage by Block Type
-- Calculate the percentage of blocks with negative total value by block type
--
-- This query shows what fraction of blocks have negative total value where
-- total_value = builder_balance_increase + proposer_subsidy
-- (i.e., the total MEV extracted from the block is negative) for both MEV-Boost and vanilla blocks.
--
-- MEV-Boost blocks: relays IS NOT NULL and array_length(relays, 1) IS NOT NULL
-- Vanilla blocks: relays IS NULL or array_length(relays, 1) IS NULL
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - block_type: 'mev_boost' or 'vanilla'
-- - negative_blocks: Number of blocks with negative total value
-- - total_blocks: Total number of blocks
-- - negative_pct: Percentage of blocks with negative total value
-- - total_negative_value_eth: Total negative value (absolute value) where total_value < 0
--
-- Usage in Grafana:
-- - Visualization: Bar gauge or Table
-- - Shows which block type has more unprofitable blocks

WITH block_stats AS (
    SELECT
        CASE
            WHEN is_block_vanilla THEN 'vanilla'
            ELSE 'mev_boost'
        END as block_type,
        COUNT(*) FILTER (WHERE total_value < 0) as negative_blocks,
        COUNT(*) as total_blocks,
        SUM(ABS(total_value)) FILTER (WHERE total_value < 0) as total_negative_value_eth
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
    GROUP BY is_block_vanilla
)
SELECT
    block_type,
    negative_blocks,
    total_blocks,
    ROUND((negative_blocks::numeric / NULLIF(total_blocks, 0) * 100), 2) as negative_pct,
    ROUND(total_negative_value_eth::numeric, 4) as total_negative_value_eth
FROM block_stats
ORDER BY negative_pct DESC;
