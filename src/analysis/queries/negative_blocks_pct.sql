-- Negative Blocks Percentage by Block Type
-- Calculate the percentage of blocks with negative profit by block type
--
-- This query shows what fraction of blocks have negative builder_balance_increase
-- (i.e., the builder lost money) for both MEV-Boost and vanilla blocks.
--
-- MEV-Boost blocks: relays IS NOT NULL and array_length(relays, 1) IS NOT NULL
-- Vanilla blocks: relays IS NULL or array_length(relays, 1) IS NULL
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - block_type: 'mev_boost' or 'vanilla'
-- - negative_blocks: Number of blocks with negative profit
-- - total_blocks: Total number of blocks
-- - negative_pct: Percentage of blocks with negative profit
-- - total_negative_value_eth: Total value lost in negative blocks (absolute value)
--
-- Usage in Grafana:
-- - Visualization: Bar gauge or Table
-- - Shows which block type has more unprofitable blocks

WITH block_stats AS (
    SELECT
        CASE
            WHEN relays IS NULL OR array_length(relays, 1) IS NULL THEN 'vanilla'
            ELSE 'mev_boost'
        END as block_type,
        COUNT(*) FILTER (WHERE builder_balance_increase < 0) as negative_blocks,
        COUNT(*) as total_blocks,
        SUM(ABS(COALESCE(builder_balance_increase, 0))) FILTER (WHERE builder_balance_increase < 0) as total_negative_value_eth
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND builder_balance_increase IS NOT NULL
    GROUP BY
        CASE
            WHEN relays IS NULL OR array_length(relays, 1) IS NULL THEN 'vanilla'
            ELSE 'mev_boost'
        END
)
SELECT
    block_type,
    negative_blocks,
    total_blocks,
    ROUND((negative_blocks::numeric / NULLIF(total_blocks, 0) * 100), 2) as negative_pct,
    ROUND(total_negative_value_eth::numeric, 4) as total_negative_value_eth
FROM block_stats
ORDER BY negative_pct DESC;
