-- MEV-Boost Market Share
-- Show the fraction of blocks that use MEV-Boost vs vanilla blocks
--
-- Vanilla blocks are blocks where proposers build themselves without using MEV-Boost relays.
-- These are identified by blocks with NULL or empty relays array.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - block_type: 'mev_boost' or 'vanilla'
-- - block_count: Number of blocks of this type
-- - market_share_pct: Percentage of total blocks

WITH block_types AS (
    SELECT
        CASE
            WHEN relays IS NULL OR array_length(relays, 1) IS NULL THEN 'vanilla'
            ELSE 'mev_boost'
        END as block_type,
        COUNT(*) as block_count
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
    GROUP BY
        CASE
            WHEN relays IS NULL OR array_length(relays, 1) IS NULL THEN 'vanilla'
            ELSE 'mev_boost'
        END
),
total_blocks AS (
    SELECT SUM(block_count) as total
    FROM block_types
)
SELECT
    bt.block_type,
    bt.block_count,
    ROUND((bt.block_count::numeric / tb.total * 100), 2) as market_share_pct
FROM block_types bt
CROSS JOIN total_blocks tb
ORDER BY bt.block_count DESC;
