-- Average Profit by Block Type
-- Compare average builder profit between MEV-Boost and vanilla blocks
--
-- This query calculates the average builder_balance_increase (onchain profit)
-- for both MEV-Boost blocks (using relays) and vanilla blocks (self-built).
--
-- MEV-Boost blocks: relays IS NOT NULL and array_length(relays, 1) IS NOT NULL
-- Vanilla blocks: relays IS NULL or array_length(relays, 1) IS NULL
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - block_type: 'mev_boost' or 'vanilla'
-- - avg_profit_eth: Average profit per block in ETH
-- - total_profit_eth: Total profit in ETH
-- - block_count: Number of blocks of this type
--
-- Usage in Grafana:
-- - Visualization: Bar chart or Stat panel
-- - Format as: Table
-- - Compare profitability between MEV-Boost and vanilla blocks

WITH block_type_profits AS (
    SELECT
        CASE
            WHEN relays IS NULL OR array_length(relays, 1) IS NULL THEN 'vanilla'
            ELSE 'mev_boost'
        END as block_type,
        SUM(COALESCE(builder_balance_increase, 0)) as total_profit_eth,
        AVG(COALESCE(builder_balance_increase, 0)) as avg_profit_eth,
        COUNT(*) as block_count
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
    ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth,
    ROUND(total_profit_eth::numeric, 4) as total_profit_eth,
    block_count
FROM block_type_profits
ORDER BY avg_profit_eth DESC;
