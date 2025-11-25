-- Average Total Value
-- Row: Value and Profitability
-- Displays average ETH earned per block for MEV-Boost and vanilla.
-- Useful high-level metric to quantify the value of MEV-Boost.
--

-- Average Profit by Block Type
-- Compare average total value between MEV-Boost and vanilla blocks
--
-- This query calculates the average total value where
-- total_value = builder_balance_increase + proposer_subsidy
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
-- - avg_profit_eth: Average total value per block in ETH (builder_balance_increase + proposer_subsidy)
-- - total_profit_eth: Total value in ETH (sum of builder_balance_increase + proposer_subsidy)
-- - block_count: Number of blocks of this type
--
-- Usage in Grafana:
-- - Visualization: Bar chart or Stat panel
-- - Format as: Table
-- - Compare profitability between MEV-Boost and vanilla blocks

WITH block_type_profits AS (
    SELECT
        CASE
            WHEN is_block_vanilla THEN 'vanilla'
            ELSE 'mev_boost'
        END as block_type,
        SUM(total_value) as total_profit_eth,
        AVG(total_value) as avg_profit_eth,
        COUNT(*) as block_count
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
    GROUP BY is_block_vanilla
)
SELECT
    block_type,
    ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth
    --ROUND(total_profit_eth::numeric, 4) as total_profit_eth,
    --block_count
FROM block_type_profits
ORDER BY block_type ASC;
