-- Proposer vs Builder Profit Comparison
-- Compare total profits between proposers and builders
--
-- This query aggregates:
-- - Proposer profit: Sum of proposer_subsidy (payment from builder to proposer)
-- - Builder profit: Sum of builder_balance_increase (onchain balance increase)
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - profit_type: Either "Proposer Profit" or "Builder Profit"
-- - total_profit_eth: Total profit in ETH
-- - avg_profit_per_block_eth: Average profit per block in ETH
-- - block_count: Number of MEV-Boost blocks
--
-- Usage in Grafana:
-- - Visualization: Bar gauge or Stat panel
-- - Format as: Table
-- - Use this to see the total split between proposers and builders
-- - For percentage split, create a calculated field dividing by the sum

WITH profit_data AS (
    SELECT
        SUM(proposer_subsidy) as total_proposer_profit,
        SUM(builder_balance_increase) as total_builder_profit,
        AVG(proposer_subsidy) as avg_proposer_profit,
        AVG(builder_balance_increase) as avg_builder_profit,
        COUNT(*) as block_count
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
        
)
SELECT
    'Proposer Profit' as profit_type,
    ROUND(total_proposer_profit::numeric, 4) as total_profit_eth,
    ROUND(avg_proposer_profit::numeric, 4) as avg_profit_per_block_eth,
    block_count
FROM profit_data

UNION ALL

SELECT
    'Builder Profit' as profit_type,
    ROUND(total_builder_profit::numeric, 4) as total_profit_eth,
    ROUND(avg_builder_profit::numeric, 4) as avg_profit_per_block_eth,
    block_count
FROM profit_data

ORDER BY total_profit_eth DESC;
