-- Negative Total Value blocks
-- Row: Value and Profitability
-- Lists all blocks where total value was negative.
-- Helps diagnose which builders are losing money, and whether specific relays or strategies malfunctioned.
--

-- Top Negative Total Value Blocks
-- Show the 5 most negative total_value blocks for both MEV-Boost and vanilla blocks
--
-- This query identifies blocks where the total MEV value (builder_balance_increase + proposer_subsidy)
-- was most negative, indicating the largest losses or costs.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - block_type: 'mev_boost' or 'vanilla'
-- - block_number: Block number
-- - block_timestamp: Block timestamp
-- - builder_name: Name of the builder (or 'unknown')
-- - total_value: Total MEV value in ETH (negative = loss)
-- - builder_balance_increase: Builder profit in ETH
-- - proposer_subsidy: Payment to proposer in ETH
-- - n_relays: Number of relays used (0 for vanilla)
-- - rank: Ranking within block type (1 = most negative)
--
-- Usage in Grafana:
-- - Visualization: Table
-- - Shows extreme negative value blocks for analysis
-- - Useful for identifying anomalies or unusual market conditions

WITH ranked_negative_blocks AS (
    SELECT
        CASE
            WHEN is_block_vanilla THEN 'vanilla'
            ELSE 'mev_boost'
        END as block_type,
        block_number,
        block_timestamp,
        builder_name,
        total_value,
        builder_balance_increase,
        proposer_subsidy,
        n_relays,
        ROW_NUMBER() OVER (
            PARTITION BY is_block_vanilla
            ORDER BY total_value ASC
        ) as rank
    FROM analysis_pbs_v3
    WHERE
        $__timeFilter(block_timestamp)
        AND total_value < 0
)
SELECT
    block_type,
    block_number,
    block_timestamp,
    builder_name,
    ROUND(total_value::numeric, 6) as total_value_eth,
    ROUND(builder_balance_increase::numeric, 6) as builder_balance_increase_eth,
    ROUND(proposer_subsidy::numeric, 6) as proposer_subsidy_eth
    --n_relays,
    --rank
FROM ranked_negative_blocks
WHERE rank <= 5
ORDER BY block_type DESC, rank ASC;
