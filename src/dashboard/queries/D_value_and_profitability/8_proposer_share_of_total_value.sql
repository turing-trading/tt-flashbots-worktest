-- Proposer share of total value
-- Row: Value and Profitability
-- Useful for monitoring whether builders retain too much value or if proposer rewards are healthy.
--

-- Builder Profit Split Analysis
-- Shows the percentage of total value that goes to proposers vs builders, grouped by builder
--
-- This query aggregates by builder_name:
-- - Proposer profit: proposer_subsidy (payment from builder to proposer)
-- - Builder profit: total_value - proposer_subsidy - relay_fee
-- - Proposer profit %: Percentage of total value that goes to proposers
--
-- The total_value field already includes the correct logic for builder_extra_transfers:
-- - builder_extra_transfers are only included when total_value would otherwise be negative
-- - This represents refunds/adjustments from known builder addresses (e.g., BuilderNet)
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder
-- - proposer_profit_pct: Percentage of total value paid to proposers (0-100)
-- - total_proposer_profit_eth: Total profit paid to proposers in ETH
-- - total_builder_profit_eth: Total profit kept by builder in ETH
-- - total_value_eth: Total value (proposer + builder profit) in ETH
-- - block_count: Number of MEV-Boost blocks
--
-- Usage in Grafana:
-- - Visualization: Bar chart or Table
-- - Format proposer_profit_pct as percentage
-- - Shows which builders pay more/less to proposers
-- - Only includes top 5 builders by block count

WITH top_builders AS (
    SELECT builder_name
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
    GROUP BY builder_name
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
SELECT
    apv.builder_name,
    ROUND(AVG(apv.pct_proposer_share)::numeric, 2) as proposer_profit_pct
    --ROUND(SUM(apv.proposer_subsidy)::numeric, 4) as total_proposer_profit_eth,
    --ROUND(SUM(apv.builder_profit)::numeric, 4) as total_builder_profit_eth,
    --ROUND(SUM(apv.total_value)::numeric, 4) as total_value_eth,
    --COUNT(*) as block_count
FROM analysis_pbs apv
INNER JOIN top_builders tb ON apv.builder_name = tb.builder_name
WHERE
    $__timeFilter(apv.block_timestamp)
    AND NOT apv.is_block_vanilla
GROUP BY apv.builder_name
ORDER BY proposer_profit_pct DESC;
