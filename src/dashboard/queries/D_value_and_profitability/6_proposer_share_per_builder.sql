-- Proposer share (per builder)
-- Row: Value and Profitability
-- Proposer share of total value in block.
--

-- Builder Profit Split Over Time
-- Shows the percentage of total value that goes to proposers vs builders over time, grouped by builder
--
-- This query aggregates by time and builder_name:
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
-- - $__timeGroup(block_timestamp, '1d'): Groups data into time buckets (1 day default)
--
-- Returns:
-- - time: Time bucket
-- - builder_name: Name of the builder
-- - proposer_profit_pct: Percentage of total value paid to proposers (0-100)
-- - total_proposer_profit_eth: Total profit paid to proposers in ETH
-- - total_builder_profit_eth: Total profit kept by builder in ETH
-- - total_value_eth: Total value (proposer + builder profit) in ETH
-- - block_count: Number of MEV-Boost blocks
--
-- Usage in Grafana:
-- - Visualization: Time series (lines)
-- - Format proposer_profit_pct as percentage
-- - Shows how builder profit splits evolve over time
-- - Only includes top 5 builders by total block count in the time range

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
    $__timeGroup(apv.block_timestamp, $__interval) as time,
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
    AND NOT apv.is_block_vanilla AND apv.total_value > 0.01
GROUP BY time, apv.builder_name
ORDER BY time, apv.builder_name;
