-- Proposer share (per builder)
-- Row: Value and Profitability
-- Proposer share of total value in block.
--

-- Builder Profit Split Over Time
-- Shows the percentage of total value that goes to proposers vs builders over time, grouped by builder
--
-- This query aggregates by time and builder_name:
-- - Proposer profit: Sum of proposer_subsidy (payment from builder to proposer)
-- - Builder profit: builder_balance_increase + builder_extra_transfers (only when builder_balance_increase < 0)
--   - builder_balance_increase: Direct balance increase of the builder address
--   - builder_extra_transfers: Additional transfers from known builder addresses (e.g., BuilderNet refunds)
--     Only added when builder_balance_increase is negative (loss scenario)
-- - Proposer profit %: Percentage of total value that goes to proposers
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
    FROM analysis_pbs_v3
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla AND total_value > 0.01
    GROUP BY builder_name
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
SELECT
    $__timeGroup(apv.block_timestamp, $__interval) as time,
    apv.builder_name,
    ROUND(
        (AVG(apv.proposer_subsidy / (apv.total_value)) * 100)::numeric,
        2
    ) as proposer_profit_pct
    --ROUND((SUM(apv.proposer_subsidy) / 1e18)::numeric, 4) as total_proposer_profit_eth,
    --ROUND((SUM(apv.builder_balance_increase + CASE WHEN apv.builder_balance_increase < 0 THEN COALESCE(apv.builder_extra_transfers, 0) ELSE 0 END) / 1e18)::numeric, 4) as total_builder_profit_eth,
    --ROUND((SUM(apv.total_value) / 1e18)::numeric, 4) as total_value_eth,
    --COUNT(*) as block_count
FROM analysis_pbs_v3 apv
INNER JOIN top_builders tb ON apv.builder_name = tb.builder_name
WHERE
    $__timeFilter(apv.block_timestamp)
    AND NOT apv.is_block_vanilla AND total_value > 0.01
GROUP BY time, apv.builder_name
ORDER BY time, apv.builder_name;
