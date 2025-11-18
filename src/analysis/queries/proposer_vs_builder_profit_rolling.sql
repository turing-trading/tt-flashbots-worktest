-- Proposer vs Builder Profit Comparison (Rolling Window)
-- Compare proposer and builder profits over time
--
-- This query shows how the profit split between proposers and builders
-- evolves over time using configurable time buckets.
--
-- - Proposer profit: Sum of proposer_subsidy (payment from builder to proposer)
-- - Builder profit: Sum of builder_balance_increase (onchain balance increase)
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, '1h'): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for the rolling window
-- - profit_type: Either "Proposer Profit" or "Builder Profit"
-- - total_profit_eth: Total profit in ETH for this time bucket
-- - avg_profit_per_block_eth: Average profit per block in ETH for this time bucket
-- - block_count: Number of MEV-Boost blocks in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or a fixed value like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "profit_type" (this is the column to split by)
--    - Keep fields: Select "time" and "total_profit_eth"
-- 5. Two lines will appear: one for proposer profit, one for builder profit
--
-- Grafana Panel Configuration:
-- - Visualization: Time series
-- - Y-axis: Label as "Profit (ETH)", set unit to "currencyUSD" or leave as number
-- - Legend: Show both series for easy comparison
-- - Legend mode: Table for better readability
-- - Optional: Add a second Y-axis for percentage split
-- - Optional: Use stacked mode to see total MEV extracted
--
-- For Average Profit per Block:
-- - Change "Keep fields" to use "avg_profit_per_block_eth" instead
-- - Y-axis label: "Average Profit per Block (ETH)"
--
-- Alternative (Grafana 10+): Use "Multi-frame time series" transformation
-- This automatically groups by the profit_type column without additional configuration

WITH time_buckets AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        SUM(COALESCE(proposer_subsidy, 0)) as total_proposer_profit,
        SUM(COALESCE(builder_balance_increase, 0)) as total_builder_profit,
        AVG(COALESCE(proposer_subsidy, 0)) as avg_proposer_profit,
        AVG(COALESCE(builder_balance_increase, 0)) as avg_builder_profit,
        COUNT(*) as block_count
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND relays IS NOT NULL
        AND array_length(relays, 1) IS NOT NULL
    GROUP BY time
)
SELECT
    time,
    'Proposer Profit' as profit_type,
    ROUND(total_proposer_profit::numeric, 4) as total_profit_eth,
    ROUND(avg_proposer_profit::numeric, 4) as avg_profit_per_block_eth,
    block_count
FROM time_buckets

UNION ALL

SELECT
    time,
    'Builder Profit' as profit_type,
    ROUND(total_builder_profit::numeric, 4) as total_profit_eth,
    ROUND(avg_builder_profit::numeric, 4) as avg_profit_per_block_eth,
    block_count
FROM time_buckets

ORDER BY time, profit_type;
