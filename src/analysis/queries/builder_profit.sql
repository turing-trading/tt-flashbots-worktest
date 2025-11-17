-- Builder Profit
-- Show a ranking of builders sorted by their onchain profit
--
-- The builder's onchain profit is calculated as the balance difference before and after
-- the block (builder_balance_increase). This represents the profit the builder makes
-- from building the block.
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder
-- - total_profit_eth: Total profit in ETH (sum of all balance increases)
-- - avg_profit_eth: Average profit per block in ETH
-- - block_count: Number of MEV-Boost blocks built
-- - profit_rank: Ranking by total profit

WITH builder_profits AS (
    SELECT
        COALESCE(builder_name, 'unknown') as builder_name,
        SUM(COALESCE(builder_balance_increase, 0)) as total_profit_eth,
        AVG(COALESCE(builder_balance_increase, 0)) as avg_profit_eth,
        COUNT(*) as block_count
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND builder_balance_increase IS NOT NULL
        AND relays IS NOT NULL
        AND array_length(relays, 1) IS NOT NULL
    GROUP BY builder_name
)
SELECT
    builder_name,
    ROUND(total_profit_eth::numeric, 4) as total_profit_eth,
    ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth,
    block_count,
    RANK() OVER (ORDER BY total_profit_eth DESC) as profit_rank
FROM builder_profits
ORDER BY total_profit_eth DESC;
