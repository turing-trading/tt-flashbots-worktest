-- Builder Profit
-- Show a ranking of builders sorted by their onchain profit
--
-- The builder's onchain profit is calculated as the balance difference before and after
-- the block (builder_balance_increase). This represents the profit the builder makes
-- from building the block.
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
-- Top 9 builders are shown individually, rest grouped as "Others"
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder (or "Others")
-- - total_profit_eth: Total profit in ETH (sum of all balance increases)
-- - avg_profit_eth: Average profit per block in ETH
-- - block_count: Number of MEV-Boost blocks built
-- - profit_rank: Ranking by total profit

WITH builder_profits AS (
    SELECT
        builder_name,
        SUM(builder_balance_increase) as total_profit_eth,
        AVG(builder_balance_increase) as avg_profit_eth,
        COUNT(*) as block_count
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
    GROUP BY builder_name
),
top_builders AS (
    SELECT builder_name
    FROM builder_profits
    WHERE builder_name != 'unknown'
    ORDER BY total_profit_eth DESC
    LIMIT 9
),
categorized_profits AS (
    SELECT
        CASE
            WHEN bp.builder_name IN (SELECT builder_name FROM top_builders) THEN bp.builder_name
            ELSE 'Others'
        END as builder_name,
        bp.total_profit_eth,
        bp.avg_profit_eth,
        bp.block_count
    FROM builder_profits bp
),
aggregated_profits AS (
    SELECT
        builder_name,
        SUM(total_profit_eth) as total_profit_eth,
        AVG(avg_profit_eth) as avg_profit_eth,
        SUM(block_count) as block_count
    FROM categorized_profits
    GROUP BY builder_name
)
SELECT
    builder_name,
    ROUND(total_profit_eth::numeric, 4) as total_profit_eth,
    ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth,
    block_count,
    RANK() OVER (ORDER BY total_profit_eth DESC) as profit_rank
FROM aggregated_profits
ORDER BY total_profit_eth DESC;
