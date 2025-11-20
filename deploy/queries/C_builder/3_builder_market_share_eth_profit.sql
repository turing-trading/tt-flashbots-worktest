-- Builder Market Share (ETH profit)
-- Row: Builder
-- Distribution of total MEV/ETH profit captured by each builder.
-- Shows not just who produces blocks—but who extracts the most value.
--

-- Builder Profit
-- Show a ranking of builders sorted by their onchain profit
--
-- The builder's onchain profit is calculated as:
-- 1. builder_balance_increase: Balance difference before and after the block
-- 2. builder_extra_transfers: Additional transfers from known builder addresses
--    (e.g., BuilderNet refund addresses)
--
-- Total profit = builder_balance_increase + builder_extra_transfers
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
-- - total_profit_eth: Total profit in ETH (including extra transfers)
-- - builder_balance_eth: Direct builder balance increase
-- - extra_transfers_eth: Additional builder transfers
-- - avg_profit_eth: Average profit per block in ETH
-- - block_count: Number of MEV-Boost blocks built
-- - profit_rank: Ranking by total profit

WITH builder_profits AS (
    SELECT
        builder_name,
        SUM(builder_balance_increase) as builder_balance_eth,
        SUM(COALESCE(builder_extra_transfers, 0)) as extra_transfers_eth,
        SUM(builder_balance_increase + COALESCE(builder_extra_transfers, 0)) as total_profit_eth,
        AVG(builder_balance_increase + COALESCE(builder_extra_transfers, 0)) as avg_profit_eth,
        COUNT(*) as block_count
    FROM analysis_pbs_v3
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
        bp.builder_balance_eth,
        bp.extra_transfers_eth,
        bp.total_profit_eth,
        bp.avg_profit_eth,
        bp.block_count
    FROM builder_profits bp
),
aggregated_profits AS (
    SELECT
        builder_name,
        SUM(builder_balance_eth) as builder_balance_eth,
        SUM(extra_transfers_eth) as extra_transfers_eth,
        SUM(total_profit_eth) as total_profit_eth,
        AVG(avg_profit_eth) as avg_profit_eth,
        SUM(block_count) as block_count
    FROM categorized_profits
    GROUP BY builder_name
)
SELECT
    builder_name,
    ROUND(total_profit_eth::numeric, 4) as total_profit_eth
  --  ROUND(builder_balance_eth::numeric, 4) as builder_balance_eth,
    --ROUND(extra_transfers_eth::numeric, 4) as extra_transfers_eth
    --ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth,
    --block_count,
    --RANK() OVER (ORDER BY total_profit_eth DESC) as profit_rank
FROM aggregated_profits
ORDER BY total_profit_eth DESC;
