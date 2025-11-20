-- Builder Market Share (ETH profit)
-- Row: Builder
-- Tracks builder profitability over time.
-- Profit spikes often correlate with MEV opportunities (liquidations, arbitrage, mempool spikes).
--

-- Builder Profit (Rolling Window)
-- Show builder profit over time with rankings
--
-- The builder's onchain profit is calculated as:
-- 1. builder_balance_increase: Balance difference before and after the block
-- 2. builder_extra_transfers: Additional transfers from known builder addresses
--    (e.g., BuilderNet refund addresses)
--
-- Total profit = builder_balance_increase + builder_extra_transfers
-- This shows how builder profits evolve over time.
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
-- Top 9 builders are shown individually, rest grouped as "Others"
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, '1h'): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for the rolling window
-- - builder_name: Name of the builder (or "Others")
-- - total_profit_eth: Total profit in ETH for this time bucket (including extra transfers)
-- - builder_balance_eth: Direct builder balance increase for this time bucket
-- - extra_transfers_eth: Additional builder transfers for this time bucket
-- - avg_profit_eth: Average profit per block in ETH for this time bucket
-- - block_count: Number of MEV-Boost blocks built in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or a fixed value like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "builder_name" (this is the column to split by)
--    - Keep fields: Select "time" and "total_profit_eth" (or "avg_profit_eth")
-- 5. Each builder will become a separate line showing their profit trend
--
-- Grafana Panel Configuration:
-- - Visualization: Time series
-- - Y-axis: Label as "Profit (ETH)", set unit to "currencyUSD" or leave as number
-- - Legend: Show values (Total, Max, Last) to see cumulative profits
-- - Legend mode: Table for better readability
-- - Optional: Use "Filter data by values" to show only top N profitable builders
--
-- For Average Profit per Block:
-- - Change "Keep fields" to use "avg_profit_eth" instead of "total_profit_eth"
-- - Y-axis label: "Average Profit per Block (ETH)"
--
-- Alternative (Grafana 10+): Use "Multi-frame time series" transformation
-- This automatically groups by the builder_name column without additional configuration

WITH builder_profits AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        builder_name as builder_name,
        SUM(builder_balance_increase) as builder_balance_eth,
        SUM(COALESCE(builder_extra_transfers, 0)) as extra_transfers_eth,
        SUM(builder_balance_increase + COALESCE(builder_extra_transfers, 0)) as total_profit_eth,
        AVG(builder_balance_increase + COALESCE(builder_extra_transfers, 0)) as avg_profit_eth,
        COUNT(*) as block_count
    FROM analysis_pbs_v3
    WHERE
        $__timeFilter(block_timestamp)
        AND builder_balance_increase IS NOT NULL
        AND NOT is_block_vanilla

    GROUP BY time, builder_name
),
-- Identify top 9 builders across the entire time range
top_builders AS (
    SELECT builder_name
    FROM (
        SELECT
            builder_name,
            SUM(total_profit_eth) as total_profit
        FROM builder_profits
        WHERE builder_name != 'unknown'
        GROUP BY builder_name
        ORDER BY total_profit DESC
        LIMIT 9
    ) t
),
categorized_profits AS (
    SELECT
        bp.time,
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
        time,
        builder_name,
        SUM(builder_balance_eth) as builder_balance_eth,
        SUM(extra_transfers_eth) as extra_transfers_eth,
        SUM(total_profit_eth) as total_profit_eth,
        AVG(avg_profit_eth) as avg_profit_eth,
        SUM(block_count) as block_count
    FROM categorized_profits
    GROUP BY time, builder_name
)
SELECT
    time,
    builder_name,
    ROUND(total_profit_eth::numeric, 4) as total_profit_eth,
    ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth,
    block_count
FROM aggregated_profits
ORDER BY time, total_profit_eth DESC;
