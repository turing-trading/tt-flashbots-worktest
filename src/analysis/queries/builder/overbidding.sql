-- Overbidding Builders Analysis
-- Identify builders that pay proposers more than their actual onchain profit
--
-- Overbidding occurs when the proposer_subsidy (payment to proposer) exceeds
-- the builder_balance_increase (actual profit). This results in a net loss
-- for the builder on that block.
--
-- Overbid amount = proposer_subsidy - builder_balance_increase
-- When positive, the builder overpaid relative to their profit.
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks are excluded as they don't have proposer subsidies.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder (or "Others" for rank >9)
-- - avg_overbid_eth: Average overbid amount per block in ETH (can be negative)
-- - total_overbid_eth: Total overbid amount in ETH
-- - overbid_block_count: Number of blocks where overbid occurred (subsidy > profit)
-- - total_block_count: Total number of blocks by this builder
-- - overbid_pct: Percentage of blocks where overbidding occurred
--
-- Usage in Grafana:
-- - Visualization: Table or Bar chart
-- - Identify which builders frequently overbid and by how much

WITH builder_overbids AS (
    SELECT
        builder_name,
        AVG(proposer_subsidy - builder_balance_increase) as avg_overbid_eth,
        SUM(proposer_subsidy - builder_balance_increase) as total_overbid_eth,
        COUNT(*) FILTER (WHERE proposer_subsidy > builder_balance_increase) as overbid_block_count,
        COUNT(*) as total_block_count
    FROM analysis_pbs_v2
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
    GROUP BY builder_name
),
top_builders AS (
    SELECT builder_name
    FROM builder_overbids
    WHERE builder_name != 'unknown'
    ORDER BY total_block_count DESC
    LIMIT 9
),
categorized_overbids AS (
    SELECT
        CASE
            WHEN bo.builder_name IN (SELECT builder_name FROM top_builders) THEN bo.builder_name
            ELSE 'Others'
        END as builder_name,
        bo.avg_overbid_eth,
        bo.total_overbid_eth,
        bo.overbid_block_count,
        bo.total_block_count
    FROM builder_overbids bo
),
aggregated_overbids AS (
    SELECT
        builder_name,
        AVG(avg_overbid_eth) as avg_overbid_eth,
        SUM(total_overbid_eth) as total_overbid_eth,
        SUM(overbid_block_count) as overbid_block_count,
        SUM(total_block_count) as total_block_count
    FROM categorized_overbids
    GROUP BY builder_name
)
SELECT
    builder_name,
    ROUND(avg_overbid_eth::numeric, 4) as avg_overbid_eth,
    ROUND(total_overbid_eth::numeric, 4) as total_overbid_eth,
    overbid_block_count,
    total_block_count,
    ROUND((overbid_block_count::numeric / NULLIF(total_block_count, 0) * 100), 2) as overbid_pct
FROM aggregated_overbids
ORDER BY avg_overbid_eth DESC;
