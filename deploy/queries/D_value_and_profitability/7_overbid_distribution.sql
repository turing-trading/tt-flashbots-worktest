-- Overbid Distribution
-- Row: Value and Profitability
-- Shows how much builders are overbidding relative to block value.
-- Large overbids indicate competitive pressure among builders or strategies that prioritize winning over profitability.
--

-- Overbidding Builders Analysis
-- Identify builders that pay proposers more than their actual onchain profit
--
-- Overbidding occurs when the builder's net profit is negative after accounting for:
-- - builder_balance_increase: Direct balance increase of the builder address
-- - builder_extra_transfers: Additional transfers from known builder addresses (e.g., BuilderNet refunds)
--   Only added when builder_balance_increase is negative (loss scenario)
--
-- Net profit = builder_balance_increase + builder_extra_transfers (when builder_balance_increase < 0)
-- When the net profit is negative, the builder overpaid relative to their profit (overbid).
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks are excluded as they don't have proposer subsidies.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder (or "Others" for rank >9)
-- - overbid_block_count: Number of blocks where overbid occurred (profit < subsidy)
-- - total_block_count: Total number of blocks by this builder
-- - overbid_pct: Percentage of blocks where overbidding occurred
--
-- Usage in Grafana:
-- - Visualization: Table or Bar chart
-- - Identify which builders frequently overbid and by how much

WITH builder_overbids AS (
    SELECT
        builder_name,
        COUNT(*) FILTER (WHERE (builder_balance_increase + CASE WHEN builder_balance_increase < 0 THEN COALESCE(builder_extra_transfers, 0) ELSE 0 END) < 0) as overbid_block_count,
        COUNT(*) as total_block_count
    FROM analysis_pbs_v3
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
    LIMIT 5
),
categorized_overbids AS (
    SELECT
        CASE
            WHEN bo.builder_name IN (SELECT builder_name FROM top_builders) THEN bo.builder_name
            ELSE 'Others'
        END as builder_name,
        bo.overbid_block_count,
        bo.total_block_count
    FROM builder_overbids bo
),
aggregated_overbids AS (
    SELECT
        builder_name,
        SUM(overbid_block_count) as overbid_block_count,
        SUM(total_block_count) as total_block_count
    FROM categorized_overbids
    GROUP BY builder_name
)
SELECT
    builder_name,
    --overbid_block_count,
    --total_block_count,
    ROUND((overbid_block_count::numeric / NULLIF(total_block_count, 0) * 100), 2) as overbid_pct
FROM aggregated_overbids
WHERE builder_name!='Others' AND total_block_count>20
ORDER BY overbid_pct DESC;
