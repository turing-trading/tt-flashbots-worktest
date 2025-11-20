-- Overbid Distribution
-- Row: Value and Profitability
-- Shows how much builders are overbidding relative to block value.
-- Large overbids indicate competitive pressure among builders or strategies that prioritize winning over profitability.
--

-- Overbidding Builders Analysis
-- Identify builders that have negative net profit (after paying proposers and relays)
--
-- Overbidding occurs when the builder's net profit is negative:
-- builder_profit = total_value - proposer_subsidy - relay_fee
--
-- When builder_profit < 0, the builder lost money on the block (overbid).
--
-- The total_value field already includes the correct logic for builder_extra_transfers:
-- - builder_extra_transfers are only included when total_value would otherwise be negative
-- - This represents refunds/adjustments from known builder addresses (e.g., BuilderNet)
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks are excluded as they don't have proposer subsidies.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - builder_name: Name of the builder (or "Others" for rank >9)
-- - overbid_block_count: Number of blocks where builder had negative profit
-- - total_block_count: Total number of blocks by this builder (min 20 blocks)
-- - overbid_pct: Percentage of blocks where overbidding occurred
--
-- Usage in Grafana:
-- - Visualization: Table or Bar chart
-- - Identify which builders frequently overbid and by how much

WITH builder_overbids AS (
    SELECT
        builder_name,
        COUNT(*) FILTER (WHERE (total_value - proposer_subsidy - COALESCE(relay_fee, 0)) < 0) as overbid_block_count,
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
