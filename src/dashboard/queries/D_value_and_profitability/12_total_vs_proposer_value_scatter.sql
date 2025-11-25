-- Total Value vs Proposer Value Scatter Plot

-- Description:
-- Creates a scatter plot showing the relationship between total block value
-- and proposer subsidy, with each point colored by builder.

-- Output columns:
-- - total_value: Total MEV value (x-axis) in ETH
-- - proposer_subsidy: Proposer subsidy (y-axis) in ETH
-- - builder_name: Builder name for color coding

SELECT
    total_value,
    proposer_subsidy,
    builder_name
FROM
    analysis_pbs
WHERE
    is_block_vanilla = FALSE
    AND total_value > 0
    AND proposer_subsidy > 0
    AND $__timeFilter(block_timestamp)
ORDER BY
    block_timestamp DESC
LIMIT 10000;
