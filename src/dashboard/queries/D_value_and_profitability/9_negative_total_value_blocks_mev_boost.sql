-- Negative Total Value Blocks (MEV-boost)
-- Row: Value and Profitability
-- Counts negative-value blocks per builder.
-- Useful for identifying risky or inefficient builders and relay-builder mismatches.
--

WITH builder_negative_counts AS (
    SELECT
        builder_name,
        COUNT(*) AS negative_block_count
    FROM analysis_pbs_v3 
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
        AND total_value < 0
    GROUP BY builder_name
),
total_negative AS (
    SELECT SUM(negative_block_count) AS total
    FROM builder_negative_counts
),
pct_table AS (
    SELECT
        bnc.builder_name,
        ROUND((bnc.negative_block_count::numeric / tn.total * 100), 2) AS pct_of_negative
    FROM builder_negative_counts bnc
    CROSS JOIN total_negative tn
)
SELECT *
FROM pct_table
WHERE pct_of_negative > 5
ORDER BY pct_of_negative DESC;
