-- Missing Blocks by Day
-- Identifies gaps in block coverage on a daily basis
--
-- This query analyzes block continuity day-by-day to identify periods
-- with missing data. Useful for detecting backfill gaps or ingestion issues.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter
--
-- Returns:
-- - date: Date (YYYY-MM-DD)
-- - first_block: First block number seen on this day
-- - last_block: Last block number seen on this day
-- - expected_blocks: Expected number of blocks (last - first + 1)
-- - actual_blocks: Actual blocks present
-- - missing_blocks: Number of missing blocks
-- - coverage_pct: Percentage of blocks present
-- - has_gaps: True if any blocks are missing
--
-- Usage:
-- - Visualize data quality over time
-- - Identify specific days needing backfill
-- - Track backfill progress

WITH daily_blocks AS (
    SELECT
        DATE(timestamp) as date,
        MIN(number) as first_block,
        MAX(number) as last_block,
        COUNT(*) as actual_blocks,
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp
    FROM blocks
    WHERE $__timeFilter(timestamp)
    GROUP BY DATE(timestamp)
)
SELECT
    date,
    first_block,
    last_block,
    (last_block - first_block + 1) as expected_blocks,
    actual_blocks,
    (last_block - first_block + 1 - actual_blocks) as missing_blocks,
    ROUND((actual_blocks::numeric / NULLIF(last_block - first_block + 1, 0) * 100), 2) as coverage_pct,
    CASE
        WHEN (last_block - first_block + 1) > actual_blocks THEN true
        ELSE false
    END as has_gaps,
    first_timestamp,
    last_timestamp
FROM daily_blocks
ORDER BY date DESC;
