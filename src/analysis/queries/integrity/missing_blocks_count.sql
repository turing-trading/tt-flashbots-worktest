-- Missing Blocks Count in Time Range
-- Counts the number of missing blocks between min and max block numbers in a time range
--
-- This query identifies gaps in block coverage by comparing expected blocks
-- (continuous sequence) with actual blocks in the database.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter
--
-- Returns:
-- - total_expected: Total blocks expected in range (max - min + 1)
-- - total_actual: Actual blocks present in database
-- - missing_count: Number of missing blocks
-- - coverage_pct: Percentage of blocks present
-- - min_block: Lowest block number in range
-- - max_block: Highest block number in range
--
-- Usage:
-- - Quick health check for block ingestion
-- - Identify if backfill is needed

WITH block_range AS (
    SELECT
        MIN(number) as min_block,
        MAX(number) as max_block,
        COUNT(*) as total_actual,
        MIN(timestamp) as min_timestamp,
        MAX(timestamp) as max_timestamp
    FROM blocks
    WHERE $__timeFilter(timestamp)
),
expected AS (
    SELECT
        br.min_block,
        br.max_block,
        br.total_actual,
        br.min_timestamp,
        br.max_timestamp,
        (br.max_block - br.min_block + 1) as total_expected
    FROM block_range br
)
SELECT
    total_expected,
    total_actual,
    (total_expected - total_actual) as missing_count,
    ROUND((total_actual::numeric / NULLIF(total_expected, 0) * 100), 2) as coverage_pct,
    min_block,
    max_block,
    min_timestamp,
    max_timestamp
FROM expected;
