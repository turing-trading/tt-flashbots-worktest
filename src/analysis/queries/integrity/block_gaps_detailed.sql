-- Detailed Block Gaps
-- Lists specific block number ranges that are missing from the database
--
-- This query identifies exact gaps in block sequences by finding
-- discontinuities in the block number series. Very useful for targeted backfill.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter
-- - $gap_threshold: Minimum gap size to report (default: 1)
--
-- Returns:
-- - gap_start: First missing block number in gap
-- - gap_end: Last missing block number in gap
-- - gap_size: Number of consecutive missing blocks
-- - prev_block: Last block before gap
-- - next_block: First block after gap
-- - prev_timestamp: Timestamp of block before gap
-- - next_timestamp: Timestamp of block after gap
-- - time_span: Time duration of the gap
--
-- Usage:
-- - Generate targeted backfill lists
-- - Investigate specific data gaps
-- - Quality assurance after backfill operations

WITH numbered_blocks AS (
    SELECT
        number,
        timestamp,
        LEAD(number) OVER (ORDER BY number) as next_number,
        LEAD(timestamp) OVER (ORDER BY number) as next_timestamp
    FROM blocks
    WHERE $__timeFilter(timestamp)
),
gaps AS (
    SELECT
        number as prev_block,
        timestamp as prev_timestamp,
        next_number as next_block,
        next_timestamp,
        (next_number - number - 1) as gap_size,
        (number + 1) as gap_start,
        (next_number - 1) as gap_end
    FROM numbered_blocks
    WHERE next_number IS NOT NULL
        AND (next_number - number) > 1  -- Gap exists
)
SELECT
    gap_start,
    gap_end,
    gap_size,
    prev_block,
    next_block,
    prev_timestamp,
    next_timestamp,
    (next_timestamp - prev_timestamp) as time_span
FROM gaps
WHERE gap_size >= COALESCE($gap_threshold, 1)
ORDER BY gap_size DESC, gap_start ASC
LIMIT 100;
