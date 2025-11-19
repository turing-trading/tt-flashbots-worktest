-- Missing Analysis Records
-- Identifies blocks that exist but lack corresponding analysis_pbs records
--
-- This query helps identify which blocks have been ingested but not yet
-- analyzed. Essential for monitoring the analysis pipeline health.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter
-- - $start_date: Analysis start date (default: 2022-01-01)
--
-- Returns:
-- - missing_count: Total blocks without analysis
-- - min_missing_block: Lowest block number without analysis
-- - max_missing_block: Highest block number without analysis
-- - oldest_missing_timestamp: Timestamp of oldest unanalyzed block
-- - newest_missing_timestamp: Timestamp of newest unanalyzed block
-- - sample_missing_blocks: Array of sample block numbers (first 10)
--
-- Usage:
-- - Monitor analysis backfill progress
-- - Detect analysis pipeline failures
-- - Trigger analysis for specific block ranges

WITH missing_analysis AS (
    SELECT
        b.number,
        b.timestamp
    FROM blocks b
    LEFT JOIN analysis_pbs_v2 a ON b.number = a.block_number
    WHERE a.block_number IS NULL
        AND b.timestamp >= COALESCE($start_date, '2022-01-01'::timestamp)
        AND $__timeFilter(b.timestamp)
    ORDER BY b.number
)
SELECT
    COUNT(*) as missing_count,
    MIN(number) as min_missing_block,
    MAX(number) as max_missing_block,
    MIN(timestamp) as oldest_missing_timestamp,
    MAX(timestamp) as newest_missing_timestamp,
    ARRAY_AGG(number ORDER BY number LIMIT 10) as sample_missing_blocks
FROM missing_analysis;
