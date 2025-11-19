-- Missing Analysis Records Count in Time Range
-- Counts blocks that exist but lack analysis_pbs_v2 records
--
-- This query identifies gaps in analysis coverage by comparing blocks table
-- with analysis_pbs_v2 table within the selected time range.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter (on blocks table)
--
-- Returns:
-- - missing_analysis: Number of blocks without analysis records
--
-- Usage:
-- - Quick health check for analysis pipeline
-- - Monitor analysis backfill progress
-- - KPI panel showing analysis gaps

WITH blocks_in_range AS (
    SELECT
        COUNT(*) as total_blocks,
        MIN(number) as min_block,
        MAX(number) as max_block
    FROM blocks
    WHERE $__timeFilter(timestamp)
),
analysis_in_range AS (
    SELECT
        COUNT(*) as total_analysis
    FROM analysis_pbs_v2 a
    INNER JOIN blocks b ON a.block_number = b.number
    WHERE $__timeFilter(b.timestamp)
)
SELECT
    (bir.total_blocks - air.total_analysis) as missing_analysis
FROM blocks_in_range bir, analysis_in_range air;
