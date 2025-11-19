-- Hourly Block Ingestion Rate
-- Tracks block ingestion over time to detect slowdowns or gaps
--
-- This query helps identify periods where block ingestion was slow or stopped,
-- useful for detecting historical data pipeline issues.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter
--
-- Returns:
-- - hour: Hour timestamp (truncated to hour)
-- - blocks_ingested: Number of blocks in this hour
-- - expected_blocks: Expected blocks (~300 blocks/hour for 12s block time)
-- - missing_blocks: Estimated missing blocks
-- - ingestion_rate_pct: Percentage of expected ingestion rate
-- - is_slow: True if ingestion rate is below 80%
--
-- Usage:
-- - Visualize ingestion performance over time
-- - Detect periods needing backfill
-- - Monitor live ingestion health

WITH hourly_blocks AS (
    SELECT
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as blocks_ingested,
        MIN(number) as min_block,
        MAX(number) as max_block
    FROM blocks
    WHERE $__timeFilter(timestamp)
    GROUP BY DATE_TRUNC('hour', timestamp)
)
SELECT
    hour,
    blocks_ingested,
    300 as expected_blocks,  -- ~300 blocks per hour (12 second block time)
    GREATEST(0, 300 - blocks_ingested) as missing_blocks,
    ROUND((blocks_ingested::numeric / 300.0 * 100), 2) as ingestion_rate_pct,
    min_block,
    max_block,
    CASE
        WHEN blocks_ingested < 240 THEN true  -- Less than 80% of expected
        ELSE false
    END as is_slow
FROM hourly_blocks
ORDER BY hour DESC;
