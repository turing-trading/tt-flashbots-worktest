-- Missing Relay Data Count in Time Range
-- Counts blocks without any relay payload data
--
-- This query identifies blocks that have no corresponding relay data,
-- which could indicate vanilla blocks or missing relay ingestion.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter (on blocks table)
--
-- Returns:
-- - blocks_without_relays: Number of blocks with no relay payload records
--
-- Usage:
-- - Monitor relay data coverage
-- - Distinguish between vanilla blocks and missing data
-- - KPI panel for relay ingestion health

WITH blocks_in_range AS (
    SELECT
        COUNT(*) as total_blocks
    FROM blocks
    WHERE $__timeFilter(timestamp)
),
blocks_with_relays AS (
    SELECT
        COUNT(DISTINCT rp.block_number) as total_with_relays
    FROM relays_payloads rp
    INNER JOIN blocks b ON rp.block_number = b.number
    WHERE $__timeFilter(b.timestamp)
)
SELECT
    (bir.total_blocks - bwr.total_with_relays) as blocks_without_relays
FROM blocks_in_range bir, blocks_with_relays bwr;
