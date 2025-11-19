-- Missing Proposer Balance Records Count in Time Range
-- Counts blocks that exist but lack proposers_balance records
--
-- This query identifies gaps in proposer balance tracking by comparing
-- blocks table with proposers_balance table within the selected time range.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter (on blocks table)
--
-- Returns:
-- - missing_proposer_balance: Number of blocks without proposer balance records
--
-- Usage:
-- - Monitor proposer balance backfill progress
-- - KPI panel showing balance tracking gaps

WITH blocks_in_range AS (
    SELECT
        COUNT(*) as total_blocks
    FROM blocks
    WHERE $__timeFilter(timestamp)
),
balance_in_range AS (
    SELECT
        COUNT(*) as total_balance
    FROM proposers_balance pb
    INNER JOIN blocks b ON pb.block_number = b.number
    WHERE $__timeFilter(b.timestamp)
)
SELECT
    (bir.total_blocks - bir_balance.total_balance) as missing_proposer_balance
FROM blocks_in_range bir, balance_in_range bir_balance;
