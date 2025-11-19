-- Data Quality Summary Dashboard
-- Comprehensive overview of data integrity across all tables
--
-- This query provides a high-level health check of the entire data pipeline,
-- showing coverage and completeness metrics for blocks, analysis, relays, and proposers.
--
-- Variables:
-- - $__timeFilter(timestamp): Grafana time range filter (on blocks table)
--
-- Returns:
-- - metric_name: Name of the metric being measured
-- - total_records: Total count for this metric
-- - coverage_pct: Coverage percentage where applicable
-- - status: Health status (OK, WARNING, CRITICAL)
-- - details: Additional context or recommendations
--
-- Usage:
-- - Single-pane health check dashboard
-- - Automated monitoring and alerting
-- - Data quality reporting

WITH block_stats AS (
    SELECT
        COUNT(*) as total_blocks,
        MIN(number) as min_block,
        MAX(number) as max_block,
        (MAX(number) - MIN(number) + 1) as expected_blocks
    FROM blocks
    WHERE $__timeFilter(timestamp)
),
analysis_stats AS (
    SELECT
        COUNT(*) as total_analysis
    FROM analysis_pbs_v2 a
    INNER JOIN blocks b ON a.block_number = b.number
    WHERE $__timeFilter(b.timestamp)
),
relay_stats AS (
    SELECT
        COUNT(DISTINCT block_number) as blocks_with_relays
    FROM relays_payloads rp
    INNER JOIN blocks b ON rp.block_number = b.number
    WHERE $__timeFilter(b.timestamp)
),
proposer_stats AS (
    SELECT
        COUNT(*) as total_proposer_balances
    FROM proposers_balance pb
    INNER JOIN blocks b ON pb.block_number = b.number
    WHERE $__timeFilter(b.timestamp)
),
metrics AS (
    SELECT
        'Block Coverage' as metric_name,
        bs.total_blocks as total_records,
        ROUND((bs.total_blocks::numeric / NULLIF(bs.expected_blocks, 0) * 100), 2) as coverage_pct,
        bs.expected_blocks - bs.total_blocks as gap_count,
        CASE
            WHEN bs.total_blocks = bs.expected_blocks THEN 'OK'
            WHEN (bs.total_blocks::numeric / NULLIF(bs.expected_blocks, 0)) > 0.99 THEN 'WARNING'
            ELSE 'CRITICAL'
        END as status
    FROM block_stats bs

    UNION ALL

    SELECT
        'Analysis Coverage' as metric_name,
        a_stats.total_analysis as total_records,
        ROUND((a_stats.total_analysis::numeric / NULLIF(bs.total_blocks, 0) * 100), 2) as coverage_pct,
        bs.total_blocks - a_stats.total_analysis as gap_count,
        CASE
            WHEN a_stats.total_analysis = bs.total_blocks THEN 'OK'
            WHEN (a_stats.total_analysis::numeric / NULLIF(bs.total_blocks, 0)) > 0.99 THEN 'WARNING'
            ELSE 'CRITICAL'
        END as status
    FROM analysis_stats a_stats, block_stats bs

    UNION ALL

    SELECT
        'Relay Data Coverage' as metric_name,
        r_stats.blocks_with_relays as total_records,
        ROUND((r_stats.blocks_with_relays::numeric / NULLIF(bs.total_blocks, 0) * 100), 2) as coverage_pct,
        NULL as gap_count,
        CASE
            WHEN (r_stats.blocks_with_relays::numeric / NULLIF(bs.total_blocks, 0)) > 0.80 THEN 'OK'
            WHEN (r_stats.blocks_with_relays::numeric / NULLIF(bs.total_blocks, 0)) > 0.50 THEN 'WARNING'
            ELSE 'CRITICAL'
        END as status
    FROM relay_stats r_stats, block_stats bs

    UNION ALL

    SELECT
        'Proposer Balance Coverage' as metric_name,
        p_stats.total_proposer_balances as total_records,
        ROUND((p_stats.total_proposer_balances::numeric / NULLIF(bs.total_blocks, 0) * 100), 2) as coverage_pct,
        bs.total_blocks - p_stats.total_proposer_balances as gap_count,
        CASE
            WHEN p_stats.total_proposer_balances = bs.total_blocks THEN 'OK'
            WHEN (p_stats.total_proposer_balances::numeric / NULLIF(bs.total_blocks, 0)) > 0.95 THEN 'WARNING'
            ELSE 'CRITICAL'
        END as status
    FROM proposer_stats p_stats, block_stats bs
)
SELECT
    metric_name,
    total_records,
    coverage_pct,
    status,
    CASE
        WHEN metric_name = 'Block Coverage' AND gap_count > 0 THEN
            FORMAT('Missing %s blocks - run block backfill', gap_count)
        WHEN metric_name = 'Analysis Coverage' AND gap_count > 0 THEN
            FORMAT('Missing %s analysis records - run analysis backfill', gap_count)
        WHEN status = 'OK' THEN
            'All systems operational'
        ELSE
            'Check detailed queries for specifics'
    END as details
FROM metrics
ORDER BY
    CASE status
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING' THEN 2
        WHEN 'OK' THEN 3
    END,
    metric_name;
