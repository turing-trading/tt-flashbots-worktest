-- Total Value Distribution (percent)
-- Row: Value and Profitability
-- Distribution of block values (proposer profit) for MEV-Boost vs vanilla blocks.
-- Reveals how MEV-Boost increases value, and how often negative or low-value blocks occur.
--

-- Total Value Histogram using TimescaleDB histogram() hyperfunction
-- ---------------------------------------------------------------
-- Histogram shows the distribution of:
--   total_value = builder_balance_increase + proposer_subsidy
--
-- REQUIREMENTS:
-- - TimescaleDB (with histogram hyperfunction enabled)
-- - Use total_value_histogram.sql if histogram() is unavailable
--
-- GRAFANA SETUP:
-- - Format: Table
-- - Visualization: Bar chart
-- - X-axis: bucket_min
-- - Y-axis: block_count
-- - Sort by: bucket_min ASC
--
-- CONFIGURATION:
-- Modify these defaults as needed.
WITH config AS (
    SELECT
        -0.1  :: DOUBLE PRECISION AS hist_min,
         0.1 :: DOUBLE PRECISION AS hist_max,
        200  :: INTEGER          AS hist_nbuckets
),

-- Compute histogram bins once
histogram_data AS (
    SELECT histogram(
        total_value,
        c.hist_min,
        c.hist_max,
        c.hist_nbuckets
    ) AS buckets,
    MAX(c.hist_min) AS hist_min,
    MAX(c.hist_max) AS hist_max,
    MAX(c.hist_nbuckets) AS hist_nbuckets
    FROM analysis_pbs_v3
    CROSS JOIN config c
    WHERE
        $__timeFilter(block_timestamp)
        AND is_block_vanilla
    GROUP BY c.hist_min, c.hist_max, c.hist_nbuckets
),

-- Precompute bucket boundaries
bucket_ranges AS (
    SELECT
        i AS bucket_index,
        CASE
            WHEN i = 0 THEN '< '  || hd.hist_min::text || ' ETH'
            WHEN i = hd.hist_nbuckets + 1 THEN '>= ' || hd.hist_max::text || ' ETH'
            ELSE
                ROUND(
                    (hd.hist_min + (i - 1) * (hd.hist_max - hd.hist_min) / hd.hist_nbuckets)::numeric,
                    6
                )::text
                || ' to ' ||
                ROUND(
                    (hd.hist_min + i * (hd.hist_max - hd.hist_min) / hd.hist_nbuckets)::numeric,
                    6
                )::text || ' ETH'
        END AS value_range,

        -- Sorting key for Grafana
        CASE
            WHEN i = 0 THEN hd.hist_min - (hd.hist_max - hd.hist_min)
            WHEN i = hd.hist_nbuckets + 1 THEN hd.hist_max
            ELSE hd.hist_min + (i - 1) * (hd.hist_max - hd.hist_min) / hd.hist_nbuckets
        END AS bucket_min
    FROM histogram_data hd,
    generate_series(0, (SELECT hist_nbuckets + 1 FROM config)) AS i
),

-- Extract counts for each bucket
bucket_counts AS (
    SELECT
        br.bucket_index,
        br.value_range,
        br.bucket_min,
        hd.buckets[br.bucket_index + 1] AS block_count  -- PostgreSQL arrays = 1-indexed
    FROM bucket_ranges br
    CROSS JOIN histogram_data hd
),

-- Total blocks for optional normalization
total_count AS (
    SELECT SUM(block_count) AS total
    FROM bucket_counts
)

SELECT
    ROUND(bc.bucket_min::numeric, 6) AS bucket_min,
    -- bc.block_count
    -- Uncomment to add percentages:
    ROUND((bc.block_count::numeric / tc.total * 100), 3) AS vanilla
FROM bucket_counts bc
CROSS JOIN total_count tc
ORDER BY bc.bucket_min;
