-- Proposer Market Share
-- Row: Proposer
-- Shows the relative block production by proposer entities over time.
-- Reveals validator ecosystem changes, staking pool growth, and network decentralization trends.
--

-- Proposer Market Share (Rolling Window)
-- Calculate each proposer entity's share of blocks over a rolling time window
--
-- This query calculates the percentage of blocks proposed by each entity
-- within configurable time buckets (e.g., hourly, daily) over the selected time range.
-- Perfect for time series visualizations showing how proposer market share changes over time.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, '1h'): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for the rolling window
-- - proposer_name: Proposer entity name (or "Others")
-- - blocks_proposed: Number of blocks proposed by this entity in this time bucket
-- - market_share_pct: Percentage of blocks proposed by this entity in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or a fixed value like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "proposer_name" (this is the column to split by)
--    - Keep fields: Select "time" and "market_share_pct"
-- 5. Each unique proposer entity will become a separate line on the graph
--
-- Grafana Panel Configuration:
-- - Visualization: Time series
-- - Y-axis: Label as "Market Share (%)", set unit to "percent (0-100)"
-- - Legend: Show values (Avg, Max, Last) to see market share percentages
-- - Legend mode: List or Table for better readability
--
-- Alternative (Grafana 10+): Use "Multi-frame time series" transformation
-- This automatically groups by the proposer_name column without additional configuration

WITH proposer_counts AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        CASE
            WHEN proposer_name IS NULL THEN 'Unknown'
            WHEN proposer_name = 'lido' THEN 'Lido'
            WHEN proposer_name = 'coinbase' THEN 'Coinbase'
            WHEN proposer_name = 'binance' THEN 'Binance'
            WHEN proposer_name = 'kraken' THEN 'Kraken'
            WHEN proposer_name = 'ether.fi' THEN 'Ether.fi'
            WHEN proposer_name = 'okx' THEN 'OKX'
            WHEN proposer_name = 'everstake' THEN 'Everstake'
            WHEN proposer_name = 'bitcoin suisse' THEN 'Bitcoin Suisse'
            WHEN proposer_name = 'rocketpool' THEN 'Rocketpool'
            WHEN proposer_name = 'stakefish' THEN 'Stakefish'
            WHEN proposer_name = 'kiln' THEN 'Kiln'
            WHEN proposer_name = 'figment' THEN 'Figment'
            WHEN proposer_name = 'staked.us' THEN 'Staked.us'
            WHEN proposer_name = 'p2p.org' THEN 'P2P.org'
            WHEN proposer_name = 'mantle' THEN 'Mantle'
            WHEN proposer_name = 'renzo' THEN 'Renzo'
            WHEN proposer_name = 'swell' THEN 'Swell'
            WHEN proposer_name = 'frax finance' THEN 'Frax Finance'
            WHEN proposer_name = 'kelp dao' THEN 'Kelp DAO'
            WHEN proposer_name = 'liquid collective' THEN 'Liquid Collective'
            WHEN proposer_name = 'stader' THEN 'Stader'
            WHEN proposer_name = 'puffer' THEN 'Puffer'
            WHEN proposer_name = 'abyss' THEN 'Abyss'
            ELSE INITCAP(proposer_name)
        END as proposer_name,
        COUNT(*) as blocks_proposed
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
    GROUP BY time, proposer_name
),
-- Identify top 9 proposers across the entire time range
top_proposers AS (
    SELECT proposer_name
    FROM (
        SELECT
            proposer_name,
            SUM(blocks_proposed) as total_blocks
        FROM proposer_counts
        WHERE proposer_name != 'Unknown'
        GROUP BY proposer_name
        ORDER BY total_blocks DESC
        LIMIT 9
    ) t
),
categorized_counts AS (
    SELECT
        pc.time,
        CASE
            WHEN pc.proposer_name IN (SELECT proposer_name FROM top_proposers) THEN pc.proposer_name
            ELSE 'Others'
        END as proposer_name,
        pc.blocks_proposed
    FROM proposer_counts pc
),
aggregated_counts AS (
    SELECT
        time,
        proposer_name,
        SUM(blocks_proposed) as blocks_proposed
    FROM categorized_counts
    GROUP BY time, proposer_name
),
total_per_time AS (
    SELECT
        time,
        SUM(blocks_proposed) as total
    FROM aggregated_counts
    GROUP BY time
)
SELECT
    ac.time,
    ac.proposer_name,
    --ac.blocks_proposed,
    ROUND((ac.blocks_proposed::numeric / tpt.total * 100), 2) as market_share_pct
FROM aggregated_counts ac
JOIN total_per_time tpt ON ac.time = tpt.time
ORDER BY ac.time, ac.proposer_name;
