-- Proposer Profit (ETH)
-- Row: Proposer
-- Tracks proposer entity profitability over time.
-- Profit spikes often correlate with MEV opportunities captured by validators.
--

-- Proposer Profit (Rolling Window)
-- Show proposer entity profit over time
--
-- Proposer profit is the proposer_subsidy field - the payment from builders to proposers.
--
-- This shows how proposer profits evolve over time per entity.
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
-- Top 9 proposers are shown individually, rest grouped as "Others"
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, '1h'): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for the rolling window
-- - proposer_name: Name of the proposer entity (or "Others")
-- - total_profit_eth: Total proposer subsidy in ETH for this time bucket
-- - avg_profit_eth: Average subsidy per block in ETH for this time bucket
-- - block_count: Number of MEV-Boost blocks proposed in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or a fixed value like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "proposer_name" (this is the column to split by)
--    - Keep fields: Select "time" and "total_profit_eth" (or "avg_profit_eth")
-- 5. Each proposer entity will become a separate line showing their profit trend
--
-- Grafana Panel Configuration:
-- - Visualization: Time series
-- - Y-axis: Label as "Profit (ETH)", set unit to "currencyUSD" or leave as number
-- - Legend: Show values (Total, Max, Last) to see cumulative profits
-- - Legend mode: Table for better readability
--
-- Alternative (Grafana 10+): Use "Multi-frame time series" transformation
-- This automatically groups by the proposer_name column without additional configuration

WITH proposer_profits AS (
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
        SUM(proposer_subsidy) as total_profit_eth,
        AVG(proposer_subsidy) as avg_profit_eth,
        COUNT(*) as block_count
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
            SUM(total_profit_eth) as total_profit
        FROM proposer_profits
        WHERE proposer_name != 'Unknown'
        GROUP BY proposer_name
        ORDER BY total_profit DESC
        LIMIT 9
    ) t
),
categorized_profits AS (
    SELECT
        pp.time,
        CASE
            WHEN pp.proposer_name IN (SELECT proposer_name FROM top_proposers) THEN pp.proposer_name
            ELSE 'Others'
        END as proposer_name,
        pp.total_profit_eth,
        pp.avg_profit_eth,
        pp.block_count
    FROM proposer_profits pp
),
aggregated_profits AS (
    SELECT
        time,
        proposer_name,
        SUM(total_profit_eth) as total_profit_eth,
        AVG(avg_profit_eth) as avg_profit_eth,
        SUM(block_count) as block_count
    FROM categorized_profits
    GROUP BY time, proposer_name
)
SELECT
    time,
    proposer_name,
    ROUND(total_profit_eth::numeric, 4) as total_profit_eth,
    ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth,
    block_count
FROM aggregated_profits
ORDER BY time, proposer_name;
