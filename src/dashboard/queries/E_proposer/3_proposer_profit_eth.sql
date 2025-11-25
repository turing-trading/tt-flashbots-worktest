-- Proposer Profit (ETH)
-- Row: Proposer
-- Distribution of total proposer subsidies (ETH) received by each entity.
-- Shows which staking entities capture the most MEV value from builders.
--

-- Proposer Profit
-- Show a ranking of proposer entities sorted by their total subsidies received
--
-- Proposer profit is the proposer_subsidy field - the payment from builders to proposers.
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
-- Top 9 proposers are shown individually, rest grouped as "Others"
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - proposer_name: Name of the proposer entity (or "Others")
-- - total_profit_eth: Total proposer subsidy received in ETH
-- - avg_profit_eth: Average subsidy per block in ETH
-- - block_count: Number of MEV-Boost blocks proposed

WITH proposer_profits AS (
    SELECT
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
    GROUP BY proposer_name
),
top_proposers AS (
    SELECT proposer_name
    FROM proposer_profits
    WHERE proposer_name != 'Unknown'
    ORDER BY total_profit_eth DESC
    LIMIT 9
),
categorized_profits AS (
    SELECT
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
        proposer_name,
        SUM(total_profit_eth) as total_profit_eth,
        AVG(avg_profit_eth) as avg_profit_eth,
        SUM(block_count) as block_count
    FROM categorized_profits
    GROUP BY proposer_name
)
SELECT
    proposer_name,
    ROUND(total_profit_eth::numeric, 4) as total_profit_eth
    --ROUND(avg_profit_eth::numeric, 4) as avg_profit_eth,
    --block_count
FROM aggregated_profits
ORDER BY total_profit_eth DESC;
