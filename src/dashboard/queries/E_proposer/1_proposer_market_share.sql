-- Proposer Market Share
-- Row: Proposer
-- Displays each proposer entity's share of delivered blocks.
-- Helps identify dominant staking entities and diversification across the validator ecosystem.
--

-- Proposer Market Share
-- For all MEV-Boost blocks, calculate each proposer entity's share of blocks
--
-- This query calculates the percentage of blocks proposed by each entity
-- by using the proposer_name field from the proposer_mapping table.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - proposer_name: Proposer entity name (or "Unknown")
-- - blocks_proposed: Number of blocks proposed by this entity
-- - market_share_pct: Percentage of total MEV-Boost blocks proposed

WITH proposer_counts AS (
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
        COUNT(*) as blocks_proposed
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
    GROUP BY proposer_name
),
top_proposers AS (
    SELECT proposer_name
    FROM proposer_counts
    WHERE proposer_name != 'Unknown'
    ORDER BY blocks_proposed DESC
    LIMIT 9
),
categorized_counts AS (
    SELECT
        CASE
            WHEN pc.proposer_name IN (SELECT proposer_name FROM top_proposers) THEN pc.proposer_name
            ELSE 'Others'
        END as proposer_name,
        pc.blocks_proposed
    FROM proposer_counts pc
),
aggregated_counts AS (
    SELECT
        proposer_name,
        SUM(blocks_proposed) as blocks_proposed
    FROM categorized_counts
    GROUP BY proposer_name
),
total_blocks AS (
    SELECT SUM(blocks_proposed) as total
    FROM aggregated_counts
)
SELECT
    ac.proposer_name,
    --ac.blocks_proposed,
    ROUND((ac.blocks_proposed::numeric / tb.total * 100), 2) as market_share_pct
FROM aggregated_counts ac
CROSS JOIN total_blocks tb
ORDER BY ac.blocks_proposed DESC;
