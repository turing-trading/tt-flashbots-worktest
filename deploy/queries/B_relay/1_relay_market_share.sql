-- Relay Market Share
-- Row: Relay
-- Displays each relay’s share of delivered blocks.
-- Helps identify dominant relays and diversification across the relay ecosystem.
--

-- Relay Market Share
-- For all mev-boost relays, calculate each relay's share of payloads delivered
--
-- This query calculates the percentage of blocks delivered by each relay
-- by unnesting the relays array and counting occurrences.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
--
-- Returns:
-- - relay: Relay name
-- - blocks_delivered: Number of blocks delivered by this relay
-- - market_share_pct: Percentage of total blocks delivered

WITH relay_blocks AS (
    SELECT
        UNNEST(relays) as relay,
        block_number
    FROM analysis_pbs_v3
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
),
relay_counts AS (
    SELECT
        relay,
        COUNT(*) as blocks_delivered
    FROM relay_blocks
    GROUP BY relay
),
total_blocks AS (
    SELECT SUM(blocks_delivered) as total
    FROM relay_counts
)
SELECT
    CASE
        WHEN rc.relay = 'relay-analytics.ultrasound.money' THEN 'Ultrasound'
        WHEN rc.relay = 'bloxroute.max-profit.blxrbdn.com' THEN 'Bloxroute Max Profit'
        WHEN rc.relay = 'titanrelay.xyz' THEN 'Titan'
        WHEN rc.relay = 'bloxroute.regulated.blxrbdn.com' THEN 'Bloxroute Regulated'
        WHEN rc.relay = 'aestus.live' THEN 'Aestus'
        WHEN rc.relay = 'agnostic-relay.net' THEN 'Agnostic'
        WHEN rc.relay = 'boost-relay.flashbots.net' THEN 'Flashbots'
        WHEN rc.relay = 'relay.ethgas.com' THEN 'EthGas'
        WHEN rc.relay = 'relay.btcs.com' THEN 'BTCS'
        WHEN rc.relay = 'mainnet-relay.securerpc.com' THEN 'Secure RPC'
        WHEN rc.relay = 'relay.wenmerge.com' THEN 'Wenmerge'
        ELSE rc.relay
    END as relay,
    --rc.blocks_delivered,
    ROUND((rc.blocks_delivered::numeric / tb.total * 100), 2) as market_share_pct
FROM relay_counts rc
CROSS JOIN total_blocks tb
ORDER BY relay;
