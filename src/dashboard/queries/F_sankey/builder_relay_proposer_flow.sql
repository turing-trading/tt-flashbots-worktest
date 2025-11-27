-- Sankey flow: Builder -> Relay -> Proposer
-- Returns data showing the complete flow path with three columns as percentages
-- Filtered to top 6 builders and top 10 proposers (excluding unknown)
-- Relay names are normalized for display, proposer names are capitalized
WITH top_builders AS (
    SELECT
        builder_name
    FROM
        analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND builder_name IS NOT NULL
        AND builder_name != 'unknown'
    GROUP BY
        builder_name
    ORDER BY
        COUNT(*) DESC
    LIMIT 6
),
top_proposers AS (
    SELECT
        proposer_name
    FROM
        analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND proposer_name IS NOT NULL
    GROUP BY
        proposer_name
    ORDER BY
        COUNT(*) DESC
    LIMIT 10
),
unnested_flows AS (
    SELECT
        a.builder_name,
        UNNEST(a.relays) as relay,
        a.proposer_name
    FROM
        analysis_pbs a
    WHERE
        $__timeFilter(a.block_timestamp)
        AND a.builder_name IS NOT NULL
        AND a.builder_name != 'unknown'
        AND a.proposer_name IS NOT NULL
        AND a.relays IS NOT NULL
        AND array_length(a.relays, 1) > 0
        AND a.builder_name IN (SELECT builder_name FROM top_builders)
        AND a.proposer_name IN (SELECT proposer_name FROM top_proposers)
),
total_count AS (
    SELECT COUNT(*) as total FROM unnested_flows
)
SELECT
    builder_name as "Builder",
    CASE
        WHEN relay = 'relay-analytics.ultrasound.money' THEN 'Ultrasound'
        WHEN relay = 'bloxroute.max-profit.blxrbdn.com' THEN 'Bloxroute Max Profit'
        WHEN relay = 'bloxroute.regulated.blxrbdn.com' THEN 'Bloxroute Regulated'
        WHEN relay = 'bloxroute.ethical.blxrbdn.com' THEN 'Bloxroute Ethical'
        WHEN relay = 'boost-relay.flashbots.net' THEN 'Flashbots'
        WHEN relay = 'agnostic-relay.net' THEN 'Agnostic'
        WHEN relay = 'titanrelay.xyz' THEN 'Titan'
        WHEN relay = 'aestus.live' THEN 'Aestus'
        WHEN relay = 'builder-relay-mainnet.blocknative.com' THEN 'Blocknative'
        WHEN relay = 'mainnet-relay.securerpc.com' THEN 'Secure RPC'
        WHEN relay = 'relay.ethgas.com' THEN 'EthGas'
        WHEN relay = 'relay.edennetwork.io' THEN 'Eden Network'
        WHEN relay = 'relay.btcs.com' THEN 'BTCS'
        WHEN relay = 'relayooor.wtf' THEN 'Relayooor'
        WHEN relay = 'relay.wenmerge.com' THEN 'Wenmerge'
        ELSE relay
    END as "Relay",
    INITCAP(proposer_name) as "Proposer",
    ROUND(100.0 * COUNT(*) / NULLIF((SELECT total FROM total_count), 0), 2) as "Value"
FROM
    unnested_flows
GROUP BY
    builder_name, relay, proposer_name
ORDER BY
    COUNT(*) DESC
