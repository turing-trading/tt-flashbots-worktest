-- Relay Market Share
-- Row: Relay
-- Shows the relative performance and activity of relays over time.
-- Reveals reliability issues, relay churn, or surges in usage by builders or validators.
--

-- Relay Market Share (Rolling Window)
-- Calculate each relay's share of payloads delivered over a rolling time window
--
-- This query calculates the percentage of blocks delivered by each relay
-- within configurable time buckets (e.g., hourly, daily) over the selected time range.
-- Perfect for time series visualizations showing how relay market share changes over time.
--
-- Note: When a builder submits the same block to multiple relays, that block
-- will be counted once for each relay, so the total may exceed 100%.
--
-- Variables:
-- - $__timeFilter(block_timestamp): Grafana time range filter
-- - $__timeGroup(block_timestamp, '1h'): Grafana time grouping (e.g., '1h', '1d', '1w')
--
-- Returns:
-- - time: Time bucket for the rolling window
-- - relay: Relay name
-- - blocks_delivered: Number of blocks delivered by this relay in this time bucket
-- - market_share_pct: Percentage of blocks delivered by this relay in this time bucket
--
-- Usage in Grafana (Step-by-Step):
-- 1. Set Format as: Time series
-- 2. For the $__timeGroup interval, use: $__interval or a fixed value like '1h', '6h', '1d'
-- 3. In the Transform tab, add: "Partition by values"
-- 4. Configure the transformation:
--    - Field: Select "relay" (this is the column to split by)
--    - Keep fields: Select "time" and "market_share_pct"
-- 5. Each unique relay value will become a separate line on the graph
--
-- Grafana Panel Configuration:
-- - Visualization: Time series
-- - Y-axis: Label as "Market Share (%)", set unit to "percent (0-100)"
-- - Legend: Show values (Avg, Max, Last) to see market share percentages
-- - Legend mode: List or Table for better readability
--
-- Alternative (Grafana 10+): Use "Multi-frame time series" transformation
-- This automatically groups by the relay column without additional configuration

WITH relay_blocks AS (
    SELECT
        $__timeGroup(block_timestamp, $__interval) as time,
        UNNEST(relays) as relay,
        block_number
    FROM analysis_pbs
    WHERE
        $__timeFilter(block_timestamp)
        AND NOT is_block_vanilla
),
relay_counts AS (
    SELECT
        time,
        relay,
        COUNT(*) as blocks_delivered
    FROM relay_blocks
    GROUP BY time, relay
),
total_per_time AS (
    SELECT
        time,
        SUM(blocks_delivered) as total
    FROM relay_counts
    GROUP BY time
)
SELECT
    rc.time,
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
    ROUND((rc.blocks_delivered::numeric / tpt.total * 100), 2) as market_share_pct
FROM relay_counts rc
JOIN total_per_time tpt ON rc.time = tpt.time
ORDER BY rc.time, relay;
