-- Proposer vs.  Builder Profit
-- Row: Value and Profitability
-- Breaks down MEV revenue split between proposers and builders.
-- Useful for monitoring whether builders retain too much value or if proposer rewards are healthy.
--

-- Proposer vs Builder Profit Comparison
-- Compare total profits between proposers and builders
--
-- This query shows the profit distribution across MEV participants:
-- - Proposer profit: proposer_subsidy (payment from builder to proposer)
-- - Builder profit: total_value - proposer_subsidy - relay_fee
-- - Relay fee: relay_fee (payments to relays)
--
-- The total_value field already includes the correct logic for builder_extra_transfers:
-- - builder_extra_transfers are only included when total_value would otherwise be negative
-- - This represents refunds/adjustments from known builder addresses (e.g., BuilderNet)
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
--
-- The percentages show the profit distribution across all MEV participants and sum to ~100%.
--

SELECT
    AVG(pct_proposer_share) as "Proposer Profit",
    AVG(pct_builder_share) as "Builder Profit",
    AVG(pct_relay_fee) as "Relay Fee"
FROM analysis_pbs
WHERE
    $__timeFilter(block_timestamp)
    AND NOT is_block_vanilla
    AND total_value > 0
