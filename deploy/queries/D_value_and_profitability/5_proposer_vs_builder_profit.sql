-- Proposer vs.  Builder Profit
-- Row: Value and Profitability
-- Breaks down MEV revenue split between proposers and builders.
-- Useful for monitoring whether builders retain too much value or if proposer rewards are healthy.
--

-- Proposer vs Builder Profit Comparison
-- Compare total profits between proposers and builders
--
-- This query aggregates:
-- - Proposer profit: Sum of proposer_subsidy (payment from builder to proposer)
-- - Builder profit: builder_balance_increase + builder_extra_transfers (only when builder_balance_increase < 0)
--   - builder_balance_increase: Direct balance increase of the builder address
--   - builder_extra_transfers: Additional transfers from known builder addresses (e.g., BuilderNet refunds)
--     Only added when builder_balance_increase is negative (loss scenario)
-- - Relay fee: Sum of relay_fee (payments to relays)
--
-- Only counts MEV-Boost blocks (where relays is not NULL).
-- Vanilla blocks (self-built by proposers) are excluded.
--
-- The percentages show the profit distribution across all MEV participants.
--

SELECT
    AVG(proposer_subsidy/total_value) as "Proposer Profit",
    AVG((builder_balance_increase + CASE WHEN builder_balance_increase < 0 THEN COALESCE(builder_extra_transfers, 0) ELSE 0 END)/total_value) as "Builder Profit",
    AVG(relay_fee/total_value) as "Relay Fee"
FROM analysis_pbs_v3
WHERE
    $__timeFilter(block_timestamp)
    AND NOT is_block_vanilla
    AND total_value > 0
