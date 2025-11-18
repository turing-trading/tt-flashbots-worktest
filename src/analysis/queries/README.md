# Analysis Queries

SQL queries for Grafana dashboards analyzing PBS (Proposer-Builder Separation) data.

## Folder Structure

### `builder/`
Builder-specific queries:
- `market_share.sql` - Builder market share distribution
- `market_share_rolling.sql` - Builder market share over time
- `profit.sql` - Builder profit analysis and ranking
- `profit_rolling.sql` - Builder profit trends over time
- `overbidding.sql` - Builders that pay proposers more than their profit

### `profit/`
Profit analysis queries:
- `avg_by_block_type.sql` - Average total value by MEV-Boost vs vanilla blocks
- `negative_blocks_pct.sql` - Percentage of blocks with negative total value
- `proposer_vs_builder.sql` - Profit comparison between proposers and builders
- `proposer_vs_builder_rolling.sql` - Proposer vs builder profit trends over time

### `market_share/`
Market share queries:
- `mev_boost.sql` - MEV-Boost vs vanilla block distribution
- `mev_boost_rolling.sql` - MEV-Boost adoption over time
- `relay.sql` - Relay market share distribution
- `relay_rolling.sql` - Relay market share trends over time

### `distribution/`
Value distribution queries:
- `total_value_histogram_timescale.sql` - Histogram of total MEV value using TimescaleDB

## Database Schema

All queries use the `analysis_pbs_v2` table with the following key fields:

- `block_number` (int) - Block number
- `block_timestamp` (timestamp) - Block timestamp
- `builder_balance_increase` (float) - Builder profit in ETH
- `proposer_subsidy` (float) - Payment to proposer in ETH
- `total_value` (float) - Pre-computed `builder_balance_increase + proposer_subsidy`
- `is_block_vanilla` (bool) - `true` for vanilla blocks, `false` for MEV-Boost
- `n_relays` (int) - Number of relays used
- `relays` (array) - List of relay names
- `builder_name` (string) - Builder name (defaults to 'unknown')

## Grafana Variables

All queries support these Grafana variables:
- `$__timeFilter(block_timestamp)` - Time range filter
- `$__timeGroup(block_timestamp, $__interval)` - Time grouping for rolling queries
