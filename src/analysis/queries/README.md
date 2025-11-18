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

### `relay/`
Relay data quality and gap detection queries:
- `daily_coverage_stats.sql` - Daily block counts per relay with outlier detection for gap analysis

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

## Gap Detection and Retry

The `relay/daily_coverage_stats.sql` query supports a gap detection workflow for identifying and filling missing data from relays (e.g., titanrelay.xyz).

### Workflow

1. **Analyze gaps** (read-only):
   ```bash
   poetry run python src/data/relays/analyze_gaps.py
   ```
   - Runs the `daily_coverage_stats.sql` query
   - Identifies days where relay coverage is abnormally low (outliers)
   - Groups by day and detects statistical outliers (<50% of average or <2 stddev below mean)
   - Consolidates adjacent gaps to reduce API calls
   - Saves gaps to `relay_gaps.json`

2. **Review gaps**:
   ```bash
   cat relay_gaps.json
   ```

3. **Retry filling gaps**:
   ```bash
   poetry run python src/data/relays/retry_gaps.py
   ```
   - Reads gaps from `relay_gaps.json`
   - Fetches missing data from relay APIs for specific slot ranges
   - Uses exponential backoff retry logic (5 attempts)
   - Updates `relays_payloads` table with recovered data

### Helper Functions

The gap detection system uses helper functions in `src/data/relays/gap_detection.py`:
- `timestamp_to_slot()` / `slot_to_timestamp()` - Convert between dates and beacon chain slots
- `detect_outliers()` - Statistical outlier detection
- `consolidate_gaps()` - Merge adjacent gap ranges
- `estimate_missing_blocks()` - Calculate expected blocks in a slot range
