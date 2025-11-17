# Grafana SQL Queries for PBS Analysis

This directory contains SQL queries designed for Grafana dashboards to analyze Proposer-Builder Separation (PBS) metrics.

## Queries

### 1. Relay Market Share (`relay_market_share.sql`)

**Purpose**: Calculate each MEV-Boost relay's share of payloads delivered.

**Metrics**:
- `relay`: Name of the relay
- `blocks_delivered`: Number of blocks delivered by this relay
- `market_share_pct`: Percentage of total blocks delivered

**Notes**:
- A single block can be delivered by multiple relays (stored as array in `analysis_pbs.relays`)
- The query unnests the relays array to count individual relay participation

**Grafana Visualization**: Pie chart or bar chart showing relay distribution

---

### 2. Builder Market Share (`builder_market_share.sql`)

**Purpose**: Calculate the fraction of blocks produced by each builder.

**Metrics**:
- `builder_name`: Name of the builder
- `blocks_built`: Number of blocks built
- `market_share_pct`: Percentage of total blocks built

**Notes**:
- Builder names are parsed from block `extra_data` via the `builders_identifiers` mapping table
- Unknown builders are labeled as 'unknown'

**Grafana Visualization**: Pie chart, bar chart, or table panel

---

### 3. MEV-Boost Market Share (`mev_boost_market_share.sql`)

**Purpose**: Show the fraction of blocks that use MEV-Boost vs vanilla blocks.

**Metrics**:
- `block_type`: 'mev_boost' or 'vanilla'
- `block_count`: Number of blocks of this type
- `market_share_pct`: Percentage of total blocks

**Notes**:
- **Vanilla blocks**: Blocks where proposers build themselves without MEV-Boost relays
- Identified by blocks with `NULL` or empty `relays` array
- **MEV-Boost blocks**: Blocks with at least one relay in the `relays` array

**Grafana Visualization**: Pie chart or gauge showing MEV-Boost adoption rate

---

### 4. Builder Profit (`builder_profit.sql`)

**Purpose**: Rank builders by their onchain profit.

**Metrics**:
- `builder_name`: Name of the builder
- `total_profit_eth`: Total profit in ETH (sum of all balance increases)
- `avg_profit_eth`: Average profit per block in ETH
- `block_count`: Number of blocks built
- `profit_rank`: Ranking by total profit

**Notes**:
- Onchain profit is calculated as the balance difference before and after the block
- Stored in `analysis_pbs.builder_balance_increase` (in ETH, converted from Wei)
- Only includes blocks where balance increase data is available

**Grafana Visualization**: Table panel showing top builders by profit, or time series showing profit over time

---

## Data Source

All queries use the `analysis_pbs` table, which aggregates data from:
- `blocks`: Block timestamps and numbers
- `proposers_balances`: Builder balance increases (profit)
- `relays_payloads`: Relay information and proposer subsidies
- `builders_identifiers`: Builder name mappings

## Time Filtering

All queries support Grafana's time range filter using:
```sql
WHERE $__timeFilter(block_timestamp)
```

This allows dynamic time range selection in Grafana dashboards.

## Usage in Grafana

1. Add PostgreSQL data source to Grafana
2. Create a new panel
3. Select "PostgreSQL" as data source
4. Copy the SQL query from the desired `.sql` file
5. Adjust visualization type based on recommendations above
6. Set time range using Grafana's time picker

## Example Query Modification

To filter by specific builder:
```sql
WHERE
    $__timeFilter(block_timestamp)
    AND builder_name = 'beaverbuild.org'
```

To get time series data, add grouping:
```sql
SELECT
    DATE_TRUNC('hour', block_timestamp) as time,
    builder_name,
    COUNT(*) as blocks_built
FROM analysis_pbs
WHERE $__timeFilter(block_timestamp)
GROUP BY time, builder_name
ORDER BY time
```
