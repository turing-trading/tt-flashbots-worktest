# Grafana SQL Queries for PBS Analysis

This directory contains SQL queries designed for Grafana dashboards to analyze Proposer-Builder Separation (PBS) metrics.

## Query Types

Each metric has two versions:
- **Static** (`*.sql`): Aggregate metrics for the selected time range (pie charts, tables)
- **Rolling** (`*_rolling.sql`): Time series data showing trends over time (line charts, area charts)

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

### 5. Relay Market Share - Rolling (`relay_market_share_rolling.sql`)

**Purpose**: Track how relay market share changes over time.

**Metrics**:
- `time`: Time bucket (configurable interval)
- `relay`: Relay name
- `blocks_delivered`: Number of blocks in this time bucket
- `market_share_pct`: Market share percentage for this time bucket

**Grafana Transformation**: Use "Partition by values" on the `relay` field to create one line per relay

**Grafana Visualization**: Time series line chart showing relay adoption trends

---

### 6. Builder Market Share - Rolling (`builder_market_share_rolling.sql`)

**Purpose**: Track how builder market share changes over time (MEV-Boost blocks only).

**Metrics**:
- `time`: Time bucket (configurable interval)
- `builder_name`: Builder name
- `blocks_built`: Number of blocks in this time bucket
- `market_share_pct`: Market share percentage for this time bucket

**Grafana Transformation**: Use "Partition by values" on the `builder_name` field to create one line per builder

**Grafana Visualization**: Time series line chart showing builder dominance trends

---

### 7. MEV-Boost Market Share - Rolling (`mev_boost_market_share_rolling.sql`)

**Purpose**: Track MEV-Boost adoption rate over time.

**Metrics**:
- `time`: Time bucket (configurable interval)
- `block_type`: 'mev_boost' or 'vanilla'
- `block_count`: Number of blocks in this time bucket
- `market_share_pct`: Percentage for this time bucket

**Grafana Transformation**: Use "Partition by values" on the `block_type` field to create two lines

**Grafana Visualization**: Time series area chart (stacked) showing MEV-Boost vs vanilla adoption

---

### 8. Builder Profit - Rolling (`builder_profit_rolling.sql`)

**Purpose**: Track builder profits over time.

**Metrics**:
- `time`: Time bucket (configurable interval)
- `builder_name`: Builder name
- `total_profit_eth`: Total profit in this time bucket
- `avg_profit_eth`: Average profit per block in this time bucket
- `block_count`: Number of blocks built

**Grafana Transformation**: Use "Partition by values" on the `builder_name` field, keep `total_profit_eth` or `avg_profit_eth`

**Grafana Visualization**: Time series line chart showing profit trends per builder

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

### For Static Queries (snapshots)
1. Add PostgreSQL data source to Grafana
2. Create a new panel
3. Select "PostgreSQL" as data source
4. Copy the SQL query from the desired `.sql` file
5. Set **Format as**: Table or Time series (depending on visualization)
6. Adjust visualization type based on recommendations above
7. Set time range using Grafana's time picker

### For Rolling Queries (time series)
1. Add PostgreSQL data source to Grafana
2. Create a new panel with Time series visualization
3. Select "PostgreSQL" as data source
4. Copy the SQL query from the desired `*_rolling.sql` file
5. Set **Format as**: Time series
6. **Add Transformation**:
   - Click "Transform" tab
   - Add "Partition by values"
   - Select the grouping field (e.g., `relay`, `builder_name`, `block_type`)
   - Keep the time and value fields
7. Configure the panel (Y-axis labels, legend, etc.)
8. Set time range using Grafana's time picker

### Transformation Guide

**"Partition by values" Transformation Steps:**
1. Transform tab → "+ Add transformation"
2. Select: "Partition by values"
3. Configure:
   - **Field**: The column to split by (relay, builder_name, block_type)
   - **Keep fields**: Select `time` and the metric column (market_share_pct, total_profit_eth, etc.)
4. Each unique value in the field becomes a separate line

**Alternative (Grafana 10+):** Use "Multi-frame time series" transformation for automatic grouping

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
