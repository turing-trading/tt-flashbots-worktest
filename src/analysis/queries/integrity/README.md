# Data Integrity Queries

Grafana queries for monitoring data completeness and quality across the MEV-Boost data pipeline.

## Overview

These queries help identify:
- Missing blocks in the database
- Gaps in block sequences
- Incomplete analysis coverage
- Data pipeline health issues
- Backfill requirements

## Queries

### Block Integrity Queries

### 1. `missing_blocks_count.sql`
**Quick health check** - Single-number summary of missing blocks

**Use Case:** Dashboard KPI panel showing total missing blocks
**Returns:** Total missing blocks, coverage percentage, block range
**Time to Run:** <1 second

**Example Output:**
```
total_expected | total_actual | missing_count | coverage_pct | min_block | max_block
-----------------------------------------------------------------------------
     1,000,000 |      998,547 |         1,453 |        99.85 | 15000000  | 16000000
```

---

### 2. `missing_blocks_daily.sql`
**Daily breakdown** - Identifies which days have missing blocks

**Use Case:** Time series visualization to pinpoint problem periods
**Returns:** Daily statistics with gap counts and coverage percentages
**Time to Run:** 1-3 seconds
**Visualization:** Line chart or heatmap

**Example Output:**
```
date       | missing_blocks | coverage_pct | has_gaps
-------------------------------------------------------
2024-01-15 |            123 |        98.95 | true
2024-01-14 |              0 |       100.00 | false
2024-01-13 |             45 |        99.62 | true
```

---

### 3. `block_gaps_detailed.sql`
**Detailed gap analysis** - Lists specific block number ranges missing

**Use Case:** Generate targeted backfill lists, investigate specific gaps
**Returns:** Exact start/end block numbers for each gap
**Time to Run:** 2-5 seconds
**Limit:** Top 100 largest gaps

**Parameters:**
- `$gap_threshold`: Minimum gap size to report (default: 1)

**Example Output:**
```
gap_start | gap_end  | gap_size | prev_block | next_block | time_span
------------------------------------------------------------------------
15234567  | 15234789 |      223 |   15234566 |   15234790 | 00:44:36
15450123  | 15450234 |      112 |   15450122 |   15450235 | 00:22:24
```

**Backfill Command:**
```bash
# Use gap_start and gap_end to backfill specific ranges
python -m src.data.blocks.backfill --start 15234567 --end 15234789
```

---

### 4. `missing_analysis_records.sql`
**Analysis pipeline health** - Blocks ingested but not analyzed

**Use Case:** Monitor analysis backfill progress, detect pipeline failures
**Returns:** Count and samples of blocks without analysis
**Time to Run:** 1-2 seconds

**Parameters:**
- `$start_date`: Analysis start date (default: 2022-01-01)

**Example Output:**
```
missing_count | min_missing_block | max_missing_block | sample_missing_blocks
------------------------------------------------------------------------------
        5,432 |          15234567 |          15456789 | {15234567, 15234568, ...}
```

---

### 5. `data_quality_summary.sql`
**Comprehensive dashboard** - Single-pane health check

**Use Case:** Main monitoring dashboard, automated alerts
**Returns:** Status for blocks, analysis, relays, proposers
**Time to Run:** 2-4 seconds
**Visualization:** Table with colored status indicators

**Example Output:**
```
metric_name              | total_records | coverage_pct | status   | details
----------------------------------------------------------------------------------
Block Coverage           |       998,547 |        99.85 | WARNING  | Missing 1453 blocks
Analysis Coverage        |       998,000 |        99.95 | OK       | All systems operational
Relay Data Coverage      |       856,234 |        85.75 | OK       | All systems operational
Proposer Balance Coverage|       997,123 |        99.86 | OK       | All systems operational
```

**Grafana Alert Example:**
```yaml
# Alert if any metric shows CRITICAL status
WHEN last_value("status") = "CRITICAL"
```

---

### 6. `hourly_ingestion_rate.sql`
**Performance monitoring** - Tracks ingestion speed over time

**Use Case:** Detect slowdowns, monitor live ingestion
**Returns:** Hourly block counts vs expected rate
**Time to Run:** 1-3 seconds
**Visualization:** Line chart with threshold bands

**Example Output:**
```
hour                | blocks_ingested | expected_blocks | missing_blocks | ingestion_rate_pct | is_slow
-------------------------------------------------------------------------------------------------------
2024-01-15 14:00:00 |             298 |             300 |              2 |              99.33 | false
2024-01-15 13:00:00 |             156 |             300 |            144 |              52.00 | true
2024-01-15 12:00:00 |             300 |             300 |              0 |             100.00 | false
```

**Note:** Expected rate assumes 12-second block time (~300 blocks/hour)

---

### Relay Data Integrity Queries

### 7. `blocks_per_relay_timeseries.sql`
**Volume tracking** - Total blocks delivered by each relay over time

**Use Case:** Monitor relay activity levels, compare relay performance
**Returns:** Time-bucketed block counts per relay
**Time to Run:** 2-4 seconds
**Visualization:** Time series (line or stacked area chart)

**Grafana Setup:**
```yaml
Format: Time series
Transform: Partition by values → field: "relay"
Y-axis: Blocks Delivered
Legend: Table mode with Min/Max/Mean
Stack: Enable for stacked area chart
```

**Example Output:**
```
time                | relay                    | blocks_delivered
------------------------------------------------------------------
2024-01-15 12:00:00 | agnostic-relay.net       |             145
2024-01-15 12:00:00 | bloxroute.max.blxrbdn.com|             128
2024-01-15 12:00:00 | flashbots                |             156
```

**Visualization Options:**
- **Line Chart**: Individual relay trends
- **Stacked Area**: Total relay capacity over time
- **Bar Chart (Stacked)**: Daily relay contribution

---

### 8. `relay_availability_timeseries.sql`
**Uptime monitoring** - Track which relays are active in each time window

**Use Case:** Detect relay outages, monitor relay onboarding/offboarding
**Returns:** Binary (1/0) indicating if relay delivered blocks
**Time to Run:** 3-5 seconds
**Visualization:** State timeline or heatmap

**Grafana Setup:**
```yaml
Format: Time series
Transform: Partition by values → field: "relay"
Visualization: State timeline
Color: Green (1=active), Red (0=inactive)
```

**Example Output:**
```
time                | relay                    | is_active | blocks_delivered
--------------------------------------------------------------------------------
2024-01-15 12:00:00 | agnostic-relay.net       |         1 |              145
2024-01-15 13:00:00 | agnostic-relay.net       |         0 |                0  ← Gap!
2024-01-15 14:00:00 | agnostic-relay.net       |         1 |              152
```

**Alert Example:**
```sql
# Alert if relay inactive for >24h
WHEN last_over_time(is_active[24h]) = 0
```

---

### 9. `relay_volume_and_gaps.sql`
**Combined monitoring** - Volume + anomaly detection in one view

**Use Case:** Comprehensive relay health dashboard
**Returns:** Block counts + gap detection + severity scoring
**Time to Run:** 3-6 seconds
**Visualization:** Dual-axis time series (volume + gap severity)

**Grafana Setup:**
```yaml
Format: Time series (dual axis)
Left Y-axis: blocks_delivered (bars)
Right Y-axis: gap_severity_pct (line, red)
Thresholds:
  - >25% gap: Yellow
  - >50% gap: Red
```

**Example Output:**
```
time       | relay      | blocks_delivered | relay_avg | gap_severity_pct | status
--------------------------------------------------------------------------------------
2024-01-15 | flashbots  |              156 |    150.25 |              0.0 | OK
2024-01-15 | bloxroute  |               45 |    125.50 |             64.1 | CRITICAL
2024-01-15 | ultrasound |              130 |    140.75 |              7.6 | OK
```

**Status Levels:**
- **OK**: ≥75% of relay average
- **WARNING**: 50-75% of relay average
- **CRITICAL**: <50% of relay average

**Variables:**
- `$outlier_threshold`: Percentage below average to flag (default: 50)

---

## Grafana Setup

### Dashboard Layout Example

```
┌─────────────────────────────────────────────────────────┐
│  Data Quality Summary (Table)                           │
│  [data_quality_summary.sql]                             │
└─────────────────────────────────────────────────────────┘

┌──────────────────┬──────────────────┬────────────────────┐
│ Missing Blocks   │ Analysis Gap     │ Coverage %         │
│ [KPI: count]     │ [KPI: count]     │ [Gauge]            │
│ 1,453            │ 547              │ 99.85%             │
└──────────────────┴──────────────────┴────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Daily Missing Blocks (Time Series)                     │
│  [missing_blocks_daily.sql]                             │
│  X: date, Y: missing_blocks, Color: has_gaps            │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Hourly Ingestion Rate (Area Chart)                     │
│  [hourly_ingestion_rate.sql]                            │
│  X: hour, Y: ingestion_rate_pct, Threshold: 80%         │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Block Gaps Detailed (Table)                            │
│  [block_gaps_detailed.sql]                              │
│  Sort by: gap_size DESC                                 │
└─────────────────────────────────────────────────────────┘
```

### Variable Configuration

In Grafana dashboard settings, add these variables:

```
$gap_threshold
  Type: Constant
  Value: 1
  Description: Minimum gap size to report

$start_date
  Type: Constant
  Value: 2022-01-01
  Description: Analysis start date
```

### Alerts

**Critical: Missing Blocks Alert**
```sql
-- Alert when >1000 blocks are missing
SELECT missing_count
FROM missing_blocks_count.sql
WHERE missing_count > 1000
```

**Warning: Slow Ingestion Alert**
```sql
-- Alert when ingestion rate drops below 80%
SELECT AVG(ingestion_rate_pct)
FROM hourly_ingestion_rate.sql
WHERE hour >= NOW() - INTERVAL '6 hours'
HAVING AVG(ingestion_rate_pct) < 80
```

---

## Query Performance

All queries are optimized for quick execution (<5 seconds) on production databases with:
- Proper indexing on `blocks.number` and `blocks.timestamp`
- Time range filters using Grafana's `$__timeFilter()`
- LIMIT clauses on potentially large result sets

---

## Related Queries

- **Relay Coverage**: `../relay/daily_coverage_stats.sql` - Detect relay-specific gaps
- **Builder Stats**: `../builder/market_share.sql` - Verify builder data completeness
- **MEV-Boost Coverage**: `../market_share/mev_boost.sql` - Track MEV-Boost adoption

---

## Troubleshooting

### Query Returns No Results
- Check time range in Grafana (top-right corner)
- Verify database connection
- Ensure blocks table has data in selected range

### Query Times Out
- Reduce time range
- Add LIMIT clause if not present
- Check database indexes: `CREATE INDEX idx_blocks_timestamp ON blocks(timestamp);`

### Coverage Always Shows 100%
- May indicate time range contains no gaps
- Expand time range to historical data
- Check if backfill has already completed

---

## Development

To test queries locally:
```bash
psql -d mev_boost -f missing_blocks_count.sql
```

To add new integrity queries:
1. Create `.sql` file in this directory
2. Follow naming convention: `{category}_{metric}.sql`
3. Include header comment with description and usage
4. Test with sample data
5. Update this README
