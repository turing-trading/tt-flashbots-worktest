# MEV-Boost Relay Dashboard

This directory contains a grafanalib-based implementation of the MEV-Boost Relay Grafana dashboard.

## Overview

The dashboard has been migrated from a JSON file (`deploy/grafana/dashboard.json`) to a programmatic Python implementation using [grafanalib](https://github.com/weaveworks/grafanalib).

## Directory Structure

```
src/dashboard/
├── __init__.py              # Package initialization
├── dashboard.py             # Main dashboard definition
├── panels.py                # Helper functions for creating panels
├── queries.py               # Utilities for loading SQL queries
├── generate.py              # CLI script to generate dashboard JSON
├── README.md                # This file
└── queries/                 # SQL query files (moved from deploy/queries)
    ├── A_general/           # General MEV-Boost metrics
    ├── B_relay/             # Relay-specific metrics
    ├── C_builder/           # Builder-specific metrics
    └── D_value_and_profitability/  # Value distribution and profitability
```

## Dashboard Structure

The dashboard contains **4 main sections**:

### 1. General
- **MEV-Boost Market Share** (Pie + Time Series)
  - Shows percentage of blocks using MEV-Boost vs vanilla blocks

### 2. Relay
- **Relay Market Share** (Pie + Time Series)
  - Displays each relay's share of delivered blocks

### 3. Builder
- **Builder Market Share by Block Count** (Pie + Time Series)
  - Shows which builders produce the most blocks
- **Builder Market Share by ETH Profit** (Pie + Time Series)
  - Shows which builders capture the most value

### 4. Value and Profitability
- **Total Value Distribution** (Bar Chart)
  - Distribution of block values for MEV-Boost vs vanilla
- **Average Total Value** (Stat)
  - Average ETH earned per block
- **Negative Total Value** (Stat)
  - Percentage of blocks with negative value
- **Proposer vs. Builder Profit** (Pie Chart)
  - Revenue split between proposers and builders
- **Proposer Share per Builder** (Time Series)
  - Proposer share of total value over time
- **Overbid Distribution** (Bar Chart)
  - How much builders are overbidding
- **Proposer Share of Total Value** (Bar Chart)
  - Average proposer profit percentage by builder
- **Negative Value Blocks** (Bar Charts + Table)
  - Analysis of blocks with negative total value

## Usage

### Generate Dashboard JSON

To generate the dashboard JSON file:

```bash
python -m src.dashboard.generate
```

This will create `src/dashboard/generated_dashboard.json`.

### Import to Grafana

1. Generate the dashboard JSON as shown above
2. In Grafana, go to **Dashboards** → **Import**
3. Upload the `generated_dashboard.json` file
4. Configure the datasource to point to your TimescaleDB instance

### Modify the Dashboard

The dashboard is defined programmatically in Python files:

1. **dashboard.py** - Main dashboard structure and panel layout
2. **panels.py** - Helper functions for creating different panel types
3. **queries/** - SQL query files referenced by the panels

To modify:
1. Edit the relevant Python files or SQL queries
2. Run `python -m src.dashboard.generate` to regenerate the JSON
3. Re-import to Grafana

## Benefits of grafanalib

Using grafanalib instead of raw JSON provides:

- **Version Control**: Easier to track changes in Python code
- **Modularity**: Reusable functions for common panel types
- **Type Safety**: Python type hints help catch errors
- **Maintainability**: Easier to understand and modify
- **DRY Principle**: Avoid repetition in dashboard definitions
- **Testing**: Can write unit tests for dashboard generation

## Requirements

- Python 3.14+
- grafanalib (installed via Poetry)
- PostgreSQL/TimescaleDB datasource in Grafana

## SQL Queries

All SQL queries are stored in the `queries/` directory, organized by category:

- **A_general/**: General MEV-Boost adoption metrics
- **B_relay/**: Relay performance and market share
- **C_builder/**: Builder performance and profitability
- **D_value_and_profitability/**: Detailed value analysis

Queries use Grafana variables:
- `$__timeFilter(block_timestamp)` - Time range filter
- `$__timeGroup(block_timestamp, $__interval)` - Dynamic time bucketing
- `${DS_TIMESCALEDB_- FLASHBOTS}` - Datasource reference

## Notes

- The dashboard expects data in the `analysis_pbs` table
- Default time range is last 20 days
- Dashboard UID: `e46c6ca2-cd80-4811-955b-f4fcafc860af`
- Totals in relay market share can exceed 100% when blocks are submitted to multiple relays
