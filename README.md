# Flashbots MEV-Boost Data Pipeline

A comprehensive data pipeline for collecting, processing, and analyzing Ethereum MEV-Boost (Proposer-Builder Separation) data. This project streams live block data, fetches information from MEV-Boost relays, and aggregates PBS metrics for analysis and visualization.

## Overview

This pipeline provides real-time and historical data collection for analyzing the MEV-Boost ecosystem:

- **Live streaming** of Ethereum block headers via WebSocket
- **Historical backfilling** of blocks, proposer balances, relay payloads, and builder identifiers
- **PBS analysis** aggregating data across all sources
- **Grafana-ready SQL queries** for market share, profit analysis, and more
- **Production deployment** with Docker and Kubernetes

## Architecture

### Data Flow

```
Ethereum Node (WebSocket) → Live Stream → Processing Queues
                                              ├─> Blocks DB
                                              ├─> Proposer Balances DB
                                              ├─> Relay Payloads DB
                                              └─> Builder Identifiers DB
                                                        ↓
                                              PBS Analysis Aggregation
                                                        ↓
                                              Grafana Dashboards
```

### Module Structure

```
src/
├── live.py                    # WebSocket coordinator for live streaming
├── analysis/                  # PBS analysis and aggregation
│   ├── backfill.py            # Historical PBS data aggregation
│   ├── live.py                # Live PBS data aggregation
│   ├── constants.py           # Builder name normalization
│   └── queries/               # Grafana SQL queries
│       ├── builder_market_share.sql
│       ├── builder_profit.sql
│       ├── proposer_vs_builder_profit.sql
│       └── ...
├── data/
│   ├── blocks/                # Block header collection
│   │   ├── backfill.py        # Historical block fetching
│   │   └── live.py            # Live block streaming
│   ├── proposers/             # Proposer balance tracking
│   │   ├── backfill.py        # Historical balance calculation
│   │   └── live.py            # Live balance tracking
│   ├── relays/                # Relay payload collection
│   │   ├── backfill.py        # Historical relay data
│   │   ├── live.py            # Live relay monitoring
│   │   └── constants.py       # Relay configuration
│   └── builders/              # Builder identification
│       ├── backfill.py        # Historical builder mapping
│       └── live.py            # Live builder identification
└── helpers/                   # Shared utilities
    ├── db.py                  # Database configuration
    └── logging.py             # Structured logging
```

## Features

### Data Collection

- **Blocks**: Full block headers with timestamps and miner addresses
- **Proposer Balances**: Before/after balance calculations to track proposer profit
- **Relay Payloads**: MEV-Boost relay bids and winning payloads
- **Builder Identifiers**: Builder public keys mapped to canonical names
- **PBS Analysis**: Aggregated view combining all data sources

### Builder Name Normalization

Automatically cleans and normalizes builder names for consistent analysis:
- Groups variants (e.g., "titanbuilder.xyz", "Titan (titanbuilder.xyz)" → "Titan")
- Categorizes BuilderNet participants (Flashbots, Beaver, Nethermind)
- Marks unknown builders (geth variants, generic names)

### Grafana SQL Queries

Production-ready queries for visualization:
- **Market Share**: Builder dominance over time (top 9 + "Others")
- **Profit Analysis**: Builder vs. proposer profit split
- **MEV-Boost Adoption**: Percentage of blocks using relays
- **Relay Distribution**: Which relays are most popular
- All queries support time-series rolling windows

## Setup

### Prerequisites

- **Python 3.13+** (tested with 3.14)
- **Poetry** for dependency management
- **PostgreSQL** (or TimescaleDB for better time-series performance)
- **Ethereum RPC endpoint** with WebSocket support
- **Environment variables** (see Configuration)

### Installation

Install Poetry if you haven't already:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Install dependencies:

```bash
poetry install
```

### Configuration

Create a `.env` file with the following variables:

```bash
# Database connection
DATABASE_URL=postgresql://user:password@localhost:5432/flashbots

# Ethereum endpoints
ETH_RPC_URL=https://your-rpc-endpoint.com
ETH_WS_URL=wss://your-websocket-endpoint.com
```

Copy the example environment file:

```bash
cp .env.example .env
# Edit .env with your values
```

## Usage

### Development Workflow

Use the Makefile for all common operations:

```bash
make install       # Install dependencies
make lint          # Run ruff linting
make lint-fix      # Auto-fix lint issues
make format        # Format code with ruff
make format-check  # Verify formatting
make type-check    # Run pyright type checking
make test          # Run pytest
make test-cov      # Run tests with coverage
make all           # Run full pipeline (lint + format + type-check + test)
make clean         # Remove caches and generated files
```

### Running Data Collection

#### Live Streaming

Stream real-time block data from Ethereum:

```bash
poetry run python src/live.py
```

This starts the coordinator which:
1. Connects to Ethereum WebSocket
2. Subscribes to `newHeads` events
3. Distributes headers to all processing modules
4. Auto-reconnects with exponential backoff on disconnect

#### Historical Backfilling

Backfill historical data (run in separate terminals):

```bash
# Backfill blocks (from Ethereum RPC)
poetry run python src/data/blocks/backfill.py

# Backfill proposer balances (from Ethereum RPC)
poetry run python src/data/proposers/backfill.py

# Backfill relay payloads (from MEV-Boost relay APIs)
poetry run python src/data/relays/backfill.py

# Backfill builder identifiers (from relay data)
poetry run python src/data/builders/backfill.py

# Aggregate PBS analysis (from all sources)
poetry run python src/analysis/backfill.py
```

**Note**: Backfill scripts support:
- Automatic checkpointing (resume from last position)
- Progress bars with ETA
- Batch processing (configurable batch size)
- Independent relay failures (one failing relay doesn't stop others)

### Database Schema

The pipeline uses PostgreSQL with the following tables:

- `blocks` - Block headers (includes extra_data for builder identification)
- `proposers_balance` - Proposer balance changes
- `relays_payloads` - Relay bid data
- `analysis_pbs_v2` - Aggregated PBS metrics with builder names parsed from extra_data
- `*_checkpoints` - Progress tracking for backfill

Tables are created automatically on first run.

## Grafana Integration

### Loading Queries

Queries are in `src/analysis/queries/`. To use in Grafana:

1. Add PostgreSQL data source pointing to your database
2. Create a new panel
3. Copy query contents into the SQL editor
4. Configure time range variables (`$__timeFilter`, `$__timeGroup`)

### Available Queries

| Query | Description | Type |
|-------|-------------|------|
| `builder_market_share.sql` | Builder dominance (top 9 + Others) | Static |
| `builder_market_share_rolling.sql` | Builder market share over time | Time series |
| `builder_profit.sql` | Builder profitability ranking | Static |
| `builder_profit_rolling.sql` | Builder profit trends | Time series |
| `proposer_vs_builder_profit.sql` | Profit split comparison | Static |
| `proposer_vs_builder_profit_rolling.sql` | Profit split over time | Time series |
| `mev_boost_market_share.sql` | MEV-Boost vs vanilla blocks | Static |
| `relay_market_share.sql` | Relay usage distribution | Static |

All queries include:
- Inline documentation explaining purpose and usage
- Grafana configuration instructions
- Support for time range filtering
- Example visualization settings
