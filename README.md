# Flashbots MEV-Boost Data Pipeline

A comprehensive data pipeline for collecting, processing, and analyzing Ethereum MEV-Boost (Proposer-Builder Separation) data. This project streams live block data, fetches information from MEV-Boost relays, and aggregates PBS metrics for analysis and visualization.

## Overview

This pipeline provides real-time and historical data collection for analyzing the MEV-Boost ecosystem:

- **Live streaming** of Ethereum block headers via WebSocket
- **Historical backfilling** of blocks, builder balances, relay payloads, and builder identifiers
- **PBS analysis** aggregating data across all sources
- **Grafana-ready SQL queries** for market share, profit analysis, and more
- **Production deployment** with Docker and Kubernetes

## Architecture

### Data Flow

```
Ethereum Node (WebSocket) → Live Stream
                                ├─> Blocks DB
                                ├─> Builder Balances DB
                                └─> Relay Payloads DB
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
├── data/
│   ├── blocks/                # Block header collection
│   ├── builders/              # Builder balance tracking
│   ├── relays/                # Relay payload collection
│   └── adjustments/           # Builder identification
└── helpers/                   # Shared utilities
```

## Features

### Data Collection

- **Blocks**: Full block headers with timestamps and miner addresses
- **Builder Balances**: Before/after balance calculations to track builder profit
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

# Backfill builder balances (from Ethereum RPC)
poetry run python src/data/builders/backfill.py

# Backfill extra builder balances for known addresses (from Ethereum RPC)
poetry run python src/data/builders/backfill_extra_builders.py

# Backfill relay payloads (from MEV-Boost relay APIs)
poetry run python src/data/relays/backfill.py

# Backfill Ultrasound relay adjustments
poetry run python src/data/adjustments/backfill.py

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
- `builder_balance` - Builder balance changes
- `extra_builder_balance` - Extra builder balance tracking for known addresses
- `relays_payloads` - Relay bid data
- `ultrasound_adjustments` - Ultrasound relay fee adjustments
- `analysis_pbs` - Aggregated PBS metrics with builder names parsed from extra_data
- `*_checkpoints` - Progress tracking for backfill

Tables are created automatically on first run.

## Grafana Integration

### Loading Queries

Queries are organized in `deploy/queries/` by category:
- `A_general/` - MEV-Boost adoption and market share
- `B_relay/` - Relay distribution and usage
- `C_builder/` - Builder market share and profitability
- `D_value_and_profitability/` - Value distribution, profit analysis, and overbidding

To use in Grafana:

1. Add PostgreSQL data source pointing to your database
2. Create a new panel
3. Copy query contents from the appropriate category into the SQL editor
4. Configure time range variables (`$__timeFilter`, `$__timeGroup`)

### Available Queries by Category

#### A. General Metrics
| File | Description | Type |
|------|-------------|------|
| `1_mev_boost_market_share.sql` | MEV-Boost vs vanilla blocks | Static |
| `2_mev_boost_market_share.sql` | MEV-Boost adoption over time | Time series |

#### B. Relay Analytics
| File | Description | Type |
|------|-------------|------|
| `1_relay_market_share.sql` | Relay usage distribution | Static |
| `2_relay_market_share.sql` | Relay market share over time | Time series |

#### C. Builder Analytics
| File | Description | Type |
|------|-------------|------|
| `1_builder_market_share_number_of_blocks.sql` | Builder dominance by block count (top 9 + Others) | Static |
| `2_builder_market_share_number_of_blocks.sql` | Builder market share over time | Time series |
| `3_builder_market_share_eth_profit.sql` | Builder market share by ETH profit | Static |
| `4_builder_market_share_eth_profit.sql` | Builder ETH profit over time | Time series |

#### D. Value & Profitability
| File | Description | Type |
|------|-------------|------|
| `1_total_value_distribution_percent.sql` | Total value distribution across blocks | Static |
| `2_total_value_distribution_percent_1.sql` | Total value distribution (time series) | Time series |
| `3_average_total_value.sql` | Average total value by block type | Static |
| `4_negative_total_value.sql` | Negative value block percentage | Static |
| `5_proposer_vs_builder_profit.sql` | Profit split between proposers and builders | Static |
| `6_proposer_share_per_builder.sql` | Proposer profit share by builder | Static |
| `7_overbid_distribution.sql` | Builder overbidding behavior | Static |
| `8_proposer_share_of_total_value.sql` | Proposer capture rate of total value | Static |
| `9_negative_total_value_blocks_mev_boost.sql` | Negative value MEV-Boost blocks | Static |
| `10_negative_total_value_blocks_vanilla.sql` | Negative value vanilla blocks | Static |
| `11_negative_total_value_blocks.sql` | All negative value block details | Static |

All queries include:
- Inline documentation explaining purpose and usage
- Grafana configuration instructions
- Support for time range filtering with `$__timeFilter`
- Top N builders with "Others" grouping where applicable
