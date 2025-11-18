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
│   ├── backfill.py           # Historical PBS data aggregation
│   ├── live.py               # Live PBS data aggregation
│   ├── constants.py          # Builder name normalization
│   └── queries/              # Grafana SQL queries
│       ├── builder_market_share.sql
│       ├── builder_profit.sql
│       ├── proposer_vs_builder_profit.sql
│       └── ...
├── data/
│   ├── blocks/               # Block header collection
│   │   ├── backfill.py      # Historical block fetching
│   │   └── live.py          # Live block streaming
│   ├── proposers/            # Proposer balance tracking
│   │   ├── backfill.py      # Historical balance calculation
│   │   └── live.py          # Live balance tracking
│   ├── relays/               # Relay payload collection
│   │   ├── backfill.py      # Historical relay data
│   │   ├── live.py          # Live relay monitoring
│   │   └── constants.py     # Relay configuration
│   └── builders/             # Builder identification
│       ├── backfill.py      # Historical builder mapping
│       └── live.py          # Live builder identification
└── helpers/                  # Shared utilities
    ├── db.py                # Database configuration
    └── logging.py           # Structured logging
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

- `blocks` - Block headers
- `proposers_balance` - Proposer balance changes
- `relays_payloads` - Relay bid data
- `builders_identifiers` - Builder name mapping
- `analysis_pbs` - Aggregated PBS metrics
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

## Deployment

### Docker Build

Build the Docker image:

```bash
cd deploy
./build.sh
```

This builds and pushes to your container registry (configured in `build.sh`).

### Kubernetes Deployment

Deploy to Kubernetes:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/docker-registry-secret.yaml
kubectl apply -f deploy/k8s/deployment.yaml
```

The deployment:
- Runs the live streaming coordinator
- Auto-restarts on failure
- Uses ConfigMaps for environment configuration
- Pulls from private container registry

Monitor logs:

```bash
kubectl logs -n flashbots -l app=flashbots-live --follow
```

## Development

### Code Quality Standards

This project enforces strict code quality:

- **Linting**: Ruff with default rules
- **Formatting**: Ruff formatter (88 char line length)
- **Type Checking**: Pyright in strict mode
- **Testing**: Pytest with coverage reporting
- **CI/CD**: All checks must pass before merge

### Testing

Run tests:

```bash
make test           # Basic test run
make test-cov       # With coverage report
```

Coverage reports are generated in `htmlcov/`.

### Adding New Queries

To add a new Grafana query:

1. Create a new `.sql` file in `src/analysis/queries/`
2. Follow the existing query format:
   - Header comment with description
   - Variable documentation (`$__timeFilter`, etc.)
   - Return column documentation
   - Grafana configuration instructions
3. Test the query in Grafana before committing

### Modifying Builder Name Mapping

Edit `src/analysis/constants.py`:

```python
BUILDER_NAME_MAPPING = {
    "raw_name": "Canonical Name",
    # Add new mappings here
}
```

The `clean_builder_name()` function applies these mappings automatically.

## Performance Considerations

### Database Optimization

For best performance:

1. **Use TimescaleDB** instead of plain PostgreSQL:
   ```sql
   SELECT create_hypertable('analysis_pbs', 'block_timestamp');
   ```

2. **Create indexes** on frequently queried columns:
   ```sql
   CREATE INDEX idx_analysis_pbs_builder_name ON analysis_pbs(builder_name);
   CREATE INDEX idx_analysis_pbs_timestamp ON analysis_pbs(block_timestamp);
   ```

3. **Configure PostgreSQL** for time-series workloads:
   - Increase `shared_buffers`
   - Tune `work_mem` for aggregations
   - Enable parallel query execution

### Backfill Strategies

- Start with **recent data** (last 6 months) for quick value
- Run **relays backfill** with caution (rate limits apply)
- Use **smaller batch sizes** (1000) for memory-constrained systems
- Run **multiple backfill scripts** in parallel for faster completion

## Troubleshooting

### Relay Timeouts

If relay backfill fails with timeouts:
- Check `src/data/relays/constants.py` for problematic relays
- Comment out relays with Cloudflare protection
- Increase retry delays in `backfill.py`

### WebSocket Disconnections

Live streaming auto-reconnects, but if issues persist:
- Check `ETH_WS_URL` is correct and accessible
- Verify firewall/proxy allows WebSocket connections
- Monitor logs for specific error messages

### Database Connection Issues

- Verify `DATABASE_URL` format: `postgresql://user:pass@host:port/db`
- Check PostgreSQL is running and accepting connections
- Ensure database exists (create with `createdb flashbots`)

### Type Checking Errors

SQLAlchemy patterns may cause false positives:
- Use `# type: ignore` for known safe patterns
- Check existing code for examples
- Refer to SQLAlchemy type stubs documentation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make all` to verify all checks pass
5. Submit a pull request

## License

[Specify your license here]

## Acknowledgments

- **Flashbots**: For the MEV-Boost protocol and relay infrastructure
- **Ethereum Foundation**: For Ethereum and the Beacon Chain
- Built with Python 3.14, SQLAlchemy, httpx, and Rich

## Contact

[Your contact information]
