# Ethereum Blocks Backfill

This module fetches Ethereum block data from the AWS Public Blockchain Dataset and stores it in the database.

## Data Source

**AWS Public Blockchain Dataset**
- Base URL: `https://aws-public-blockchain.s3.us-east-2.amazonaws.com`
- Path: `v1.0/eth/blocks/date=YYYY-MM-DD/`
- Format: Snappy-compressed Parquet files
- Partitioning: By date (one file per day)
- Coverage: From Ethereum genesis (2015-07-30) to present

## Database Schema

The blocks are stored in the `blocks` table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `number` | BigInteger | Block number (primary key, indexed) |
| `hash` | String(66) | Block hash (unique, indexed) |
| `parent_hash` | String(66) | Parent block hash (indexed) |
| `nonce` | String(18) | Block nonce |
| `sha3_uncles` | String(66) | SHA3 of uncles |
| `logs_bloom` | String(514) | Logs bloom filter |
| `transactions_root` | String(66) | Transactions root hash |
| `state_root` | String(66) | State root hash |
| `receipts_root` | String(66) | Receipts root hash |
| `miner` | String(42) | Miner address (indexed) |
| `difficulty` | Float | Mining difficulty |
| `total_difficulty` | Float | Cumulative difficulty |
| `size` | Integer | Block size in bytes |
| `extra_data` | String | Extra data field |
| `gas_limit` | BigInteger | Gas limit |
| `gas_used` | BigInteger | Gas used |
| `timestamp` | DateTime | Block timestamp (indexed) |
| `transaction_count` | Integer | Number of transactions |
| `base_fee_per_gas` | Float | Base fee (nullable, EIP-1559) |

## Usage

### Install Dependencies

```bash
make install
```

### Run Backfill

```bash
# Backfill from genesis to today
python -m src.data.blocks.backfill

# Or run directly
cd /path/to/project
python src/data/blocks/backfill.py
```

### Custom Date Range

Edit `src/data/blocks/backfill.py` and modify the parameters:

```python
backfill = BackfillBlocks(
    start_date="2015-07-30",  # Start date
    end_date="2015-08-31",    # End date (or None for today)
    batch_size=1000,          # Batch size for DB inserts
)
```

## Features

- **Incremental backfill**: Automatically resumes from last checkpoint
- **Progress tracking**: Visual progress bar with rich formatting
- **Batch inserts**: Efficient bulk inserts with configurable batch size
- **Upsert logic**: Uses `ON CONFLICT DO UPDATE` to handle duplicates
- **Error handling**: Graceful handling of missing dates and network errors
- **Logging**: Comprehensive logging with configurable log levels

## Checkpoint System

The backfill tracks progress using the `block_checkpoints` table:
- **One row per date** - Each successfully processed date gets its own checkpoint
- **Block count tracking** - Stores how many blocks were processed for each date
- **Smart resume** - On restart, queries all completed dates and skips them
- **Idempotent** - Can safely re-run for the same date range without duplicates
- **Handles failures** - If processing fails mid-range, completed dates are preserved

**Schema:**
```sql
CREATE TABLE block_checkpoints (
    date VARCHAR(10) PRIMARY KEY,  -- YYYY-MM-DD
    block_count INTEGER NOT NULL   -- Number of blocks for this date
);
```

## Performance

- Processes ~1 day of blocks in 1-5 seconds (depending on network speed)
- Typical throughput: ~1,000-5,000 blocks/second
- Memory usage: ~20-50 MB per batch

## Example Output

**First run:**
```
Backfilling Ethereum blocks from AWS S3
Date range: 2015-07-30 to 2015-08-31
Total dates in range: 33
Already completed: 0
To process: 33

⠋ 2015-07-30 ✓ 6,911 blocks ━━━━━━━━━━━━━━━━━━━━ 1/33 • 0:00:02
⠋ 2015-07-31 ✓ 6,912 blocks ━━━━━━━━━━━━━━━━━━━━ 2/33 • 0:00:04
...

✓ Backfill completed - Processed 234,567 blocks across 33 dates
```

**Resume after interruption:**
```
Backfilling Ethereum blocks from AWS S3
Date range: 2015-07-30 to 2015-08-31
Total dates in range: 33
Already completed: 15
To process: 18

⠋ 2015-08-15 ✓ 7,145 blocks ━━━━━━━━━━━━━━━━━━━━ 1/18 • 0:00:01
...
```

## Notes

- Requires PostgreSQL database credentials in `.env` file
- Some dates may not have data (pre-genesis or future dates)
- Base fee per gas is `NULL` for blocks before EIP-1559 (London hard fork)
