# Miner Balance Backfill

This module calculates miner balance increases by querying Ethereum node balances at consecutive blocks.

## Overview

For each block in the `blocks` table that doesn't have a corresponding entry in `miners_balance`, the backfill:
1. Gets the miner's balance at block N-1
2. Gets the miner's balance at block N
3. Calculates the increase: `balance_after - balance_before`
4. Stores the result in the `miners_balance` table

## Data Source

**Ethereum JSON-RPC Node**
- Uses `eth_getBalance` method
- Queries balances at specific block numbers
- Batches 10 balance queries per JSON-RPC request
- Runs 5 batch requests in parallel for improved throughput

## Database Schema

**Table: `miners_balance`**

| Column | Type | Description |
|--------|------|-------------|
| `block_number` | BigInteger | Block number (primary key, indexed) |
| `miner` | String(42) | Miner address (indexed) |
| `balance_before` | Numeric | Balance in Wei at block N-1 |
| `balance_after` | Numeric | Balance in Wei at block N |
| `balance_increase` | Numeric | Increase in Wei (after - before, can be negative) |

**Index:** `idx_miner_block` on `(miner, block_number)` for efficient queries by miner.

## Configuration

Set the following environment variable in your `.env` file:

```bash
ETH_RPC_URL=https://your-ethereum-node-url
# Examples:
# - Infura: https://mainnet.infura.io/v3/YOUR-PROJECT-ID
# - Alchemy: https://eth-mainnet.g.alchemy.com/v2/YOUR-API-KEY
# - Local node: http://localhost:8545
```

## Usage

### Install Dependencies

```bash
make install
```

### Run Backfill

```bash
# Backfill all missing blocks
python src/data/miners/backfill.py

# Or with custom parameters
python -c "
from asyncio import run
from src.data.miners.backfill import BackfillMinerBalances

backfill = BackfillMinerBalances(
    eth_rpc_url='https://your-node-url',
    batch_size=10,        # RPC batch size
    db_batch_size=100,    # DB insert batch size
    parallel_batches=5,   # Parallel RPC requests
)
run(backfill.run(limit=1000))  # Process up to 1000 blocks
"
```

## Features

- **Automatic iteration**: Processes ALL missing blocks in 10K chunks automatically
- **Recent-first processing**: Always starts with the most recent missing blocks
- **Automatic gap detection**: Only processes blocks missing from `miners_balance`
- **DB-friendly**: Queries max 10,000 blocks at a time to minimize database load
- **Batch JSON-RPC**: Groups 10 `eth_getBalance` calls per request
- **Parallel RPC requests**: Runs 5 batch requests concurrently for 5x throughput
- **Batch DB inserts**: Inserts 100 records at a time
- **Overall progress tracking**: Shows total progress across all missing blocks
- **Upsert logic**: Safe to re-run on same blocks
- **Error handling**: Continues on RPC errors, logs issues

### Automatic Iteration & Progress Tracking

The backfill automatically processes ALL missing blocks with comprehensive progress:

1. Query total count of missing blocks upfront
2. Display overall progress bar
3. Fetch 10,000 most recent missing blocks
4. Process them (fetch balances in parallel, calculate increases, store)
5. Update overall progress
6. Repeat until no missing blocks remain

**Single command processes everything with real-time overall progress!**

## Performance

- **RPC batching**: 10 balance queries per HTTP request
- **Parallel requests**: 5 concurrent batch requests (configurable)
- **DB batching**: 100 records per insert
- **Typical speed**:
  - ~250-500 blocks/second with parallel requests (depends on RPC provider)
  - For 1M blocks: ~30-60 minutes with good RPC provider
- **Memory usage**: ~20-40 MB (slightly higher due to parallel requests)

## Example Output

```
Creating tables if not exist...
Backfilling miner balance increases
Ethereum RPC: https://mainnet.infura.io/v3/...
Batch size: 10 balances/request
Parallel batches: 5 concurrent requests
DB batch size: 100 records
Query limit: 10,000 blocks per iteration

Querying total missing blocks...
Found 25,432 total missing blocks

⠋ Overall Progress ━━━━━━━━━━━━━━ 12,500/25,432 • 0:04:10 • 0:04:05 remaining

✓ Backfill completed - Processed 25,432 blocks across 3 iterations
```

## How It Works

### SQL Query for Missing Blocks

```sql
SELECT b.number, b.miner
FROM blocks b
LEFT JOIN miners_balance mb ON b.number = mb.block_number
WHERE mb.block_number IS NULL
  AND b.number > 0
ORDER BY b.number DESC  -- Most recent blocks first
```

### JSON-RPC Batch Request

For blocks with miners `[0xabc...123, 0xdef...456]` at blocks `[100, 101]`:

```json
[
  {
    "jsonrpc": "2.0",
    "method": "eth_getBalance",
    "params": ["0xabc...123", "0x63"],
    "id": 0
  },
  {
    "jsonrpc": "2.0",
    "method": "eth_getBalance",
    "params": ["0xabc...123", "0x64"],
    "id": 1
  },
  ...
]
```

### Balance Calculation

```python
balance_increase = balance_after - balance_before
```

- Positive: Miner received block reward + fees
- Negative: Miner spent funds (rare for mining addresses)
- Zero: No change (shouldn't happen for block miners)

## Notes

- **Block 0**: Skipped (genesis block, no previous block)
- **Requires blocks data**: Must run blocks backfill first
- **RPC rate limits**: Adjust `batch_size` if hitting rate limits
- **Reorg handling**: Not implemented (assumes canonical chain)
- **Archive node**: May need archive node for historical balances

## Troubleshooting

**"ETH_RPC_URL is not set"**
- Add `ETH_RPC_URL` to your `.env` file

**"No missing blocks to process"**
- All blocks already have balance data
- Check `blocks` table has data: `SELECT COUNT(*) FROM blocks;`

**RPC timeouts**
- Reduce `batch_size` (try 5 instead of 10)
- Use a faster RPC provider
- Increase timeout in code if needed

**Slow performance**
- Increase `parallel_batches` (try 10-20 with good RPC provider)
- Increase `batch_size` if RPC allows (max ~20)
- Increase `db_batch_size` (try 500-1000)
- Use a local Ethereum node for best performance
- Reduce `parallel_batches` if hitting rate limits
