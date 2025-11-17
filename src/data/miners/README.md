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
- Batches 10 balance queries per JSON-RPC request for efficiency

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
    batch_size=10,      # RPC batch size
    db_batch_size=100,  # DB insert batch size
)
run(backfill.run(limit=1000))  # Process 1000 blocks
"
```

## Features

- **Automatic iteration**: Processes ALL missing blocks in 10K chunks automatically
- **Recent-first processing**: Always starts with the most recent missing blocks
- **Automatic gap detection**: Only processes blocks missing from `miners_balance`
- **DB-friendly**: Queries max 10,000 blocks at a time to minimize database load
- **Batch JSON-RPC**: Groups 10 `eth_getBalance` calls per request
- **Batch DB inserts**: Inserts 100 records at a time
- **Progress tracking**: Visual progress bar with rich formatting per iteration
- **Upsert logic**: Safe to re-run on same blocks
- **Error handling**: Continues on RPC errors, logs issues

### Automatic Iteration

The backfill automatically processes ALL missing blocks in manageable 10K chunks:

1. Query for 10,000 most recent missing blocks
2. Process them (fetch balances, calculate increases, store)
3. Query again for next 10,000
4. Repeat until no missing blocks remain

**Single command processes everything - no manual looping needed!**

## Performance

- **RPC batching**: 10 balance queries per HTTP request
- **DB batching**: 100 records per insert
- **Typical speed**:
  - ~50-100 blocks/second (depends on RPC response time)
  - For 1M blocks: ~3-6 hours
- **Memory usage**: ~10-20 MB

## Example Output

```
Creating tables if not exist...
Backfilling miner balance increases
Ethereum RPC: https://mainnet.infura.io/v3/...
Batch size: 10 balances/request
DB batch size: 100 records
Query limit: 10,000 blocks per iteration

Iteration 1: Querying for missing blocks...
Found 10,000 missing blocks in this iteration
⠋ Iteration 1 ━━━━━━━━━━━━━━ 10,000/10,000 • 0:05:23
✓ Iteration 1 completed - Processed 10,000 blocks

Iteration 2: Querying for missing blocks...
Found 10,000 missing blocks in this iteration
⠋ Iteration 2 ━━━━━━━━━━━━━━ 10,000/10,000 • 0:05:12
✓ Iteration 2 completed - Processed 10,000 blocks

Iteration 3: Querying for missing blocks...
Found 5,432 missing blocks in this iteration
⠋ Iteration 3 ━━━━━━━━━━━━━━ 5,432/5,432 • 0:02:45
✓ Iteration 3 completed - Processed 5,432 blocks

Iteration 4: Querying for missing blocks...
No more missing blocks - backfill complete!

✓ Backfill completed - Processed 25,432 total blocks across 3 iterations
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
- Increase `batch_size` if RPC allows (max ~20)
- Increase `db_batch_size` (try 500-1000)
- Use a local Ethereum node for best performance
