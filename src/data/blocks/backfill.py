"""Backfill Ethereum block data from AWS Public Blockchain Dataset."""

import asyncio
import io
import os
from asyncio import run
from datetime import datetime, timedelta

import httpx
import pandas as pd
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.blocks.db import BlockCheckpoints, BlockDB
from src.data.blocks.models import Block
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger

load_dotenv()


class BackfillBlocks:
    """Backfill Ethereum blocks from AWS S3."""

    def __init__(
        self,
        start_date: str = "2015-07-30",
        end_date: str | None = None,
        batch_size: int = 1000,
        eth_rpc_url: str | None = None,
        rpc_batch_size: int = 50,
        parallel_batches: int = 30,
    ):
        """Initialize backfill.

        Args:
            start_date: Start date in YYYY-MM-DD format (default: genesis)
            end_date: End date in YYYY-MM-DD format (default: today)
            batch_size: Number of blocks to insert per batch
            eth_rpc_url: Ethereum JSON-RPC endpoint (defaults to env var ETH_RPC_URL)
            rpc_batch_size: Number of blocks per JSON-RPC batch request
            parallel_batches: Number of batch RPC requests to run in parallel
        """
        self.base_url = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com"
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").date()
            if end_date
            else datetime.now().date()
        )
        self.batch_size = batch_size
        self.eth_rpc_url = eth_rpc_url or os.getenv("ETH_RPC_URL")
        self.rpc_batch_size = rpc_batch_size
        self.parallel_batches = parallel_batches
        self.logger = get_logger("backfill_blocks", log_level="INFO")
        self.console = Console()

    async def _get_completed_dates(self, session: AsyncSession) -> set[str]:
        """Get all dates that have been successfully processed.

        Returns:
            Set of date strings in YYYY-MM-DD format
        """
        stmt = select(BlockCheckpoints.date)
        result = await session.execute(stmt)
        completed_dates = {row[0] for row in result.fetchall()}
        return completed_dates

    async def _add_checkpoint(
        self, session: AsyncSession, date: str, block_count: int
    ) -> None:
        """Add a checkpoint for a successfully processed date.

        Args:
            session: Database session
            date: Date string in YYYY-MM-DD format
            block_count: Number of blocks processed for this date
        """
        # Use upsert to handle re-processing of dates
        stmt = pg_insert(BlockCheckpoints).values(date=date, block_count=block_count)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date"],
            set_={"block_count": stmt.excluded.block_count},
        )
        await session.execute(stmt)
        await session.commit()

    async def _fetch_parquet(
        self, client: httpx.AsyncClient, date: str
    ) -> pd.DataFrame | None:
        """Fetch parquet file for a given date from S3.

        Args:
            client: HTTP client
            date: Date string in YYYY-MM-DD format

        Returns:
            DataFrame with block data or None if not found
        """
        # Construct S3 URL for the date partition
        # Format: v1.0/eth/blocks/date=YYYY-MM-DD/
        s3_key = f"v1.0/eth/blocks/date={date}/"

        try:
            # First, list objects in this partition to get the parquet filename
            list_url = f"{self.base_url}/?prefix={s3_key}"
            response = await client.get(list_url, timeout=30.0)
            response.raise_for_status()

            # Parse XML response to extract Key
            import xml.etree.ElementTree as ET

            root = ET.fromstring(response.text)
            namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
            keys = root.findall(".//s3:Key", namespace)

            if not keys:
                self.logger.warning(f"No data found for date {date}")
                return None

            # Get the first (and usually only) parquet file
            parquet_key = keys[0].text
            if not parquet_key:
                return None

            # Download the parquet file
            parquet_url = f"{self.base_url}/{parquet_key}"
            self.logger.debug(f"Fetching {parquet_url}")

            response = await client.get(parquet_url, timeout=60.0)
            response.raise_for_status()

            # Read parquet from bytes
            df = pd.read_parquet(io.BytesIO(response.content))
            self.logger.debug(f"Fetched {len(df)} blocks for date {date}")

            return df

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"No data available for date {date}")
            else:
                self.logger.error(f"HTTP {e.response.status_code} for date {date}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching data for date {date}: {e}")
            return None

    def _dataframe_to_blocks(self, df: pd.DataFrame) -> list[Block]:
        """Convert DataFrame to list of Block Pydantic models."""
        blocks = []
        for _, row in df.iterrows():
            # Handle timestamp conversion
            timestamp_val = row["timestamp"]
            if isinstance(timestamp_val, pd.Timestamp):
                timestamp = timestamp_val.to_pydatetime()
            elif hasattr(timestamp_val, "to_pydatetime"):
                timestamp = timestamp_val.to_pydatetime()  # type: ignore[union-attr]
            else:
                timestamp = pd.Timestamp(timestamp_val).to_pydatetime()  # type: ignore[arg-type]

            # Handle base_fee_per_gas which might be NaN
            base_fee_val = row["base_fee_per_gas"]
            try:
                base_fee_per_gas = (
                    None
                    if pd.isna(base_fee_val)  # type: ignore[arg-type]
                    else float(base_fee_val)
                )
            except (TypeError, ValueError):
                base_fee_per_gas = None

            block = Block(
                number=int(row["number"]),
                hash=str(row["hash"]),
                parent_hash=str(row["parent_hash"]),
                nonce=str(row["nonce"]),
                sha3_uncles=str(row["sha3_uncles"]),
                transactions_root=str(row["transactions_root"]),
                state_root=str(row["state_root"]),
                receipts_root=str(row["receipts_root"]),
                miner=str(row["miner"]),
                size=int(row["size"]),
                extra_data=str(row["extra_data"]),
                gas_limit=int(row["gas_limit"]),
                gas_used=int(row["gas_used"]),
                timestamp=timestamp,
                transaction_count=int(row["transaction_count"]),
                base_fee_per_gas=base_fee_per_gas,
            )
            blocks.append(block)
        return blocks

    async def _store_blocks(self, session: AsyncSession, blocks: list[Block]) -> None:
        """Store blocks in the database using batch upsert."""
        if not blocks:
            return

        # Process in batches
        for i in range(0, len(blocks), self.batch_size):
            batch = blocks[i : i + self.batch_size]
            values = [block.model_dump() for block in batch]

            # Upsert using ON CONFLICT
            stmt = pg_insert(BlockDB).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["number"],
                set_={
                    BlockDB.hash: stmt.excluded.hash,
                    BlockDB.parent_hash: stmt.excluded.parent_hash,
                    BlockDB.nonce: stmt.excluded.nonce,
                    BlockDB.sha3_uncles: stmt.excluded.sha3_uncles,
                    BlockDB.transactions_root: stmt.excluded.transactions_root,
                    BlockDB.state_root: stmt.excluded.state_root,
                    BlockDB.receipts_root: stmt.excluded.receipts_root,
                    BlockDB.miner: stmt.excluded.miner,
                    BlockDB.size: stmt.excluded.size,
                    BlockDB.extra_data: stmt.excluded.extra_data,
                    BlockDB.gas_limit: stmt.excluded.gas_limit,
                    BlockDB.gas_used: stmt.excluded.gas_used,
                    BlockDB.timestamp: stmt.excluded.timestamp,
                    BlockDB.transaction_count: stmt.excluded.transaction_count,
                    BlockDB.base_fee_per_gas: stmt.excluded.base_fee_per_gas,
                },
            )

            await session.execute(stmt)

        await session.commit()

    async def _get_latest_block_number(self, client: httpx.AsyncClient) -> int:
        """Get the latest block number from RPC.

        Returns:
            Latest block number
        """
        if not self.eth_rpc_url:
            raise ValueError("ETH_RPC_URL must be configured to fetch missing blocks")

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1,
        }

        response = await client.post(self.eth_rpc_url, json=payload, timeout=30.0)
        response.raise_for_status()
        result = response.json()
        return int(result["result"], 16)

    async def _get_min_max_block_numbers(
        self, session: AsyncSession
    ) -> tuple[int, int]:
        """Get min and max block numbers from the blocks table.

        Returns:
            Tuple of (min_block, max_block)
        """
        stmt = select(func.min(BlockDB.number), func.max(BlockDB.number))
        result = await session.execute(stmt)
        row = result.one()
        return (row[0] or 0, row[1] or 0)

    async def _find_missing_blocks(
        self, session: AsyncSession, start_block: int, end_block: int
    ) -> list[int]:
        """Find missing block numbers in the range [start_block, end_block].

        Compares actual block numbers in the table with expected sequential numbers.

        Args:
            session: Database session
            start_block: Start of range (inclusive)
            end_block: End of range (inclusive)

        Returns:
            List of missing block numbers
        """
        # Generate expected sequence and find gaps
        query_str = f"""
        WITH expected_blocks AS (
            SELECT generate_series({start_block}, {end_block}) AS block_number
        )
        SELECT e.block_number
        FROM expected_blocks e
        LEFT JOIN blocks b ON e.block_number = b.number
        WHERE b.number IS NULL
        ORDER BY e.block_number DESC
        """

        result = await session.execute(text(query_str))
        missing = [row[0] for row in result.fetchall()]
        return missing

    async def _fetch_block_via_rpc(
        self, client: httpx.AsyncClient, block_number: int
    ) -> Block | None:
        """Fetch a single block via RPC.

        Args:
            client: HTTP client
            block_number: Block number to fetch

        Returns:
            Block model or None if failed
        """
        if not self.eth_rpc_url:
            raise ValueError("ETH_RPC_URL must be configured")

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(block_number), False],  # False = don't include transactions
            "id": 1,
        }

        try:
            response = await client.post(self.eth_rpc_url, json=payload, timeout=30.0)
            response.raise_for_status()
            result = response.json()

            if "result" not in result or result["result"] is None:
                self.logger.warning(f"Block {block_number} not found via RPC")
                return None

            block_data = result["result"]

            # Convert RPC response to Block model
            timestamp = datetime.fromtimestamp(int(block_data["timestamp"], 16))

            return Block(
                number=int(block_data["number"], 16),
                hash=block_data["hash"],
                parent_hash=block_data["parentHash"],
                nonce=block_data["nonce"],
                sha3_uncles=block_data["sha3Uncles"],
                transactions_root=block_data["transactionsRoot"],
                state_root=block_data["stateRoot"],
                receipts_root=block_data["receiptsRoot"],
                miner=block_data["miner"],
                size=int(block_data["size"], 16),
                extra_data=block_data["extraData"],
                gas_limit=int(block_data["gasLimit"], 16),
                gas_used=int(block_data["gasUsed"], 16),
                timestamp=timestamp,
                transaction_count=len(block_data.get("transactions", [])),
                base_fee_per_gas=(
                    int(block_data["baseFeePerGas"], 16)
                    if "baseFeePerGas" in block_data
                    else None
                ),
            )

        except Exception as e:
            self.logger.error(f"Error fetching block {block_number} via RPC: {e}")
            return None

    async def _batch_fetch_blocks(
        self,
        client: httpx.AsyncClient,
        block_numbers: list[int],
    ) -> dict[int, Block]:
        """Batch multiple eth_getBlockByNumber calls into a single JSON-RPC request.

        Args:
            client: HTTP client
            block_numbers: List of block numbers to fetch

        Returns:
            Dict mapping block_number to Block model
        """
        if not self.eth_rpc_url:
            raise ValueError("ETH_RPC_URL must be configured")

        # Build batch JSON-RPC request
        batch_payload = []
        for idx, block_number in enumerate(block_numbers):
            batch_payload.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [
                        hex(block_number),
                        False,
                    ],  # False = don't include transactions
                    "id": idx,
                }
            )

        try:
            self.logger.debug(
                f"Fetching {len(block_numbers)} blocks (blocks {block_numbers[0]} to {block_numbers[-1]})"
            )

            response = await client.post(
                self.eth_rpc_url,
                json=batch_payload,
                timeout=60.0,  # Increased timeout for batch requests
            )
            response.raise_for_status()

            results = response.json()

            # Map results back to block numbers
            block_map = {}
            for result in results:
                idx = result["id"]
                block_number = block_numbers[idx]

                if "result" not in result or result["result"] is None:
                    self.logger.warning(f"Block {block_number} not found via RPC")
                    continue

                block_data = result["result"]

                try:
                    # Convert RPC response to Block model
                    timestamp = datetime.fromtimestamp(int(block_data["timestamp"], 16))

                    block = Block(
                        number=int(block_data["number"], 16),
                        hash=block_data["hash"],
                        parent_hash=block_data["parentHash"],
                        nonce=block_data["nonce"],
                        sha3_uncles=block_data["sha3Uncles"],
                        transactions_root=block_data["transactionsRoot"],
                        state_root=block_data["stateRoot"],
                        receipts_root=block_data["receiptsRoot"],
                        miner=block_data["miner"],
                        size=int(block_data["size"], 16),
                        extra_data=block_data["extraData"],
                        gas_limit=int(block_data["gasLimit"], 16),
                        gas_used=int(block_data["gasUsed"], 16),
                        timestamp=timestamp,
                        transaction_count=len(block_data.get("transactions", [])),
                        base_fee_per_gas=(
                            int(block_data["baseFeePerGas"], 16)
                            if "baseFeePerGas" in block_data
                            else None
                        ),
                    )
                    block_map[block_number] = block

                except Exception as e:
                    self.logger.error(f"Error parsing block {block_number}: {e}")
                    continue

            return block_map

        except Exception as e:
            self.logger.error(f"Error in batch block request: {e}")
            # Return empty dict on error
            return {}

    async def _backfill_missing_blocks(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        missing_blocks: list[int],
        progress: Progress,
        task_id: TaskID,
    ) -> int:
        """Backfill missing blocks via RPC using batched and parallelized requests.

        Args:
            client: HTTP client
            session: Database session
            missing_blocks: List of missing block numbers
            progress: Progress display
            task_id: Progress task ID

        Returns:
            Number of blocks successfully fetched
        """
        if not missing_blocks:
            return 0

        # Split into batches of rpc_batch_size
        batches = [
            missing_blocks[i : i + self.rpc_batch_size]
            for i in range(0, len(missing_blocks), self.rpc_batch_size)
        ]

        all_blocks = {}
        fetched_count = 0

        # Process batches in parallel chunks
        for i in range(0, len(batches), self.parallel_batches):
            parallel_chunk = batches[i : i + self.parallel_batches]

            # Execute multiple batch requests in parallel
            results = await asyncio.gather(
                *[self._batch_fetch_blocks(client, batch) for batch in parallel_chunk]
            )

            # Merge results and store
            for block_map in results:
                all_blocks.update(block_map)

                # Store blocks from this batch
                if block_map:
                    blocks_to_store = list(block_map.values())
                    await self._store_blocks(session, blocks_to_store)
                    fetched_count += len(blocks_to_store)

                    # Update progress
                    progress.update(
                        task_id,
                        advance=len(blocks_to_store),
                        description=f"Fetching missing blocks via RPC ({fetched_count}/{len(missing_blocks)})",
                    )

        return fetched_count

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _backfill_date(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        date: str,
        progress: Progress,
        task_id: TaskID,
    ) -> int:
        """Backfill data for a single date.

        Returns:
            Number of blocks processed
        """
        df = await self._fetch_parquet(client, date)

        if df is None or len(df) == 0:
            progress.update(
                task_id, advance=1, description=f"{date} [yellow]⊘[/yellow] no data"
            )
            # Still add checkpoint with 0 blocks to mark as processed
            return 0

        # Convert to Pydantic models
        blocks = self._dataframe_to_blocks(df)

        # Store in database
        await self._store_blocks(session, blocks)

        # Add checkpoint for this date
        await self._add_checkpoint(session, date, len(blocks))

        progress.update(
            task_id,
            advance=1,
            description=f"{date} [green]✓[/green] {len(blocks):,} blocks",
        )

        return len(blocks)

    async def run_missing_blocks(self) -> None:
        """Find and backfill missing blocks via RPC.

        Checks for missing blocks between min(blocks) and (latest_block - 10).
        """
        if not self.eth_rpc_url:
            self.console.print(
                "[yellow]ETH_RPC_URL not configured - skipping missing blocks check[/yellow]"
            )
            return

        await self.create_tables()

        self.console.print("\n[bold blue]Checking for missing blocks[/bold blue]")
        self.console.print(
            f"[cyan]RPC batch size: {self.rpc_batch_size} blocks/request[/cyan]"
        )
        self.console.print(
            f"[cyan]Parallel batches: {self.parallel_batches} concurrent requests[/cyan]\n"
        )

        async with httpx.AsyncClient() as client:
            # Get latest block number
            latest_block = await self._get_latest_block_number(client)
            block_end = latest_block - 10  # Safety buffer of 10 blocks

            self.console.print(f"[cyan]Latest block: {latest_block:,}[/cyan]")
            self.console.print(
                f"[cyan]Check range end: {block_end:,} (latest - 10)[/cyan]"
            )

            async with AsyncSessionLocal() as session:
                # Get min/max from database
                min_block, max_block = await self._get_min_max_block_numbers(session)

                if min_block == 0:
                    self.console.print(
                        "[yellow]No blocks in database - run date backfill first[/yellow]"
                    )
                    return

                self.console.print(
                    f"[cyan]Database range: {min_block:,} to {max_block:,}[/cyan]"
                )

                # Find missing blocks in range
                self.console.print(
                    f"[cyan]Scanning for gaps between {min_block:,} and {block_end:,}...[/cyan]"
                )
                missing_blocks = await self._find_missing_blocks(
                    session, min_block, block_end
                )

                if not missing_blocks:
                    self.console.print(
                        "[green]✓ No missing blocks found - database is complete![/green]"
                    )
                    return

                self.console.print(
                    f"[yellow]Found {len(missing_blocks):,} missing blocks[/yellow]\n"
                )

                # Create progress display
                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TextColumn("•"),
                    TimeElapsedColumn(),
                    console=self.console,
                    expand=True,
                )

                with progress:
                    task_id = progress.add_task(
                        "Fetching missing blocks via RPC", total=len(missing_blocks)
                    )

                    fetched = await self._backfill_missing_blocks(
                        client, session, missing_blocks, progress, task_id
                    )

                self.console.print(
                    f"\n[bold green]✓ Missing blocks backfill completed[/bold green] - "
                    f"Fetched {fetched:,} blocks"
                )

    async def run(self) -> None:
        """Run the backfill process."""
        await self.create_tables()

        # Generate all dates in range
        all_dates = []
        current_date = self.start_date
        while current_date <= self.end_date:
            all_dates.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        # Get already completed dates
        async with AsyncSessionLocal() as session:
            completed_dates = await self._get_completed_dates(session)

        # Filter out completed dates
        dates_to_process = [date for date in all_dates if date not in completed_dates]

        if not dates_to_process:
            self.console.print(
                "[yellow]No new dates to process - all dates in range already completed[/yellow]"
            )
            return

        total_days = len(dates_to_process)
        completed_count = len(completed_dates)

        self.console.print(
            "[bold blue]Backfilling Ethereum blocks from AWS S3[/bold blue]"
        )
        self.console.print(
            f"[cyan]Date range: {self.start_date} to {self.end_date}[/cyan]"
        )
        self.console.print(f"[cyan]Total dates in range: {len(all_dates):,}[/cyan]")
        self.console.print(f"[cyan]Already completed: {completed_count:,}[/cyan]")
        self.console.print(f"[cyan]To process: {total_days:,}[/cyan]\n")

        # Create progress display
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=self.console,
            expand=True,
        )

        total_blocks = 0

        with progress:
            task_id = progress.add_task("Processing dates", total=total_days)

            async with httpx.AsyncClient() as client:
                async with AsyncSessionLocal() as session:
                    for date_str in dates_to_process[::-1]:
                        blocks_count = await self._backfill_date(
                            client, session, date_str, progress, task_id
                        )
                        total_blocks += blocks_count

        # self.console.print(
        #     f"\n[bold green]✓ Backfill completed[/bold green] - "
        #     f"Processed {total_blocks:,} blocks across {total_days:,} dates"
        # )


if __name__ == "__main__":
    # Example: Backfill from AWS S3 and fill missing blocks via RPC
    backfill = BackfillBlocks(
        start_date="2022-01-01",
        end_date=None,  # Until today
        batch_size=1000,
        eth_rpc_url=None,  # Uses ETH_RPC_URL from .env
        rpc_batch_size=50,  # 50 blocks per JSON-RPC batch request
        parallel_batches=30,  # 30 batch requests in parallel
    )

    # Run both backfills
    async def main():
        # First, backfill from AWS S3 (historical data by date)
        await backfill.run()

        # Then, find and fill missing blocks via RPC
        await backfill.run_missing_blocks()

    run(main())
