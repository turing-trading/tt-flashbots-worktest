"""Backfill extra builder balance data using Ethereum JSON-RPC."""

import asyncio
import os
from asyncio import run

import httpx
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
    TimeRemainingColumn,
)
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.builders.db import ExtraBuilderBalanceDB
from src.data.builders.known_builder_addresses import KNOWN_BUILDER_ADDRESSES
from src.data.builders.models import ExtraBuilderBalance
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger

load_dotenv()


class BackfillExtraBuilderBalances:
    """Backfill extra builder balance increases by querying Ethereum node."""

    def __init__(
        self,
        eth_rpc_url: str | None = None,
        batch_size: int = 10,
        db_batch_size: int = 100,
        parallel_batches: int = 5,
    ):
        """Initialize backfill.

        Args:
            eth_rpc_url: Ethereum JSON-RPC endpoint (defaults to env var ETH_RPC_URL)
            batch_size: Number of getBalance calls per JSON-RPC batch request
            db_batch_size: Number of records to insert per database batch
            parallel_batches: Number of batch RPC requests to run in parallel
        """
        self.eth_rpc_url = eth_rpc_url or os.getenv("ETH_RPC_URL")
        if not self.eth_rpc_url:
            raise ValueError(
                "ETH_RPC_URL must be provided or set in environment variables"
            )

        self.batch_size = batch_size
        self.db_batch_size = db_batch_size
        self.parallel_batches = parallel_batches
        self.logger = get_logger("backfill_extra_builder_balances", log_level="INFO")
        self.console = Console()

    async def _get_missing_blocks_count(self, session: AsyncSession) -> int:
        """Get total count of missing blocks for known builder addresses.

        Returns:
            Total number of blocks missing from extra_builder_balance table
        """
        # Build a list of miner addresses to filter
        miner_addresses = list(KNOWN_BUILDER_ADDRESSES.keys())
        if not miner_addresses:
            return 0

        # Format as SQL array literal
        miner_list = ", ".join([f"'{addr}'" for addr in miner_addresses])

        query_str = f"""
        SELECT COUNT(*)
        FROM blocks b
        WHERE b.miner IN ({miner_list})
        AND b.number > 0
        AND NOT EXISTS (
            SELECT 1
            FROM extra_builder_balance ebb
            WHERE ebb.block_number = b.number
            AND ebb.miner = b.miner
            LIMIT 1
        )
        """

        result = await session.execute(text(query_str))
        count = result.scalar()
        return count or 0

    async def _get_missing_block_numbers(
        self, session: AsyncSession, limit: int | None = None
    ) -> list[tuple[int, str]]:
        """Get block numbers where miner is in KNOWN_BUILDER_ADDRESSES but not in extra_builder_balance.

        Args:
            limit: Max blocks to fetch (capped at 10,000 to minimize DB impact)

        Returns:
            List of (block_number, miner) tuples
        """
        # Build a list of miner addresses to filter
        miner_addresses = list(KNOWN_BUILDER_ADDRESSES.keys())
        if not miner_addresses:
            return []

        # Format as SQL array literal
        miner_list = ", ".join([f"'{addr}'" for addr in miner_addresses])

        query_str = f"""
        SELECT b.number, b.miner
        FROM blocks b
        WHERE b.miner IN ({miner_list})
        AND b.number > 0
        AND NOT EXISTS (
            SELECT 1
            FROM extra_builder_balance ebb
            WHERE ebb.block_number = b.number
            AND ebb.miner = b.miner
            LIMIT 1
        )
        ORDER BY b.number DESC
        """

        # Cap at 10,000 to minimize database impact
        actual_limit = min(limit, 10_000) if limit else 10_000
        query_str += f" LIMIT {actual_limit}"

        self.logger.debug(
            f"Executing query to find missing blocks (limit: {actual_limit:,})..."
        )
        result = await session.execute(text(query_str))
        rows = result.fetchall()
        self.logger.debug(f"Query returned {len(rows)} missing blocks")
        return [(row[0], row[1]) for row in rows]

    async def _batch_get_balances(
        self,
        client: httpx.AsyncClient,
        requests: list[tuple[str, int]],
    ) -> dict[tuple[str, int], int]:
        """Batch multiple eth_getBalance calls into a single JSON-RPC request.

        Args:
            client: HTTP client
            requests: List of (address, block_number) tuples

        Returns:
            Dict mapping (address, block_number) to balance in wei
        """
        # Build batch JSON-RPC request
        batch_payload = []
        for idx, (address, block_number) in enumerate(requests):
            batch_payload.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [address, hex(block_number)],
                    "id": idx,
                }
            )

        try:
            # eth_rpc_url is guaranteed to be str (validated in __init__)
            assert self.eth_rpc_url is not None

            # Log first RPC call for debugging
            if requests:
                self.logger.debug(
                    f"Fetching {len(requests)} balances (blocks {requests[0][1]} to {requests[-1][1]})"
                )

            response = await client.post(
                self.eth_rpc_url,
                json=batch_payload,
                timeout=60.0,  # Increased timeout for batch requests
            )
            response.raise_for_status()

            results = response.json()

            # Map results back to (address, block_number)
            balance_map = {}
            for result in results:
                idx = result["id"]
                address, block_number = requests[idx]

                if "result" in result:
                    # Convert hex balance to int
                    balance = int(result["result"], 16)
                    balance_map[(address, block_number)] = balance
                else:
                    self.logger.error(
                        f"Error getting balance for {address} at block {block_number}: "
                        f"{result.get('error', 'Unknown error')}"
                    )
                    balance_map[(address, block_number)] = 0

            return balance_map

        except Exception as e:
            self.logger.error(f"Error in batch balance request: {e}")
            # Return zeros for all requests on error
            return dict.fromkeys(requests, 0)

    async def _process_blocks_batch(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        blocks: list[tuple[int, str]],
        progress: Progress,
        task_id: TaskID,
    ) -> int:
        """Process a batch of blocks to calculate builder balance increases.

        Args:
            client: HTTP client
            session: Database session
            blocks: List of (block_number, miner) tuples
            progress: Progress bar
            task_id: Progress task ID

        Returns:
            Number of records processed
        """
        if not blocks:
            return 0

        # Build requests for all balances we need
        # For each block N with known miner, we need balances for all builder addresses at N-1 and N
        balance_requests = []
        for block_number, miner in blocks:
            # Get builder addresses for this miner
            builder_addresses = KNOWN_BUILDER_ADDRESSES.get(miner, [])
            for builder_address in builder_addresses:
                balance_requests.append(
                    (builder_address, block_number - 1)
                )  # Balance before
                balance_requests.append(
                    (builder_address, block_number)
                )  # Balance after

        # Process balance requests in parallel batches
        all_balances = {}

        # Split into batches
        batches = [
            balance_requests[i : i + self.batch_size]
            for i in range(0, len(balance_requests), self.batch_size)
        ]

        # Process batches in parallel chunks
        for i in range(0, len(batches), self.parallel_batches):
            parallel_chunk = batches[i : i + self.parallel_batches]

            # Execute multiple batch requests in parallel
            results = await asyncio.gather(
                *[self._batch_get_balances(client, batch) for batch in parallel_chunk]
            )

            # Merge results
            for balances in results:
                all_balances.update(balances)

        # Create ExtraBuilderBalance records
        builder_balances = []
        for block_number, miner in blocks:
            builder_addresses = KNOWN_BUILDER_ADDRESSES.get(miner, [])
            for builder_address in builder_addresses:
                balance_before = all_balances.get(
                    (builder_address, block_number - 1), 0
                )
                balance_after = all_balances.get((builder_address, block_number), 0)
                balance_increase = balance_after - balance_before

                builder_balance = ExtraBuilderBalance(
                    block_number=block_number,
                    builder_address=builder_address,
                    miner=miner,
                    balance_before=balance_before,
                    balance_after=balance_after,
                    balance_increase=balance_increase,
                )
                builder_balances.append(builder_balance)

        # Store in database
        await self._store_builder_balances(session, builder_balances)

        # Update progress
        progress.update(task_id, advance=len(blocks))

        return len(blocks)

    async def _store_builder_balances(
        self, session: AsyncSession, balances: list[ExtraBuilderBalance]
    ) -> None:
        """Store builder balances in the database."""
        if not balances:
            return

        values = [balance.model_dump() for balance in balances]

        # Upsert using ON CONFLICT
        stmt = pg_insert(ExtraBuilderBalanceDB).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["block_number", "builder_address"],
            set_={
                ExtraBuilderBalanceDB.miner: stmt.excluded.miner,
                ExtraBuilderBalanceDB.balance_before: stmt.excluded.balance_before,
                ExtraBuilderBalanceDB.balance_after: stmt.excluded.balance_after,
                ExtraBuilderBalanceDB.balance_increase: stmt.excluded.balance_increase,
            },
        )

        await session.execute(stmt)
        await session.commit()

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def run(self, limit: int | None = None) -> None:
        """Run the backfill process.

        Args:
            limit: Maximum number of blocks to process per iteration (None = all missing blocks)

        Note:
            Automatically iterates over 10K blocks at a time until all missing blocks
            are processed, regardless of the limit parameter.
        """
        self.console.print("[cyan]Creating tables if not exist...[/cyan]")
        await self.create_tables()

        self.console.print(
            "[bold blue]Backfilling extra builder balance increases[/bold blue]"
        )
        self.console.print(f"[cyan]Ethereum RPC: {self.eth_rpc_url}[/cyan]")
        self.console.print(
            f"[cyan]Batch size: {self.batch_size} balances/request[/cyan]"
        )
        self.console.print(
            f"[cyan]Parallel batches: {self.parallel_batches} concurrent requests[/cyan]"
        )
        self.console.print(f"[cyan]DB batch size: {self.db_batch_size} records[/cyan]")
        self.console.print(
            f"[cyan]Known builder addresses: {len(KNOWN_BUILDER_ADDRESSES)} miners[/cyan]"
        )
        self.console.print("[cyan]Query limit: 10,000 blocks per iteration[/cyan]\n")

        # Query total missing blocks upfront
        self.console.print("[cyan]Querying total missing blocks...[/cyan]")
        async with AsyncSessionLocal() as session:
            total_missing = await self._get_missing_blocks_count(session)

        if total_missing == 0:
            self.console.print(
                "[yellow]No missing blocks - backfill complete![/yellow]"
            )
            return

        self.console.print(
            f"[green]Found {total_missing:,} total missing blocks[/green]\n"
        )

        # Create overall progress display
        overall_progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=self.console,
            expand=True,
        )

        total_processed = 0
        iteration = 0

        with overall_progress:
            overall_task = overall_progress.add_task(
                "Overall Progress", total=total_missing
            )

            # Keep processing until no more missing blocks
            while True:
                iteration += 1

                async with AsyncSessionLocal() as session:
                    # Get next batch of missing blocks (max 10K)
                    missing_blocks = await self._get_missing_block_numbers(
                        session, limit
                    )

                if not missing_blocks:
                    break

                async with httpx.AsyncClient() as client:
                    async with AsyncSessionLocal() as session:
                        # Process in batches
                        for i in range(0, len(missing_blocks), self.db_batch_size):
                            batch = missing_blocks[i : i + self.db_batch_size]

                            processed = await self._process_blocks_batch(
                                client, session, batch, overall_progress, overall_task
                            )
                            total_processed += processed

        self.console.print(
            f"\n[bold green]✓ Backfill completed[/bold green] - "
            f"Processed {total_processed:,} blocks across {iteration} iterations"
        )


if __name__ == "__main__":
    # Example: Backfill all missing blocks
    backfill = BackfillExtraBuilderBalances(
        batch_size=10,  # 10 balance queries per JSON-RPC batch
        db_batch_size=100,  # 100 records per DB insert
        parallel_batches=100,  # 100 batch requests in parallel
    )
    run(backfill.run(limit=None))  # Process all missing blocks
