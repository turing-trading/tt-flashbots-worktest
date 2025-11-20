"""Backfill builder balance data using Ethereum JSON-RPC."""

import os

from typing import TYPE_CHECKING

import asyncio
from asyncio import run

from sqlalchemy import text

from dotenv import load_dotenv
import httpx
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

from src.data.builders.db import BuilderBalancesDB
from src.data.builders.models import BuilderBalance
from src.helpers.db import AsyncSessionLocal, Base, async_engine, upsert_models


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


load_dotenv()


class BackfillBuilderBalancesDelivered:
    """Backfill builder balance increases by querying Ethereum node."""

    def __init__(
        self,
        eth_rpc_url: str | None = None,
        batch_size: int = 10,
        db_batch_size: int = 100,
        parallel_batches: int = 5,
    ) -> None:
        """Initialize backfill.

        Args:
            eth_rpc_url: Ethereum JSON-RPC endpoint (defaults to env var ETH_RPC_URL)
            batch_size: Number of getBalance calls per JSON-RPC batch request
            db_batch_size: Number of records to insert per database batch
            parallel_batches: Number of batch RPC requests to run in parallel

        Raises:
            ValueError: If ETH_RPC_URL is not provided and not set in environment
        """
        self.eth_rpc_url = eth_rpc_url or os.getenv("ETH_RPC_URL")
        if not self.eth_rpc_url:
            msg = "ETH_RPC_URL must be provided or set in environment variables"
            raise ValueError(msg)

        self.batch_size = batch_size
        self.db_batch_size = db_batch_size
        self.parallel_batches = parallel_batches
        self.console = Console()

    async def _get_missing_blocks_count(self, session: AsyncSession) -> int:
        """Get total count of missing blocks.

        Returns:
            Total number of blocks missing from builder_balance table
        """
        query_str = """
        SELECT COUNT(*)
        FROM blocks b
        LEFT JOIN builder_balance mb ON b.number = mb.block_number
        WHERE mb.block_number IS NULL
        AND b.number > 0
        """

        result = await session.execute(text(query_str))
        count = result.scalar()
        return count or 0

    async def _get_missing_block_numbers(
        self, session: AsyncSession, limit: int | None = None
    ) -> list[tuple[int, str]]:
        """Get block numbers that exist in blocks but not in builder_balance.

        Args:
            session: Database session
            limit: Max blocks to fetch (capped at 10,000 to minimize DB impact)

        Returns:
            List of (block_number, builder) tuples
        """
        # Query blocks that don't have corresponding builder balance records
        # Order by DESC to process most recent blocks first
        query_str = """
        SELECT b.number, b.miner
        FROM blocks b
        LEFT JOIN builder_balance mb ON b.number = mb.block_number
        WHERE mb.block_number IS NULL
        AND b.number > 0
        ORDER BY b.number DESC
        """

        # Cap at 10,000 to minimize database impact
        actual_limit = min(limit, 10_000) if limit else 10_000
        query_str += f" LIMIT {actual_limit}"

        result = await session.execute(text(query_str))
        rows = result.fetchall()
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

        Raises:
            ValueError: If ETH_RPC_URL is not provided and not set in environment
        """
        # Build batch JSON-RPC request
        batch_payload = []
        if not self.eth_rpc_url:
            msg = "ETH_RPC_URL must be provided or set in environment variables"
            raise ValueError(msg)

        for idx, (address, block_number) in enumerate(requests):
            batch_payload.append({
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [address, hex(block_number)],
                "id": idx,
            })

        try:
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
                    balance_map[address, block_number] = balance
                else:
                    balance_map[address, block_number] = 0

            return balance_map

        except Exception:
            # Return zeros for all requests on error
            return dict.fromkeys(requests, 0)

    async def _process_blocks_batch(
        self,
        client: httpx.AsyncClient,
        blocks: list[tuple[int, str]],
        progress: Progress,
        task_id: TaskID,
    ) -> int:
        """Process a batch of blocks to calculate builder balance increases.

        Args:
            client: HTTP client
            blocks: List of (block_number, miner) tuples
            progress: Progress bar
            task_id: Progress task ID

        Returns:
            Number of records processed
        """
        if not blocks:
            return 0

        # Build requests for all balances we need
        # For each block N, we need balance at N-1 and N
        balance_requests = []
        for block_number, builder in blocks:
            balance_requests.extend((
                (builder, block_number - 1),  # Balance before
                (builder, block_number),  # Balance after
            ))

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
            results = await asyncio.gather(*[
                self._batch_get_balances(client, batch) for batch in parallel_chunk
            ])

            # Merge results
            for balances in results:
                all_balances.update(balances)

        # Create BuilderBalance records
        builder_balances = []
        for block_number, builder in blocks:
            balance_before = all_balances.get((builder, block_number - 1), 0)
            balance_after = all_balances.get((builder, block_number), 0)
            balance_increase = balance_after - balance_before

            builder_balance = BuilderBalance(
                block_number=block_number,
                miner=builder,
                balance_before=balance_before,
                balance_after=balance_after,
                balance_increase=balance_increase,
            )
            builder_balances.append(builder_balance)

        # Store in database
        await self._store_builder_balances(builder_balances)

        # Update progress
        progress.update(task_id, advance=len(blocks))

        return len(blocks)

    async def _store_builder_balances(
        self, balances: list[BuilderBalance]
    ) -> None:
        """Store builder balances in the database."""
        if not balances:
            return

        await upsert_models(
            db_model_class=BuilderBalancesDB,
            pydantic_models=balances,
        )

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def run(self, limit: int | None = None) -> None:
        """Run the backfill process.

        Args:
            limit: Maximum number of blocks to process per iteration
                (None = all missing blocks)

        Note:
            Automatically iterates over 10K blocks at a time until all missing blocks
            are processed, regardless of the limit parameter.
        """
        self.console.print("[cyan]Creating tables if not exist...[/cyan]")
        await self.create_tables()

        self.console.print(
            "[bold blue]Backfilling builder balance increases[/bold blue]"
        )
        self.console.print(f"[cyan]Ethereum RPC: {self.eth_rpc_url}[/cyan]")
        self.console.print(
            f"[cyan]Batch size: {self.batch_size} balances/request[/cyan]"
        )
        self.console.print(
            f"[cyan]Parallel batches: {self.parallel_batches} "
            "concurrent requests[/cyan]"
        )
        self.console.print(f"[cyan]DB batch size: {self.db_batch_size} records[/cyan]")
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

                async with (
                    httpx.AsyncClient() as client,
                    AsyncSessionLocal() as session,
                ):
                    # Process in batches
                    for i in range(0, len(missing_blocks), self.db_batch_size):
                        batch = missing_blocks[i : i + self.db_batch_size]

                        processed = await self._process_blocks_batch(
                            client, batch, overall_progress, overall_task
                        )
                        total_processed += processed

        self.console.print(
            f"\n[bold green]✓ Backfill completed[/bold green] - "
            f"Processed {total_processed:,} blocks across {iteration} iterations"
        )


if __name__ == "__main__":
    # Example: Backfill all missing blocks
    backfill = BackfillBuilderBalancesDelivered(
        batch_size=10,  # 10 balance queries per JSON-RPC batch
        db_batch_size=100,  # 100 records per DB insert
        parallel_batches=100,  # 100 batch requests in parallel
    )
    run(backfill.run(limit=None))  # Process all missing blocks
