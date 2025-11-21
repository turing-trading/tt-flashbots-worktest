"""Backfill extra builder balance data using Ethereum JSON-RPC."""

from typing import TYPE_CHECKING

from asyncio import run

from sqlalchemy import text

import httpx

from src.data.builders.db import ExtraBuilderBalanceDB
from src.data.builders.known_builder_addresses import KNOWN_BUILDER_ADDRESSES
from src.data.builders.models import ExtraBuilderBalance
from src.helpers.backfill import BackfillBase
from src.helpers.config import get_eth_rpc_url
from src.helpers.constants import (
    DB_BATCH_SIZE,
    DEFAULT_PARALLEL_BATCHES,
    RPC_BATCH_SIZE,
)
from src.helpers.db import AsyncSessionLocal, upsert_models
from src.helpers.progress import create_standard_progress
from src.helpers.rpc import RPCClient, batch_get_balance_changes


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from rich.progress import Progress, TaskID


class BackfillExtraBuilderBalances(BackfillBase):
    """Backfill extra builder balance increases by querying Ethereum node."""

    def __init__(
        self,
        eth_rpc_url: str | None = None,
        batch_size: int = RPC_BATCH_SIZE,
        db_batch_size: int = DB_BATCH_SIZE,
        parallel_batches: int = DEFAULT_PARALLEL_BATCHES,
    ) -> None:
        """Initialize backfill.

        Args:
            eth_rpc_url: Ethereum JSON-RPC endpoint (defaults to env var ETH_RPC_URL)
            batch_size: Number of getBalance calls per JSON-RPC batch request
            db_batch_size: Number of records to insert per database batch
            parallel_batches: Number of batch RPC requests to run in parallel

        Raises:
            ValueError: If ETH_RPC_URL is not provided and not set in environment
            variables
        """
        super().__init__(batch_size)
        self.eth_rpc_url = get_eth_rpc_url(eth_rpc_url)
        self.rpc_client = RPCClient(self.eth_rpc_url)
        self.db_batch_size = db_batch_size
        self.parallel_batches = parallel_batches

    async def _get_missing_blocks_count(self, session: AsyncSession) -> int:
        """Get total count of missing blocks for known builder addresses.

        Returns:
            Total number of blocks missing from extra_builder_balance table
        """
        # Build a list of miner addresses to filter
        miner_addresses = list(KNOWN_BUILDER_ADDRESSES.keys())
        if not miner_addresses:
            return 0

        query_str = """
        SELECT COUNT(*)
        FROM blocks b
        WHERE b.miner = ANY(:miner_addresses)
        AND b.number > 0
        AND NOT EXISTS (
            SELECT 1
            FROM extra_builder_balance ebb
            WHERE ebb.block_number = b.number
            AND ebb.miner = b.miner
            LIMIT 1
        )
        """

        result = await session.execute(
            text(query_str), {"miner_addresses": miner_addresses}
        )
        count = result.scalar()
        return count or 0

    async def _get_missing_block_numbers(
        self, session: AsyncSession, limit: int | None = None
    ) -> list[tuple[int, str]]:
        """Get block numbers where miner is in KNOWN_BUILDER_ADDRESSES.

        Args:
            session: Database session
            limit: Max blocks to fetch (capped at 10,000 to minimize DB impact)

        Returns:
            List of (block_number, miner) tuples
        """
        # Build a list of miner addresses to filter
        miner_addresses = list(KNOWN_BUILDER_ADDRESSES.keys())
        if not miner_addresses:
            return []

        # Cap at 10,000 to minimize database impact
        actual_limit = min(limit, 10_000) if limit else 10_000

        query_str = """
        SELECT b.number, b.miner
        FROM blocks b
        WHERE b.miner = ANY(:miner_addresses)
        AND b.number > 0
        AND NOT EXISTS (
            SELECT 1
            FROM extra_builder_balance ebb
            WHERE ebb.block_number = b.number
            AND ebb.miner = b.miner
            LIMIT 1
        )
        ORDER BY b.number DESC
        LIMIT :limit
        """

        result = await session.execute(
            text(query_str),
            {"miner_addresses": miner_addresses, "limit": actual_limit},
        )
        rows = result.fetchall()
        return [(row[0], row[1]) for row in rows]

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

        # Build list of (address, block_number) pairs for all builder addresses
        address_block_pairs: list[tuple[str, int]] = []
        for block_number, miner in blocks:
            builder_addresses = KNOWN_BUILDER_ADDRESSES.get(miner, [])
            address_block_pairs.extend(
                (builder_address, block_number) for builder_address in builder_addresses
            )

        # Use helper function to get all balance changes
        changes = await batch_get_balance_changes(
            self.rpc_client,
            client,
            address_block_pairs,
            batch_size=self.batch_size,
            parallel_batches=self.parallel_batches,
        )

        # Create ExtraBuilderBalance records
        builder_balances: list[ExtraBuilderBalance] = []
        for block_number, miner in blocks:
            builder_addresses = KNOWN_BUILDER_ADDRESSES.get(miner, [])
            for builder_address in builder_addresses:
                balance_before, balance_after, balance_increase = changes.get(
                    (builder_address, block_number), (0, 0, 0)
                )

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
        await self._store_builder_balances(builder_balances)

        # Update progress
        progress.update(task_id, advance=len(blocks))

        return len(blocks)

    async def _store_builder_balances(
        self, balances: list[ExtraBuilderBalance]
    ) -> None:
        """Store builder balances in the database."""
        if not balances:
            return

        await upsert_models(
            db_model_class=ExtraBuilderBalanceDB,
            pydantic_models=balances,
        )

    async def run(self, limit: int | None = None) -> None:
        """Run the backfill process.

        Args:
            limit: Maximum number of blocks to process per iteration (None = all
            missing blocks)

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
            f"[cyan]Parallel batches: {self.parallel_batches} "
            "concurrent requests[/cyan]"
        )
        self.console.print(f"[cyan]DB batch size: {self.db_batch_size} records[/cyan]")
        self.console.print(
            f"[cyan]Known builder addresses: {len(KNOWN_BUILDER_ADDRESSES)} "
            "miners[/cyan]"
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
        overall_progress = create_standard_progress(console=self.console, expand=True)

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
    backfill = BackfillExtraBuilderBalances(
        batch_size=10,  # 10 balance queries per JSON-RPC batch
        db_batch_size=100,  # 100 records per DB insert
        parallel_batches=100,  # 100 batch requests in parallel
    )
    run(backfill.run(limit=None))  # Process all missing blocks
