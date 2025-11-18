"""Backfill PBS analysis data by aggregating from blocks, proposers_balance, and relays_payloads."""

from asyncio import run
from datetime import datetime

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
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.analysis.constants import clean_builder_name
from src.analysis.db import AnalysisPBSDB
from src.data.blocks.db import BlockDB
from src.data.builders.db import BuilderIdentifiersDB
from src.data.proposers.db import ProposerBalancesDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger
from src.helpers.parsers import wei_to_eth

START_DATE = datetime(2022, 1, 1, 0, 0, 0)


class BackfillAnalysisPBS:
    """Backfill PBS analysis data."""

    def __init__(self, batch_size: int = 10_000):
        """Initialize backfill.

        Args:
            batch_size: Number of blocks to process per batch
        """
        self.batch_size = batch_size
        self.logger = get_logger("backfill_analysis_pbs", log_level="INFO")
        self.console = Console()

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _get_missing_blocks(self, session: AsyncSession) -> list[tuple[int, ...]]:
        """Get block numbers that exist in blocks table but not in analysis_pbs.


        Returns:
            List of block numbers that need to be processed
        """
        # Use NOT EXISTS for better performance on large tables
        subquery = select(AnalysisPBSDB.block_number).where(
            AnalysisPBSDB.block_number == BlockDB.number
        )

        stmt = (
            select(BlockDB.number)
            .where(~subquery.exists())
            .where(BlockDB.timestamp >= START_DATE)
            .order_by(BlockDB.number.desc())
        )

        result = await session.execute(stmt)
        missing_blocks = result.fetchall()
        # Type ignore: SQLAlchemy Row objects are compatible with tuple
        return list(missing_blocks)  # type: ignore

    async def _get_block_count(self, session: AsyncSession) -> int:
        """Get total count of blocks in the blocks table from START_DATE onwards."""
        stmt = (
            select(func.count())
            .select_from(BlockDB)
            .where(BlockDB.timestamp >= START_DATE)
        )
        result = await session.execute(stmt)
        return result.scalar_one()

    async def _aggregate_block_data(
        self, session: AsyncSession, block_numbers: list[int]
    ) -> list[dict]:
        """Aggregate data from blocks, proposers_balance, and relays_payloads for given block numbers.

        Args:
            session: Database session
            block_numbers: List of block numbers to aggregate

        Returns:
            List of dictionaries with aggregated data
        """
        self.logger.debug(f"Aggregating data for {len(block_numbers)} blocks")

        # Build the aggregation query with builder_name from builders_identifiers
        # We need to get the builder_pubkey from relays_payloads first, then join with builders_identifiers
        stmt = (
            select(
                BlockDB.number.label("block_number"),
                BlockDB.timestamp.label("block_timestamp"),
                ProposerBalancesDB.balance_increase.label("builder_balance_increase"),
                func.array_agg(RelaysPayloadsDB.relay).label("relays"),
                func.max(RelaysPayloadsDB.value).label("proposer_subsidy"),
                func.max(BuilderIdentifiersDB.builder_name).label("builder_name"),
            )
            .select_from(BlockDB)
            .outerjoin(
                ProposerBalancesDB,
                BlockDB.number == ProposerBalancesDB.block_number,
            )
            .outerjoin(
                RelaysPayloadsDB, BlockDB.number == RelaysPayloadsDB.block_number
            )
            .outerjoin(
                BuilderIdentifiersDB,
                RelaysPayloadsDB.builder_pubkey == BuilderIdentifiersDB.builder_pubkey,
            )
            .where(BlockDB.number.in_(block_numbers))
            .group_by(
                BlockDB.number,
                BlockDB.timestamp,
                ProposerBalancesDB.balance_increase,
            )
        )

        self.logger.debug("Executing aggregation query...")
        result = await session.execute(stmt)
        rows = result.fetchall()
        self.logger.debug(f"Got {len(rows)} rows from aggregation")

        # Convert to dictionaries and convert Wei to ETH
        aggregated_data = []
        for row in rows:
            # Filter out None values from relays array
            relays = [r for r in (row.relays or []) if r is not None]

            # Convert Wei to ETH using helper
            builder_balance_increase = wei_to_eth(row.builder_balance_increase)
            proposer_subsidy = wei_to_eth(row.proposer_subsidy)

            aggregated_data.append(
                {
                    "block_number": row.block_number,
                    "block_timestamp": row.block_timestamp,
                    "builder_balance_increase": builder_balance_increase,
                    "relays": relays if relays else None,
                    "proposer_subsidy": proposer_subsidy,
                    "builder_name": clean_builder_name(row.builder_name),
                }
            )

        return aggregated_data

    async def _store_analysis_data(
        self, session: AsyncSession, data: list[dict]
    ) -> None:
        """Store aggregated analysis data in the database.

        Args:
            session: Database session
            data: List of aggregated data dictionaries
        """
        if not data:
            self.logger.debug("No data to store, skipping")
            return

        self.logger.debug(f"Storing {len(data)} rows...")

        # Upsert data into analysis_pbs table
        stmt = pg_insert(AnalysisPBSDB).values(data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["block_number"],
            set_={
                AnalysisPBSDB.block_timestamp: stmt.excluded.block_timestamp,
                AnalysisPBSDB.builder_balance_increase: stmt.excluded.builder_balance_increase,
                AnalysisPBSDB.relays: stmt.excluded.relays,
                AnalysisPBSDB.proposer_subsidy: stmt.excluded.proposer_subsidy,
                AnalysisPBSDB.builder_name: stmt.excluded.builder_name,
            },
        )

        await session.execute(stmt)
        await session.commit()
        self.logger.debug("Data stored successfully")

    async def _process_batch(
        self,
        session: AsyncSession,
        block_numbers: list[int],
        progress: Progress,
        task_id: TaskID,
    ) -> int:
        """Process a batch of blocks.

        Args:
            session: Database session
            block_numbers: List of block numbers to process
            progress: Progress display
            task_id: Progress task ID

        Returns:
            Number of blocks processed
        """
        # Aggregate data for this batch
        aggregated_data = await self._aggregate_block_data(session, block_numbers)

        # Store aggregated data
        await self._store_analysis_data(session, aggregated_data)

        # Update progress
        progress.update(task_id, advance=len(block_numbers))

        return len(block_numbers)

    async def run(self) -> None:
        """Run the backfill process."""
        self.logger.info("Creating tables if they don't exist...")
        await self.create_tables()

        async with AsyncSessionLocal() as session:
            # Get total block count
            self.logger.info("Counting total blocks...")
            total_blocks = await self._get_block_count(session)
            self.logger.info(f"Total blocks in database: {total_blocks:,}")

            # Get missing blocks
            self.logger.info("Finding missing blocks...")
            missing_blocks_result = await self._get_missing_blocks(session)
            missing_blocks = [row[0] for row in missing_blocks_result]
            self.logger.info(f"Found {len(missing_blocks):,} missing blocks")

            if not missing_blocks:
                self.console.print(
                    "[yellow]No missing blocks to process - analysis_pbs is up to date[/yellow]"
                )
                return

            total_missing = len(missing_blocks)

            self.console.print("[bold blue]Backfilling PBS Analysis Data[/bold blue]")
            self.console.print(f"[cyan]Total blocks in range: {total_blocks:,}[/cyan]")
            self.console.print(
                f"[cyan]Missing blocks to process: {total_missing:,}[/cyan]\n"
            )

            # Create progress display
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
                console=self.console,
            )

            total_processed = 0

            with progress:
                task_id = progress.add_task("Processing blocks", total=total_missing)

                # Process in batches
                for i in range(0, total_missing, self.batch_size):
                    batch = missing_blocks[i : i + self.batch_size]
                    processed = await self._process_batch(
                        session, batch, progress, task_id
                    )
                    total_processed += processed

            self.console.print(
                f"\n[bold green]✓ Backfill completed[/bold green] - "
                f"Processed {total_processed:,} blocks"
            )


if __name__ == "__main__":
    backfill = BackfillAnalysisPBS(batch_size=10000)
    run(backfill.run())
