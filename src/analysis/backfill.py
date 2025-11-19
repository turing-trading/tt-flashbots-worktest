"""Backfill PBS analysis data by aggregating from blocks, proposers_balance, and relays_payloads."""

from asyncio import run
from datetime import datetime, timedelta

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

from src.analysis.builder_name import parse_builder_name_from_extra_data
from src.analysis.db import AnalysisPBSV2DB
from src.data.blocks.db import BlockDB
from src.data.proposers.db import ProposerBalancesDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger
from src.helpers.parsers import wei_to_eth

# {"from":"2025-10-10 20:28:28","to":"2025-11-07 13:30:57"}
# START_DATE = datetime(2025, 10, 10, 20, 28, 28)
# END_DATE = datetime(2025, 11, 7, 13, 30, 57)

START_DATE = datetime.now() - timedelta(days=1)
END_DATE = datetime.now() - timedelta(minutes=10)


class BackfillAnalysisPBSV2:
    """Backfill PBS analysis data V2 with computed fields."""

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
        """Get block numbers that exist in blocks table but not in analysis_pbs_v2.


        Returns:
            List of block numbers that need to be processed
        """
        # Use NOT EXISTS for better performance on large tables
        subquery = select(AnalysisPBSV2DB.block_number).where(
            AnalysisPBSV2DB.block_number == BlockDB.number
        )

        stmt = select(BlockDB.number).where(~subquery.exists())

        # Add END_DATE filter if specified
        if END_DATE is not None:
            stmt = stmt.where(BlockDB.timestamp <= END_DATE)

        stmt = stmt.order_by(BlockDB.number.desc())

        result = await session.execute(stmt)
        missing_blocks = result.fetchall()
        # Type ignore: SQLAlchemy Row objects are compatible with tuple
        return list(missing_blocks)  # type: ignore

    async def _get_blocks_in_range(
        self, session: AsyncSession
    ) -> list[tuple[int, ...]]:
        """Get ALL block numbers in the date range (for overwrite mode).

        Returns:
            List of block numbers in the date range
        """
        stmt = select(BlockDB.number).where(BlockDB.timestamp >= START_DATE)

        # Add END_DATE filter if specified
        if END_DATE is not None:
            stmt = stmt.where(BlockDB.timestamp <= END_DATE)

        stmt = stmt.order_by(BlockDB.number.desc())

        result = await session.execute(stmt)
        blocks = result.fetchall()
        # Type ignore: SQLAlchemy Row objects are compatible with tuple
        return list(blocks)  # type: ignore

    async def _get_block_count(self, session: AsyncSession) -> int:
        """Get total count of blocks in the blocks table from START_DATE onwards."""
        stmt = (
            select(func.count())
            .select_from(BlockDB)
            .where(BlockDB.timestamp >= START_DATE)
        )

        # Add END_DATE filter if specified
        if END_DATE is not None:
            stmt = stmt.where(BlockDB.timestamp <= END_DATE)

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

        # Build the aggregation query with extra_data for parsing builder name
        stmt = (
            select(
                BlockDB.number.label("block_number"),
                BlockDB.timestamp.label("block_timestamp"),
                BlockDB.extra_data.label("extra_data"),
                ProposerBalancesDB.balance_increase.label("builder_balance_increase"),
                func.array_agg(RelaysPayloadsDB.relay).label("relays"),
                func.max(RelaysPayloadsDB.value).label("proposer_subsidy"),
            )
            .select_from(BlockDB)
            .outerjoin(
                ProposerBalancesDB,
                BlockDB.number == ProposerBalancesDB.block_number,
            )
            .outerjoin(
                RelaysPayloadsDB, BlockDB.number == RelaysPayloadsDB.block_number
            )
            .where(BlockDB.number.in_(block_numbers))
            .group_by(
                BlockDB.number,
                BlockDB.timestamp,
                BlockDB.extra_data,
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
            relays_list = relays if relays else None

            # Calculate computed fields
            n_relays = len(relays) if relays else 0
            is_block_vanilla = n_relays == 0

            # Convert Wei to ETH using helper with defaults
            builder_balance_increase = wei_to_eth(row.builder_balance_increase) or 0.0
            proposer_subsidy = wei_to_eth(row.proposer_subsidy) or 0.0
            total_value = builder_balance_increase + proposer_subsidy

            # Parse builder name directly from extra_data
            builder_name = parse_builder_name_from_extra_data(row.extra_data)

            aggregated_data.append(
                {
                    "block_number": row.block_number,
                    "block_timestamp": row.block_timestamp,
                    "builder_balance_increase": builder_balance_increase,
                    "proposer_subsidy": proposer_subsidy,
                    "total_value": total_value,
                    "is_block_vanilla": is_block_vanilla,
                    "n_relays": n_relays,
                    "relays": relays_list,
                    "builder_name": builder_name,
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

        # Upsert data into analysis_pbs_v2 table
        stmt = pg_insert(AnalysisPBSV2DB).values(data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["block_number"],
            set_={
                AnalysisPBSV2DB.block_timestamp: stmt.excluded.block_timestamp,
                AnalysisPBSV2DB.builder_balance_increase: stmt.excluded.builder_balance_increase,
                AnalysisPBSV2DB.proposer_subsidy: stmt.excluded.proposer_subsidy,
                AnalysisPBSV2DB.total_value: stmt.excluded.total_value,
                AnalysisPBSV2DB.is_block_vanilla: stmt.excluded.is_block_vanilla,
                AnalysisPBSV2DB.n_relays: stmt.excluded.n_relays,
                AnalysisPBSV2DB.relays: stmt.excluded.relays,
                AnalysisPBSV2DB.builder_name: stmt.excluded.builder_name,
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

        # Determine mode based on END_DATE
        overwrite_mode = END_DATE is not None

        async with AsyncSessionLocal() as session:
            # Get blocks to process based on mode
            if overwrite_mode:
                self.logger.info("Counting total blocks...")
                total_blocks = await self._get_block_count(session)
                self.logger.info(f"Total blocks in database: {total_blocks:,}")

                self.logger.info("OVERWRITE MODE: Getting all blocks in range...")
                self.logger.info(f"Date range: {START_DATE} to {END_DATE}")
                blocks_result = await self._get_blocks_in_range(session)
                blocks_to_process = [row[0] for row in blocks_result]
                self.logger.info(f"Found {len(blocks_to_process):,} blocks in range")
                mode_description = "Overwriting all blocks in range"
            else:
                self.logger.info("Finding missing blocks...")
                blocks_result = await self._get_missing_blocks(session)
                blocks_to_process = [row[0] for row in blocks_result]
                self.logger.info(f"Found {len(blocks_to_process):,} missing blocks")
                mode_description = "Backfilling missing blocks"

            if not blocks_to_process:
                if overwrite_mode:
                    self.console.print(
                        "[yellow]No blocks found in specified date range[/yellow]"
                    )
                else:
                    self.console.print(
                        "[yellow]No missing blocks to process - analysis_pbs_v2 is up to date[/yellow]"
                    )
                return

            total_to_process = len(blocks_to_process)

            # Display mode information
            self.console.print("[bold blue]Backfilling PBS Analysis Data[/bold blue]")
            if overwrite_mode:
                self.console.print(
                    "[yellow]Mode: OVERWRITE (END_DATE specified)[/yellow]"
                )
                self.console.print(
                    f"[cyan]Date range: {START_DATE} to {END_DATE}[/cyan]"
                )
            else:
                self.console.print(
                    "[cyan]Mode: Incremental (missing blocks only)[/cyan]"
                )
                self.console.print(f"[cyan]Start date: {START_DATE}[/cyan]")

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
                task_id = progress.add_task(mode_description, total=total_to_process)

                # Process in batches
                for i in range(0, total_to_process, self.batch_size):
                    batch = blocks_to_process[i : i + self.batch_size]
                    processed = await self._process_batch(
                        session, batch, progress, task_id
                    )
                    total_processed += processed

            self.console.print(
                f"\n[bold green]✓ Backfill completed[/bold green] - "
                f"Processed {total_processed:,} blocks"
            )


if __name__ == "__main__":
    # Batch size limited to 5000 to avoid PostgreSQL's 65535 parameter limit
    # (5000 blocks * 9 fields = 45000 parameters)
    backfill = BackfillAnalysisPBSV2(batch_size=5000)
    run(backfill.run())
