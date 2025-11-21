"""Backfill PBS analysis data V3 with slot, extra transfers, and relay fees."""

from datetime import UTC, datetime, timedelta

from typing import TYPE_CHECKING

from asyncio import run

from sqlalchemy import case, func, select

from src.analysis.builder_name import parse_builder_name_from_extra_data
from src.analysis.db import AnalysisPBSV3DB
from src.analysis.models import AnalysisPBSV3
from src.data.adjustments.db import UltrasoundAdjustmentDB
from src.data.blocks.db import BlockDB
from src.data.builders.db import BuilderBalancesDB, ExtraBuilderBalanceDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.backfill import BackfillBase
from src.helpers.constants import LARGE_BATCH_SIZE
from src.helpers.db import AsyncSessionLocal, upsert_models
from src.helpers.models import AggregatedBlockData
from src.helpers.parsers import wei_to_eth
from src.helpers.progress import create_standard_progress


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from rich.progress import Progress, TaskID


# Default to process last year of data
START_DATE: datetime = datetime.now(tz=UTC) - timedelta(days=3)
# 2024-01-01
# START_DATE = datetime(2024, 2, 1)
END_DATE: datetime | None = datetime.now(tz=UTC) - timedelta(minutes=10)


class BackfillAnalysisPBSV3(BackfillBase):
    """Backfill PBS analysis data V3 with slot, extra transfers, and relay fees."""

    def __init__(self, batch_size: int = LARGE_BATCH_SIZE) -> None:
        """Initialize backfill.

        Args:
            batch_size: Number of blocks to process per batch
        """
        super().__init__(batch_size)

    async def _get_missing_blocks(self, session: AsyncSession) -> list[tuple[int, ...]]:
        """Get block numbers that exist in blocks table but not in analysis_pbs_v3.

        Returns:
            List of block numbers that need to be processed
        """
        # Use NOT EXISTS for better performance on large tables
        subquery = select(AnalysisPBSV3DB.block_number).where(
            AnalysisPBSV3DB.block_number == BlockDB.number
        )

        stmt = select(BlockDB.number).where(~subquery.exists())

        # Add END_DATE filter if specified
        if END_DATE is not None:
            stmt = stmt.where(BlockDB.timestamp <= END_DATE)

        stmt = stmt.order_by(BlockDB.number.desc())

        result = await session.execute(stmt)
        return [tuple(row) for row in result.fetchall()]

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
        return [tuple(row) for row in result.fetchall()]

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
    ) -> list[AggregatedBlockData]:
        """Aggregate data from blocks, builder_balance, relays_payloads.

        Also includes adjustments and extra_builder_balance.

        Args:
            session: Database session
            block_numbers: List of block numbers to aggregate

        Returns:
            List of AggregatedBlockData models with aggregated data
        """
        # Build the aggregation query with all V3 fields
        # Note: We use CASE WHEN to filter only positive balance
        # increases for builder_extra_transfers

        # Create subquery for extra builder transfers to avoid
        # Cartesian product with relays (each relay entry would
        # multiply the extra transfers count)
        extra_transfers_subq = (
            select(
                ExtraBuilderBalanceDB.block_number,
                func.sum(
                    case(
                        (
                            ExtraBuilderBalanceDB.balance_increase > 0,
                            ExtraBuilderBalanceDB.balance_increase,
                        ),
                        else_=0,
                    )
                ).label("builder_extra_transfers_wei"),
            )
            .where(ExtraBuilderBalanceDB.block_number.in_(block_numbers))
            .group_by(ExtraBuilderBalanceDB.block_number)
            .subquery()
        )

        stmt = (
            select(
                BlockDB.number.label("block_number"),
                BlockDB.timestamp.label("block_timestamp"),
                BlockDB.extra_data.label("extra_data"),
                BuilderBalancesDB.balance_increase.label("builder_balance_increase"),
                func.array_agg(RelaysPayloadsDB.relay).label("relays"),
                func.max(RelaysPayloadsDB.value).label("proposer_subsidy"),
                func.min(RelaysPayloadsDB.slot).label("slot"),
                extra_transfers_subq.c.builder_extra_transfers_wei.label(
                    "builder_extra_transfers_wei"
                ),
                func.max(UltrasoundAdjustmentDB.delta).label("relay_fee_wei"),
            )
            .select_from(BlockDB)
            .outerjoin(
                BuilderBalancesDB,
                BlockDB.number == BuilderBalancesDB.block_number,
            )
            .outerjoin(
                RelaysPayloadsDB, BlockDB.hash == RelaysPayloadsDB.block_hash
            )
            .outerjoin(
                extra_transfers_subq,
                BlockDB.number == extra_transfers_subq.c.block_number,
            )
            .outerjoin(
                UltrasoundAdjustmentDB,
                RelaysPayloadsDB.slot == UltrasoundAdjustmentDB.slot,
            )
            .where(BlockDB.number.in_(block_numbers))
            .group_by(
                BlockDB.number,
                BlockDB.timestamp,
                BlockDB.extra_data,
                BuilderBalancesDB.balance_increase,
                extra_transfers_subq.c.builder_extra_transfers_wei,
            )
        )

        result = await session.execute(stmt)
        rows = result.fetchall()

        # Convert to AggregatedBlockData models
        aggregated_data: list[AggregatedBlockData] = []
        for row in rows:
            # Unpack tuple from SQL result
            (
                block_number,
                block_timestamp,
                extra_data,
                builder_balance_increase_wei,
                raw_relays,
                proposer_subsidy_wei,
                slot,
                builder_extra_transfers_wei,
                relay_fee_wei,
            ) = row

            # Get relays array (None-safe, filter out None values from array_agg)
            relays: list[str] = [r for r in list(raw_relays or []) if r is not None]  # type: ignore[misc]
            relays_list: list[str] | None = relays or None

            # Calculate computed fields
            n_relays: int = len(relays)
            is_block_vanilla: bool = n_relays == 0

            # Convert Wei to ETH using helper with defaults
            builder_balance_increase: float = (
                wei_to_eth(builder_balance_increase_wei) or 0.0
            )
            proposer_subsidy: float = wei_to_eth(proposer_subsidy_wei) or 0.0

            # Parse builder name directly from extra_data
            builder_name: str = parse_builder_name_from_extra_data(extra_data)

            # V3 specific fields
            builder_extra_transfers: float = (
                wei_to_eth(builder_extra_transfers_wei) or 0.0
            )
            relay_fee: float | None = wei_to_eth(relay_fee_wei)

            # Calculate total value including all components (treat None relay_fee as 0)
            total_value: float = (
                builder_balance_increase + proposer_subsidy + (relay_fee or 0.0)
            )

            if total_value < 0 and builder_extra_transfers > 0:
                total_value += builder_extra_transfers

            aggregated_data.append(
                AggregatedBlockData(
                    block_number=block_number,
                    block_timestamp=block_timestamp,
                    builder_balance_increase=builder_balance_increase,
                    proposer_subsidy=proposer_subsidy,
                    total_value=total_value,
                    is_block_vanilla=is_block_vanilla,
                    n_relays=n_relays,
                    relays=relays_list,
                    builder_name=builder_name,
                    slot=slot,
                    builder_extra_transfers=builder_extra_transfers,
                    relay_fee=relay_fee,
                )
            )

        return aggregated_data

    async def _store_analysis_data(self, data: list[AggregatedBlockData]) -> None:
        """Store aggregated analysis data in the database.

        Args:
            data: List of aggregated data models
        """
        if not data:
            return

        # Convert AggregatedBlockData to AnalysisPBSV3 models
        models = [AnalysisPBSV3(**item.model_dump()) for item in data]

        # Upsert data into analysis_pbs_v3 table
        await upsert_models(
            db_model_class=AnalysisPBSV3DB,
            pydantic_models=models,
        )

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
        await self._store_analysis_data(aggregated_data)

        # Update progress
        progress.update(task_id, advance=len(block_numbers))

        return len(block_numbers)

    async def run(self) -> None:
        """Run the backfill process."""
        await self.create_tables()

        # Determine mode based on END_DATE
        overwrite_mode = END_DATE is not None

        async with AsyncSessionLocal() as session:
            # Get blocks to process based on mode
            if overwrite_mode:
                await self._get_block_count(session)

                blocks_result = await self._get_blocks_in_range(session)
                blocks_to_process = [row[0] for row in blocks_result]
                mode_description = "Overwriting all blocks in range"
            else:
                blocks_result = await self._get_missing_blocks(session)
                blocks_to_process = [row[0] for row in blocks_result]
                mode_description = "Backfilling missing blocks"

            if not blocks_to_process:
                if overwrite_mode:
                    self.console.print(
                        "[yellow]No blocks found in specified date range[/yellow]"
                    )
                else:
                    self.console.print(
                        "[yellow]No missing blocks to process - analysis_pbs_v3 is "
                        "up to date[/yellow]"
                    )
                return

            total_to_process = len(blocks_to_process)

            # Display mode information
            self.console.print(
                "[bold blue]Backfilling PBS Analysis Data V3[/bold blue]"
            )
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
            progress = create_standard_progress(console=self.console)

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
    # V3 has 12 fields per row (5000 * 12 = 60000 parameters)
    backfill = BackfillAnalysisPBSV3(batch_size=5000)
    run(backfill.run())
