"""Backfill PBS analysis data with proposer_name and precomputed columns."""

from datetime import datetime

from typing import TYPE_CHECKING

from asyncio import run

from sqlalchemy import case, func, select

from src.analysis.builder_name import parse_builder_name_from_extra_data
from src.analysis.db import AnalysisPBSDB
from src.analysis.models import AnalysisPBS
from src.data.adjustments.db import UltrasoundAdjustmentDB
from src.data.blocks.db import BlockDB
from src.data.builders.db import BuilderBalancesDB, ExtraBuilderBalanceDB
from src.data.proposers.db import ProposerMappingDB
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
# START_DATE: datetime = datetime.now(tz=UTC) - timedelta(days=1)
# 2024-01-01
# START_DATE = datetime(2023, 4, 14, tzinfo=UTC)
# END_DATE = datetime(2024, 4, 8, tzinfo=UTC)
START_DATE = datetime.fromisoformat("2022-10-07T00:51:47.941Z")
END_DATE = datetime.fromisoformat("2023-01-30T12:43:09.729Z")

# START_DATE = datetime.fromisoformat("2023-02-26T16:57:41.340Z".replace("Z", "+00:00"))
# END_DATE = datetime.fromisoformat("2023-05-01T09:28:46.668Z".replace("Z", "+00:00"))
# END_DATE: datetime | None = datetime.now(tz=UTC) - timedelta(minutes=10)


class BackfillAnalysisPBS(BackfillBase):
    """Backfill PBS analysis data with proposer_name and precomputed columns."""

    def __init__(self, batch_size: int = LARGE_BATCH_SIZE) -> None:
        """Initialize backfill.

        Args:
            batch_size: Number of blocks to process per batch
        """
        super().__init__(batch_size)

    async def _get_missing_blocks(self, session: AsyncSession) -> list[tuple[int, ...]]:
        """Get block numbers that exist in blocks table but not in analysis_pbs.

        Returns:
            List of block numbers that need to be processed
        """
        subquery = select(AnalysisPBSDB.block_number).where(
            AnalysisPBSDB.block_number == BlockDB.number
        )

        stmt = select(BlockDB.number).where(~subquery.exists())

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

        if END_DATE is not None:
            stmt = stmt.where(BlockDB.timestamp <= END_DATE)

        result = await session.execute(stmt)
        return result.scalar_one()

    async def _aggregate_block_data(
        self, session: AsyncSession, block_numbers: list[int]
    ) -> list[AggregatedBlockData]:
        """Aggregate from blocks, builder_balance, relays_payloads, proposer_mapping.

        Also includes adjustments and extra_builder_balance.

        Args:
            session: Database session
            block_numbers: List of block numbers to aggregate

        Returns:
            List of AggregatedBlockData models with aggregated data
        """
        # Create subquery for extra builder transfers
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
                # NEW: Get proposer_name from proposer_mapping
                func.max(ProposerMappingDB.label).label("proposer_name"),
            )
            .select_from(BlockDB)
            .outerjoin(
                BuilderBalancesDB,
                BlockDB.number == BuilderBalancesDB.block_number,
            )
            .outerjoin(
                RelaysPayloadsDB, BlockDB.number == RelaysPayloadsDB.block_number
            )
            .outerjoin(
                extra_transfers_subq,
                BlockDB.number == extra_transfers_subq.c.block_number,
            )
            .outerjoin(
                UltrasoundAdjustmentDB,
                RelaysPayloadsDB.slot == UltrasoundAdjustmentDB.slot,
            )
            # NEW: Join with proposer_mapping to get proposer_name
            .outerjoin(
                ProposerMappingDB,
                RelaysPayloadsDB.proposer_fee_recipient
                == ProposerMappingDB.proposer_fee_recipient,
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

        aggregated_data: list[AggregatedBlockData] = []
        for row in rows:
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
                proposer_name,
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

            # Specific fields
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

            # NEW: Compute builder_profit and percentage columns
            builder_profit: float = total_value - proposer_subsidy - (relay_fee or 0.0)

            # Only compute percentages when total_value > 0 to avoid division errors
            pct_proposer_share: float | None = None
            pct_builder_share: float | None = None
            pct_relay_fee: float | None = None

            if total_value > 0:
                pct_proposer_share = (proposer_subsidy / total_value) * 100
                pct_builder_share = (builder_profit / total_value) * 100
                pct_relay_fee = ((relay_fee or 0.0) / total_value) * 100

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
                    # NEW fields
                    proposer_name=proposer_name,
                    builder_profit=builder_profit,
                    pct_proposer_share=pct_proposer_share,
                    pct_builder_share=pct_builder_share,
                    pct_relay_fee=pct_relay_fee,
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

        # Convert AggregatedBlockData to AnalysisPBS models
        models = [AnalysisPBS(**item.model_dump()) for item in data]

        # Upsert data into analysis_pbs table
        await upsert_models(
            db_model_class=AnalysisPBSDB,
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
        aggregated_data = await self._aggregate_block_data(session, block_numbers)
        await self._store_analysis_data(aggregated_data)
        progress.update(task_id, advance=len(block_numbers))
        return len(block_numbers)

    async def run(self) -> None:
        """Run the backfill process."""
        await self.create_tables()

        overwrite_mode = END_DATE is not None

        async with AsyncSessionLocal() as session:
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
                        "[yellow]No missing blocks to process - analysis_pbs is "
                        "up to date[/yellow]"
                    )
                return

            total_to_process = len(blocks_to_process)

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

            progress = create_standard_progress(console=self.console)
            total_processed = 0

            with progress:
                task_id = progress.add_task(mode_description, total=total_to_process)

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
    # Batch size limited to 3500 to avoid PostgreSQL's 65535 parameter limit
    # analysis_pbs has 17 fields per row (3500 * 17 = 59500 parameters)
    backfill = BackfillAnalysisPBS(batch_size=3500)
    run(backfill.run())
