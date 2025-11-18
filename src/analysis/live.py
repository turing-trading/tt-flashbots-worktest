"""Live PBS analysis processor.

Consumes block headers from the live stream queue and aggregates data from
all other tables to create PBS analysis records.
"""

import asyncio
from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from src.analysis.constants import clean_builder_name
from src.analysis.db import AnalysisPBSDB
from src.analysis.models import AnalysisPBS
from src.data.blocks.db import BlockDB
from src.data.builders.db import BuilderIdentifiersDB
from src.data.proposers.db import ProposerBalancesDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.db import AsyncSessionLocal
from src.helpers.logging import get_logger

logger = get_logger(__name__)


class LiveAnalysisProcessor:
    """Processes live block headers and creates PBS analysis records."""

    def __init__(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Initialize the live analysis processor.

        Args:
            queue: Queue to consume block headers from.
        """
        self.queue = queue
        self.analyses_processed = 0

    async def aggregate_block_data(self, block_number: int) -> AnalysisPBS | None:
        """Aggregate data from all tables for PBS analysis.

        Args:
            block_number: Block number to aggregate.

        Returns:
            AnalysisPBS model or None if aggregation failed.
        """
        try:
            async with AsyncSessionLocal() as session:
                # Query all related data
                stmt = (
                    select(
                        BlockDB.number.label("block_number"),
                        BlockDB.timestamp.label("block_timestamp"),
                        ProposerBalancesDB.balance_increase.label(
                            "builder_balance_increase"
                        ),
                        func.array_agg(RelaysPayloadsDB.relay).label("relays"),
                        func.max(RelaysPayloadsDB.value).label("proposer_subsidy"),
                        func.max(BuilderIdentifiersDB.builder_name).label(
                            "builder_name"
                        ),
                    )
                    .select_from(BlockDB)
                    .outerjoin(
                        ProposerBalancesDB,
                        BlockDB.number == ProposerBalancesDB.block_number,
                    )
                    .outerjoin(
                        RelaysPayloadsDB,
                        BlockDB.number == RelaysPayloadsDB.block_number,
                    )
                    .outerjoin(
                        BuilderIdentifiersDB,
                        RelaysPayloadsDB.builder_pubkey
                        == BuilderIdentifiersDB.builder_pubkey,
                    )
                    .where(BlockDB.number == block_number)
                    .group_by(
                        BlockDB.number,
                        BlockDB.timestamp,
                        ProposerBalancesDB.balance_increase,
                    )
                )

                result = await session.execute(stmt)
                row = result.one_or_none()

                if not row:
                    logger.warning(f"No data found for block {block_number}")
                    return None

                # Convert to AnalysisPBS model
                return AnalysisPBS(
                    block_number=row.block_number,
                    block_timestamp=row.block_timestamp,
                    builder_balance_increase=(
                        float(row.builder_balance_increase) / 1e18
                        if row.builder_balance_increase is not None
                        else None
                    ),
                    relays=(
                        [r for r in row.relays if r is not None]
                        if row.relays is not None
                        else None
                    ),
                    proposer_subsidy=(
                        float(row.proposer_subsidy) / 1e18
                        if row.proposer_subsidy is not None
                        else None
                    ),
                    builder_name=clean_builder_name(row.builder_name),
                )

        except Exception as e:
            logger.error(f"Failed to aggregate data for block {block_number}: {e}")
            return None

    async def store_analysis(self, analysis: AnalysisPBS) -> None:
        """Store PBS analysis in database using upsert.

        Args:
            analysis: AnalysisPBS model to store.
        """
        try:
            async with AsyncSessionLocal() as session:
                # Check if analysis already exists
                existing = await session.get(AnalysisPBSDB, analysis.block_number)

                if existing:
                    # Update existing analysis
                    existing.block_timestamp = analysis.block_timestamp  # type: ignore
                    existing.builder_balance_increase = (  # type: ignore
                        analysis.builder_balance_increase
                    )
                    existing.relays = analysis.relays  # type: ignore
                    existing.proposer_subsidy = analysis.proposer_subsidy  # type: ignore
                    existing.builder_name = analysis.builder_name  # type: ignore
                else:
                    # Insert new analysis
                    session.add(
                        AnalysisPBSDB(
                            block_number=analysis.block_number,
                            block_timestamp=analysis.block_timestamp,
                            builder_balance_increase=analysis.builder_balance_increase,
                            relays=analysis.relays,
                            proposer_subsidy=analysis.proposer_subsidy,
                            builder_name=analysis.builder_name,
                        )
                    )

                await session.commit()
                self.analyses_processed += 1
                logger.info(f"Stored PBS analysis for block #{analysis.block_number}")

        except Exception as e:
            logger.error(
                f"Failed to store PBS analysis for block {analysis.block_number}: {e}"
            )

    async def process_queue(self) -> None:
        """Process block headers from the queue."""
        logger.info("Live analysis processor started")

        while True:
            try:
                # Get block header from queue
                header = await self.queue.get()

                # Extract block number
                block_number = int(header.get("number", "0x0"), 16)
                timestamp = header.get("timestamp")

                if not timestamp:
                    error_msg = "No timestamp in block header"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                # Wait 15 minutes for blocks to be processed
                time_to_wait = (
                    15 * 60
                    - (
                        datetime.now() - datetime.fromtimestamp(int(timestamp, 16))
                    ).total_seconds()
                )
                if time_to_wait > 0:
                    await asyncio.sleep(time_to_wait)

                # Aggregate data
                analysis = await self.aggregate_block_data(block_number)
                if not analysis:
                    self.queue.task_done()
                    continue

                # Store in database
                await self.store_analysis(analysis)

                # Mark task as done
                self.queue.task_done()

            except asyncio.CancelledError:
                logger.info("Live analysis processor cancelled")
                break
            except Exception as e:
                logger.error(f"Error processing PBS analysis: {e}")

    async def run(self) -> None:
        """Run the live analysis processor."""
        await self.process_queue()


async def main(queue: asyncio.Queue[dict[str, Any]]) -> None:
    """Main entry point for live analysis processor.

    Args:
        queue: Queue to consume block headers from.
    """
    processor = LiveAnalysisProcessor(queue)
    await processor.run()


if __name__ == "__main__":
    # For testing purposes
    test_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    asyncio.run(main(test_queue))
