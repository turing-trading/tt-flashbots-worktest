"""Live PBS analysis processor.

Consumes block headers from the live stream queue and aggregates data from
all other tables to create PBS analysis records.
"""

import asyncio
from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from src.analysis.constants import clean_builder_name
from src.analysis.db import AnalysisPBSV2DB
from src.analysis.models import AnalysisPBSV2
from src.data.blocks.db import BlockDB
from src.data.builders.db import BuilderIdentifiersDB
from src.data.proposers.db import ProposerBalancesDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.db import AsyncSessionLocal, upsert_model
from src.helpers.logging import get_logger
from src.helpers.parsers import parse_hex_block_number, parse_hex_timestamp, wei_to_eth

logger = get_logger(__name__)


class LiveAnalysisProcessorV2:
    """Processes live block headers and creates PBS analysis records (V2 with computed fields)."""

    def __init__(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Initialize the live analysis processor.

        Args:
            queue: Queue to consume block headers from.
        """
        self.queue = queue
        self.analyses_processed = 0

    async def aggregate_block_data(self, block_number: int) -> AnalysisPBSV2 | None:
        """Aggregate data from all tables for PBS analysis.

        Args:
            block_number: Block number to aggregate.

        Returns:
            AnalysisPBSV2 model or None if aggregation failed.
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

                # Process relay data
                relays_list = (
                    [r for r in row.relays if r is not None]
                    if row.relays is not None
                    else None
                )
                n_relays = len(relays_list) if relays_list else 0
                is_block_vanilla = n_relays == 0

                # Convert values with defaults
                builder_balance_increase = wei_to_eth(row.builder_balance_increase) or 0.0
                proposer_subsidy = wei_to_eth(row.proposer_subsidy) or 0.0
                total_value = builder_balance_increase + proposer_subsidy
                builder_name = clean_builder_name(row.builder_name) or "unknown"

                # Convert to AnalysisPBSV2 model
                return AnalysisPBSV2(
                    block_number=row.block_number,
                    block_timestamp=row.block_timestamp,
                    builder_balance_increase=builder_balance_increase,
                    proposer_subsidy=proposer_subsidy,
                    total_value=total_value,
                    is_block_vanilla=is_block_vanilla,
                    n_relays=n_relays,
                    relays=relays_list,
                    builder_name=builder_name,
                )

        except Exception as e:
            logger.error(f"Failed to aggregate data for block {block_number}: {e}")
            return None

    async def store_analysis(self, analysis: AnalysisPBSV2) -> None:
        """Store PBS analysis in database using upsert.

        Args:
            analysis: AnalysisPBSV2 model to store.
        """
        try:
            await upsert_model(
                db_model_class=AnalysisPBSV2DB,
                pydantic_model=analysis,
                primary_key_value=analysis.block_number,
            )
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
                block_number = parse_hex_block_number(header)
                timestamp = header.get("timestamp")

                if not timestamp:
                    error_msg = "No timestamp in block header"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                # Wait 15 minutes for blocks to be processed
                time_to_wait = (
                    15 * 60
                    - (datetime.now() - parse_hex_timestamp(timestamp)).total_seconds()
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
    processor = LiveAnalysisProcessorV2(queue)
    await processor.run()


if __name__ == "__main__":
    # For testing purposes
    test_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    asyncio.run(main(test_queue))
