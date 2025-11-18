"""Live builder identifier processor.

Consumes block headers from the live stream queue and extracts builder
identifiers from block extra_data and relay payloads.
"""

import asyncio
from datetime import datetime
from typing import Any

from sqlalchemy import select

from src.data.blocks.db import BlockDB
from src.data.builders.db import BuilderIdentifiersDB
from src.data.relays.db import RelaysPayloadsDB
from src.helpers.db import AsyncSessionLocal
from src.helpers.logging import get_logger


class LiveBuilderProcessor:
    """Processes live block headers and extracts builder identifiers."""

    def __init__(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Initialize the live builder processor.

        Args:
            queue: Queue to consume block headers from.
        """
        self.queue = queue
        self.identifiers_processed = 0
        self.logger = get_logger(__name__)

    def parse_builder_name(self, extra_data: str) -> str:
        """Parse builder name from extra_data.

        Args:
            extra_data: Block extra_data field (hex string).

        Returns:
            Parsed builder name.
        """
        try:
            # Remove 0x prefix
            if extra_data.startswith("0x"):
                extra_data = extra_data[2:]

            # Decode from hex
            decoded = bytes.fromhex(extra_data).decode("utf-8", errors="ignore")

            # Clean up
            cleaned = decoded.strip().replace("\x00", "")

            return cleaned if cleaned else "unknown"

        except Exception as e:
            self.logger.error(f"Failed to parse builder name from extra_data: {e}")
            return "unknown"

    async def get_builder_identifiers(self, block_number: int) -> list[tuple[str, str]]:
        """Get builder identifiers for a block.

        Queries the relays_payloads table for builder_pubkey and the blocks
        table for extra_data.

        Args:
            block_number: Block number.

        Returns:
            List of (builder_pubkey, builder_name) tuples.
        """
        identifiers = []

        try:
            async with AsyncSessionLocal() as session:
                # Get relay payloads for this block
                stmt = select(RelaysPayloadsDB).where(
                    RelaysPayloadsDB.block_number == block_number
                )
                result = await session.execute(stmt)
                relay_payloads = result.scalars().all()

                # Get block extra_data
                block = await session.get(BlockDB, block_number)

                if relay_payloads and block and block.extra_data is not None:
                    # Extract builder_pubkeys from relay payloads
                    builder_pubkeys = {
                        payload.builder_pubkey for payload in relay_payloads
                    }

                    # Parse builder name from extra_data
                    builder_name = self.parse_builder_name(block.extra_data.strip())

                    # Create identifiers for each unique builder_pubkey
                    for builder_pubkey in builder_pubkeys:
                        identifiers.append((builder_pubkey, builder_name))

        except Exception as e:
            self.logger.error(
                f"Failed to get builder identifiers for block {block_number}: {e}"
            )

        return identifiers

    async def store_identifiers(self, builder_pubkey: str, builder_name: str) -> None:
        """Store builder identifier in database using upsert.

        Args:
            builder_pubkey: Builder public key.
            builder_name: Builder name.
        """
        try:
            async with AsyncSessionLocal() as session:
                # Check if identifier already exists
                existing = await session.get(BuilderIdentifiersDB, builder_pubkey)

                if existing:
                    # Update existing identifier
                    existing.builder_name = builder_name  # type: ignore
                else:
                    # Insert new identifier
                    session.add(
                        BuilderIdentifiersDB(
                            builder_pubkey=builder_pubkey,
                            builder_name=builder_name,
                        )
                    )

                await session.commit()
                self.identifiers_processed += 1
                self.logger.info(
                    f"Stored builder identifier: {builder_name} ({builder_pubkey[:10]}...)"
                )

        except Exception as e:
            self.logger.error(f"Failed to store builder identifier: {e}")

    async def process_queue(self) -> None:
        """Process block headers from the queue."""
        self.logger.info("Live builder processor started")

        while True:
            try:
                # Get block header from queue
                header = await self.queue.get()

                # Extract block number
                block_number = int(header.get("number", "0x0"), 16)
                timestamp = header.get("timestamp")
                if not timestamp:
                    error_msg = "No timestamp in block header"
                    self.logger.error(error_msg)
                    raise Exception(error_msg)

                # Wait 1 minute for blocks to be processed
                time_to_wait = (
                    60
                    - (
                        datetime.now() - datetime.fromtimestamp(int(timestamp, 16))
                    ).total_seconds()
                )
                if time_to_wait > 0:
                    await asyncio.sleep(time_to_wait)

                identifiers = await self.get_builder_identifiers(block_number)

                if not identifiers:
                    self.logger.debug(
                        f"No builder identifiers found for block {block_number}"
                    )
                else:
                    # Store each identifier
                    for builder_pubkey, builder_name in identifiers:
                        await self.store_identifiers(builder_pubkey, builder_name)

                # Mark task as done
                self.queue.task_done()

            except asyncio.CancelledError:
                self.logger.info("Live builder processor cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error processing builder identifiers: {e}")

    async def run(self) -> None:
        """Run the live builder processor."""
        await self.process_queue()


async def main(queue: asyncio.Queue[dict[str, Any]]) -> None:
    """Main entry point for live builder processor.

    Args:
        queue: Queue to consume block headers from.
    """
    processor = LiveBuilderProcessor(queue)
    await processor.run()


if __name__ == "__main__":
    # For testing purposes
    test_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    asyncio.run(main(test_queue))
