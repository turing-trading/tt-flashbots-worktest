"""Live block processor.

Consumes block headers from the live stream queue and stores them in the database.
Fetches full block data from ETH_RPC_URL to get additional fields like extra_data.
"""

import asyncio
import os
from typing import Any

import httpx
from dotenv import load_dotenv

from src.data.blocks.db import BlockDB
from src.data.blocks.models import Block
from src.helpers.db import upsert_model
from src.helpers.logging import get_logger
from src.helpers.parsers import (
    parse_hex_block_number,
    parse_hex_int,
    parse_hex_timestamp,
    wei_to_eth,
)

load_dotenv()

logger = get_logger(__name__)


class LiveBlockProcessor:
    """Processes live block headers and stores them in the database."""

    def __init__(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Initialize the live block processor.

        Args:
            queue: Queue to consume block headers from.
        """
        self.queue = queue
        self.rpc_url = os.getenv("ETH_RPC_URL")
        if not self.rpc_url:
            raise ValueError("ETH_RPC_URL environment variable is not set")

        self.blocks_processed = 0
        self.client = httpx.AsyncClient(timeout=30.0)

    async def fetch_full_block(self, block_number: int) -> dict[str, Any] | None:
        """Fetch full block data from RPC endpoint.

        Args:
            block_number: Block number to fetch.

        Returns:
            Full block data dictionary or None if failed.
        """
        try:
            assert self.rpc_url is not None  # Checked in __init__
            response = await self.client.post(
                self.rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [
                        hex(block_number),
                        False,
                    ],  # False = don't include full txs
                    "id": 1,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "result" in data and data["result"]:
                return data["result"]

            logger.warning(f"No result for block {block_number}")
            return None

        except Exception as e:
            logger.error(f"Failed to fetch block {block_number}: {e}")
            return None

    def parse_block(self, block_data: dict[str, Any]) -> Block | None:
        """Parse block data into Block model.

        Args:
            block_data: Raw block data from RPC.

        Returns:
            Block model or None if parsing failed.
        """
        try:
            return Block(
                number=parse_hex_int(block_data["number"]),
                hash=block_data["hash"],
                parent_hash=block_data["parentHash"],
                nonce=block_data["nonce"],
                sha3_uncles=block_data["sha3Uncles"],
                transactions_root=block_data["transactionsRoot"],
                state_root=block_data["stateRoot"],
                receipts_root=block_data["receiptsRoot"],
                miner=block_data["miner"],
                size=parse_hex_int(block_data["size"]),
                extra_data=block_data["extraData"],
                gas_limit=parse_hex_int(block_data["gasLimit"]),
                gas_used=parse_hex_int(block_data["gasUsed"]),
                timestamp=parse_hex_timestamp(block_data["timestamp"]),
                transaction_count=len(block_data.get("transactions", [])),
                base_fee_per_gas=wei_to_eth(
                    parse_hex_int(block_data["baseFeePerGas"])
                    if "baseFeePerGas" in block_data
                    else None
                ),
            )
        except Exception as e:
            logger.error(f"Failed to parse block: {e}")
            return None

    async def store_block(self, block: Block) -> None:
        """Store block in database using upsert.

        Args:
            block: Block model to store.
        """
        try:
            await upsert_model(
                db_model_class=BlockDB,
                pydantic_model=block,
                primary_key_value=block.number,
            )
            self.blocks_processed += 1
            logger.info(f"Stored block #{block.number}")

        except Exception as e:
            logger.error(f"Failed to store block {block.number}: {e}")

    async def process_queue(self) -> None:
        """Process block headers from the queue."""
        logger.info("Live block processor started")

        while True:
            try:
                # Get block header from queue
                header = await self.queue.get()

                # Extract block number
                block_number = parse_hex_block_number(header)

                # Fetch full block data
                full_block = await self.fetch_full_block(block_number)
                if not full_block:
                    continue

                # Parse block
                block = self.parse_block(full_block)
                if not block:
                    continue

                # Store in database
                await self.store_block(block)

                # Mark task as done
                self.queue.task_done()

            except asyncio.CancelledError:
                logger.info("Live block processor cancelled")
                break
            except Exception as e:
                logger.error(f"Error processing block: {e}")

    async def run(self) -> None:
        """Run the live block processor."""
        try:
            await self.process_queue()
        finally:
            await self.client.aclose()


async def main(queue: asyncio.Queue[dict[str, Any]]) -> None:
    """Main entry point for live block processor.

    Args:
        queue: Queue to consume block headers from.
    """
    processor = LiveBlockProcessor(queue)
    await processor.run()


if __name__ == "__main__":
    # For testing purposes
    test_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    asyncio.run(main(test_queue))
