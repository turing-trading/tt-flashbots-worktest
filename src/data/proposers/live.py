"""Live proposer balance processor.

Consumes block headers from the live stream queue and calculates proposer
balance changes by querying the RPC endpoint.
"""

import asyncio
import os
from typing import Any

import httpx
from dotenv import load_dotenv

from src.data.proposers.db import ProposerBalancesDB
from src.data.proposers.models import ProposerBalance
from src.helpers.db import AsyncSessionLocal
from src.helpers.logging import get_logger

load_dotenv()

logger = get_logger(__name__)


class LiveProposerProcessor:
    """Processes live block headers and calculates proposer balance changes."""

    def __init__(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Initialize the live proposer processor.

        Args:
            queue: Queue to consume block headers from.
        """
        self.queue = queue
        self.rpc_url = os.getenv("ETH_RPC_URL")
        if not self.rpc_url:
            raise ValueError("ETH_RPC_URL environment variable is not set")

        self.balances_processed = 0
        self.client = httpx.AsyncClient(timeout=30.0)

    async def get_balance(self, address: str, block_number: int) -> int | None:
        """Get balance of an address at a specific block.

        Args:
            address: Ethereum address.
            block_number: Block number.

        Returns:
            Balance in Wei or None if failed.
        """
        try:
            assert self.rpc_url is not None  # Checked in __init__
            response = await self.client.post(
                self.rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [address, hex(block_number)],
                    "id": 1,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "result" in data:
                return int(data["result"], 16)

            logger.warning(f"No result for balance query: {address} @ {block_number}")
            return None

        except Exception as e:
            logger.error(f"Failed to get balance for {address} @ {block_number}: {e}")
            return None

    async def calculate_balance_change(
        self, miner: str, block_number: int
    ) -> ProposerBalance | None:
        """Calculate proposer balance change for a block.

        Args:
            miner: Miner/proposer address.
            block_number: Block number.

        Returns:
            ProposerBalance model or None if calculation failed.
        """
        try:
            # Get balance before (at block N-1) and after (at block N)
            balance_before_task = self.get_balance(miner, block_number - 1)
            balance_after_task = self.get_balance(miner, block_number)

            balance_before, balance_after = await asyncio.gather(
                balance_before_task, balance_after_task
            )

            if balance_before is None or balance_after is None:
                return None

            balance_increase = balance_after - balance_before

            return ProposerBalance(
                block_number=block_number,
                miner=miner,
                balance_before=balance_before,
                balance_after=balance_after,
                balance_increase=balance_increase,
            )

        except Exception as e:
            logger.error(
                f"Failed to calculate balance change for block {block_number}: {e}"
            )
            return None

    async def store_balance(self, balance: ProposerBalance) -> None:
        """Store proposer balance in database using upsert.

        Args:
            balance: ProposerBalance model to store.
        """
        try:
            async with AsyncSessionLocal() as session:
                # Check if balance record already exists
                existing = await session.get(ProposerBalancesDB, balance.block_number)

                if existing:
                    # Update existing record
                    existing.miner = balance.miner  # type: ignore
                    existing.balance_before = balance.balance_before  # type: ignore
                    existing.balance_after = balance.balance_after  # type: ignore
                    existing.balance_increase = balance.balance_increase  # type: ignore
                else:
                    # Insert new record
                    session.add(
                        ProposerBalancesDB(
                            block_number=balance.block_number,
                            miner=balance.miner,
                            balance_before=balance.balance_before,
                            balance_after=balance.balance_after,
                            balance_increase=balance.balance_increase,
                        )
                    )

                await session.commit()
                self.balances_processed += 1
                logger.info(
                    f"Stored proposer balance for block #{balance.block_number}: "
                    f"{balance.balance_increase / 1e18:.6f} ETH"
                )

        except Exception as e:
            logger.error(
                f"Failed to store proposer balance for block {balance.block_number}: {e}"
            )

    async def process_queue(self) -> None:
        """Process block headers from the queue."""
        logger.info("Live proposer processor started")

        while True:
            try:
                # Get block header from queue
                header = await self.queue.get()

                # Extract block number and miner
                block_number = int(header.get("number", "0x0"), 16)
                miner = header.get("miner")

                if not miner:
                    logger.warning(f"No miner in block header for block {block_number}")
                    self.queue.task_done()
                    continue

                # Calculate balance change
                balance = await self.calculate_balance_change(miner, block_number)
                if not balance:
                    self.queue.task_done()
                    continue

                # Store in database
                await self.store_balance(balance)

                # Mark task as done
                self.queue.task_done()

            except asyncio.CancelledError:
                logger.info("Live proposer processor cancelled")
                break
            except Exception as e:
                logger.error(f"Error processing proposer balance: {e}")

    async def run(self) -> None:
        """Run the live proposer processor."""
        try:
            await self.process_queue()
        finally:
            await self.client.aclose()


async def main(queue: asyncio.Queue[dict[str, Any]]) -> None:
    """Main entry point for live proposer processor.

    Args:
        queue: Queue to consume block headers from.
    """
    processor = LiveProposerProcessor(queue)
    await processor.run()


if __name__ == "__main__":
    # For testing purposes
    test_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    asyncio.run(main(test_queue))
