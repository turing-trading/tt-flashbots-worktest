"""Live relay payload processor.

Consumes block headers from the live stream queue and queries relay APIs
for payload data, then stores in the database.
"""

import asyncio
from datetime import datetime
from typing import Any

import httpx

from src.data.relays.constants import RELAYS
from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.models import RelaysPayloads
from src.helpers.db import upsert_model
from src.helpers.logging import get_logger
from src.helpers.parsers import parse_hex_block_number, parse_hex_timestamp


class LiveRelayProcessor:
    """Processes live block headers and queries relay APIs for payload data."""

    def __init__(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Initialize the live relay processor.

        Args:
            queue: Queue to consume block headers from.
        """
        self.queue = queue
        self.payloads_processed = 0
        self.client = httpx.AsyncClient(timeout=30.0)

        self.logger = get_logger(__name__)

    async def fetch_relay_payload(
        self, relay: str, block_number: int
    ) -> list[dict[str, Any]]:
        """Fetch payload data from a relay for a specific block.

        Args:
            relay: Relay domain name.
            block_number: Block number to query.

        Returns:
            List of payload dictionaries.
        """
        try:
            url = f"https://{relay}/relay/v1/data/bidtraces/proposer_payload_delivered"
            params = {"block_number": block_number}

            response = await self.client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            return data if isinstance(data, list) else []

        except httpx.HTTPStatusError as e:
            if e.response.status_code != 404:
                self.logger.warning(
                    f"Relay {relay} returned {e.response.status_code} for block {block_number}"
                )
            return []
        except Exception as e:
            self.logger.error(
                f"Failed to fetch from {relay} for block {block_number}: {e}"
            )
            return []

    async def fetch_all_relays(
        self, block_number: int
    ) -> list[tuple[str, dict[str, Any]]]:
        """Fetch payload data from all relays concurrently.

        Args:
            block_number: Block number to query.

        Returns:
            List of (relay_name, payload) tuples.
        """
        tasks = [self.fetch_relay_payload(relay, block_number) for relay in RELAYS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        payloads = []
        for relay, result in zip(RELAYS, results, strict=True):
            if isinstance(result, list):
                for payload in result:
                    payloads.append((relay, payload))
            elif isinstance(result, Exception):
                self.logger.error(f"Error fetching from {relay}: {result}")

        return payloads

    def parse_relay_payload(
        self, relay: str, payload_data: dict[str, Any]
    ) -> RelaysPayloads | None:
        """Parse relay payload data into RelaysPayloads model.

        Args:
            relay: Relay name.
            payload_data: Raw payload data from relay API.

        Returns:
            RelaysPayloads model or None if parsing failed.
        """
        try:
            return RelaysPayloads(
                slot=payload_data["slot"],
                parent_hash=payload_data["parent_hash"],
                block_hash=payload_data["block_hash"],
                builder_pubkey=payload_data["builder_pubkey"],
                proposer_pubkey=payload_data["proposer_pubkey"],
                proposer_fee_recipient=payload_data["proposer_fee_recipient"],
                gas_limit=payload_data["gas_limit"],
                gas_used=payload_data["gas_used"],
                value=payload_data["value"],
                block_number=payload_data["block_number"],
                num_tx=payload_data["num_tx"],
            )
        except Exception as e:
            self.logger.error(f"Failed to parse relay payload from {relay}: {e}")
            return None

    async def store_payloads(self, relay: str, payload: RelaysPayloads) -> None:
        """Store relay payload in database using upsert.

        Args:
            relay: Relay name.
            payload: RelaysPayloads model to store.
        """
        try:
            await upsert_model(
                db_model_class=RelaysPayloadsDB,
                pydantic_model=payload,
                primary_key_value=(payload.slot, relay),
                extra_fields={"relay": relay},
            )
            self.payloads_processed += 1
            self.logger.info(
                f"Stored relay payload from {relay} for block #{payload.block_number}"
            )

        except Exception as e:
            self.logger.error(f"Failed to store relay payload from {relay}: {e}")

    async def process_queue(self) -> None:
        """Process block headers from the queue."""
        self.logger.info("Live relay processor started")

        while True:
            try:
                # Get block header from queue
                header = await self.queue.get()
                timestamp = header.get("timestamp")
                if not timestamp:
                    error_msg = "No timestamp in block header"
                    self.logger.error(error_msg)
                    raise Exception(error_msg)

                # Wait 13 minutes (2 epochs) for relays to serve the block
                # This ensures relays have processed and made the data available
                time_to_wait = (
                    13 * 60
                    - (datetime.now() - parse_hex_timestamp(timestamp)).total_seconds()
                )
                if time_to_wait > 0:
                    await asyncio.sleep(time_to_wait)

                # Extract block number
                block_number = parse_hex_block_number(header)

                # Fetch payloads from all relays
                relay_payloads = await self.fetch_all_relays(block_number)

                if not relay_payloads:
                    self.logger.info(
                        f"No relay payloads found for block {block_number}"
                    )
                else:
                    self.logger.info(
                        f"Found {len(relay_payloads)} relay payloads for block {block_number}"
                    )

                # Process each payload
                for relay, payload_data in relay_payloads:
                    payload = self.parse_relay_payload(relay, payload_data)
                    if payload:
                        await self.store_payloads(relay, payload)

                # Mark task as done
                self.queue.task_done()

            except asyncio.CancelledError:
                self.logger.info("Live relay processor cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error processing relay payloads: {e}")

    async def run(self) -> None:
        """Run the live relay processor."""
        try:
            await self.process_queue()
        finally:
            await self.client.aclose()


async def main(queue: asyncio.Queue[dict[str, Any]]) -> None:
    """Main entry point for live relay processor.

    Args:
        queue: Queue to consume block headers from.
    """
    processor = LiveRelayProcessor(queue)
    await processor.run()


if __name__ == "__main__":
    # For testing purposes
    test_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    asyncio.run(main(test_queue))
