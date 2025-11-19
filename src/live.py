"""Unified Live Ethereum Block Processor.

This module handles real-time processing of Ethereum blocks using a queue-based
architecture where each block header is processed sequentially through all stages.

Processing flow:
1. WebSocket receives block headers → Queue
2. Consumer processes each header:
   - Fetch/insert block data
   - Fetch/insert proposer balance
   - Fetch/insert relay payloads
   - Compute/insert PBS analysis

No SQL joins - all data is read from what was just inserted.

Usage:
    python src/live.py
"""

import asyncio
import json
import os
import signal
import sys
from datetime import datetime
from typing import Any

import httpx
import websockets
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from websockets.legacy.client import WebSocketClientProtocol

from src.analysis.builder_name import parse_builder_name_from_extra_data
from src.analysis.db import AnalysisPBSV2DB
from src.analysis.models import AnalysisPBSV2
from src.data.blocks.db import BlockDB
from src.data.blocks.models import Block
from src.data.proposers.db import ProposerBalancesDB
from src.data.proposers.models import ProposerBalance
from src.data.relays.constants import RELAYS
from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.models import RelaysPayloads
from src.helpers.db import AsyncSessionLocal, upsert_model
from src.helpers.logging import get_logger
from src.helpers.parsers import (
    parse_hex_block_number,
    parse_hex_int,
    parse_hex_timestamp,
    wei_to_eth,
)

# Load environment variables
load_dotenv()

logger = get_logger(__name__)


class LiveProcessor:
    """Unified processor for live Ethereum blocks with queue-based architecture."""

    def __init__(self) -> None:
        """Initialize the live processor."""
        ws_url = os.getenv("ETH_WS_URL")
        rpc_url = os.getenv("ETH_RPC_URL")

        if not ws_url:
            raise ValueError("ETH_WS_URL environment variable is not set")
        if not rpc_url:
            raise ValueError("ETH_RPC_URL environment variable is not set")

        self.ws_url: str = ws_url
        self.rpc_url: str = rpc_url

        # HTTP client for RPC and relay API calls
        self.http_client = httpx.AsyncClient(timeout=30.0)

        # Queue for block headers
        self.headers_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)

        # Stats
        self.blocks_received = 0
        self.blocks_processed = 0
        self.proposers_processed = 0
        self.relays_processed = 0
        self.analysis_processed = 0
        self.last_block_number = 0
        self.last_block_time: datetime | None = None
        self.connection_status = "Initializing"
        self.reconnect_count = 0

        # Shutdown flag
        self.should_shutdown = False

    async def connect_and_subscribe(self) -> None:
        """Connect to WebSocket and subscribe to newHeads with auto-reconnect."""
        retry_delay = 1.0
        max_retry_delay = 60.0

        while not self.should_shutdown:
            try:
                logger.info(f"Connecting to {self.ws_url}")
                self.connection_status = "Connecting"

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as websocket:
                    # Subscribe to newHeads
                    subscribe_msg = {
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": ["newHeads"],
                    }
                    await websocket.send(json.dumps(subscribe_msg))

                    # Wait for subscription confirmation
                    response = await websocket.recv()
                    response_data = json.loads(response)

                    if "result" in response_data:
                        subscription_id = response_data["result"]
                        logger.info(
                            f"Successfully subscribed to newHeads: {subscription_id}"
                        )
                        self.connection_status = "Connected"
                        retry_delay = 1.0  # Reset retry delay on successful connection

                        # Stream blocks
                        await self._stream_blocks(websocket)  # type: ignore
                    else:
                        logger.error(f"Subscription failed: {response_data}")
                        self.connection_status = "Subscription failed"

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                self.connection_status = "Disconnected"
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                self.connection_status = f"Error: {str(e)[:50]}"

            if not self.should_shutdown:
                # Exponential backoff
                self.reconnect_count += 1
                logger.info(
                    f"Reconnecting in {retry_delay}s (attempt {self.reconnect_count})"
                )
                self.connection_status = f"Reconnecting in {retry_delay}s"
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)

    async def _stream_blocks(self, websocket: WebSocketClientProtocol) -> None:
        """Stream block headers from WebSocket and put them in queue.

        Args:
            websocket: Connected WebSocket client.
        """
        async for message in websocket:
            if self.should_shutdown:
                break

            try:
                data = json.loads(message)

                # Check if this is a newHeads notification
                if "params" in data and "result" in data["params"]:
                    block_header = data["params"]["result"]

                    # Update stats
                    self.blocks_received += 1
                    self.last_block_number = int(block_header.get("number", "0x0"), 16)
                    self.last_block_time = datetime.now()

                    # Log block info
                    logger.info(
                        f"New block #{self.last_block_number} "
                        f"hash={block_header.get('hash', 'N/A')[:10]}..."
                    )

                    # Put header in queue (non-blocking with timeout)
                    try:
                        await asyncio.wait_for(
                            self.headers_queue.put(block_header), timeout=1.0
                        )
                    except TimeoutError:
                        logger.warning(
                            f"Queue full, dropping block #{self.last_block_number}"
                        )

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode WebSocket message: {e}")
            except Exception as e:
                logger.error(f"Error processing block header: {e}")

    async def process_headers_from_queue(self) -> None:
        """Consume block headers from queue and process each one."""
        logger.info("Header queue consumer started")

        while not self.should_shutdown:
            try:
                # Get header from queue with timeout to allow shutdown checks
                header = await asyncio.wait_for(self.headers_queue.get(), timeout=1.0)

                # Process this header in a separate task
                asyncio.create_task(self._process_header(header))

                # Mark task as done
                self.headers_queue.task_done()

            except TimeoutError:
                # No item in queue, continue to check shutdown flag
                continue
            except asyncio.CancelledError:
                logger.info("Header queue consumer cancelled")
                break
            except Exception as e:
                logger.error(f"Error in queue consumer: {e}")

    async def _process_header(self, header: dict[str, Any]) -> None:
        """Process a single block header through all stages.

        Args:
            header: Block header dictionary from newHeads.
        """
        try:
            block_number = parse_hex_block_number(header)
            timestamp = parse_hex_timestamp(header["timestamp"])

            # Stage 1: Fetch/insert block
            block = await self._store_block(block_number)
            if not block:
                logger.error(f"Failed to store block #{block_number}, skipping")
                return

            # Stage 2: Fetch/insert proposer balance
            balance_data = await self._store_proposer_balance(
                block_number, header.get("miner")
            )

            # Stage 3: Fetch/insert relay payloads (with smart waiting)
            relay_data = await self._store_relay_payloads_with_retry(
                block_number, timestamp
            )

            # Stage 4: Compute/insert analysis (using data from previous stages)
            await self._store_analysis_simple(
                block_number,
                block.timestamp,
                block.extra_data,
                balance_data,
                relay_data,
            )

        except Exception as e:
            logger.error(f"Error processing block #{block_number}: {e}")

    async def _store_block(self, block_number: int) -> Block | None:
        """Fetch and store full block data from RPC.

        Args:
            block_number: Block number to fetch.

        Returns:
            Block object if successful, None otherwise.
        """
        try:
            # Fetch full block via RPC
            rpc_request = {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(block_number), False],  # False = only tx hashes
                "id": 1,
            }

            response = await self.http_client.post(self.rpc_url, json=rpc_request)
            response.raise_for_status()
            result = response.json()

            if "result" not in result or result["result"] is None:
                logger.error(f"Block #{block_number} not found in RPC response")
                return None

            block_data = result["result"]

            # Parse block data
            block = Block(
                number=parse_hex_int(block_data.get("number")),
                hash=block_data.get("hash", ""),
                parent_hash=block_data.get("parentHash", ""),
                nonce=block_data.get("nonce", ""),
                sha3_uncles=block_data.get("sha3Uncles", ""),
                transactions_root=block_data.get("transactionsRoot", ""),
                state_root=block_data.get("stateRoot", ""),
                receipts_root=block_data.get("receiptsRoot", ""),
                miner=block_data.get("miner", ""),
                size=parse_hex_int(block_data.get("size")),
                extra_data=block_data.get("extraData", ""),
                gas_limit=parse_hex_int(block_data.get("gasLimit")),
                gas_used=parse_hex_int(block_data.get("gasUsed")),
                timestamp=parse_hex_timestamp(block_data["timestamp"]),
                transaction_count=len(block_data.get("transactions", [])),
                base_fee_per_gas=wei_to_eth(
                    parse_hex_int(block_data.get("baseFeePerGas"), 0)
                ),
            )

            # Store in database
            await upsert_model(
                db_model_class=BlockDB,
                pydantic_model=block,
                primary_key_value=block.number,
            )

            self.blocks_processed += 1
            logger.info(f"Stored block #{block_number}")

            return block

        except Exception as e:
            logger.error(f"Failed to store block #{block_number}: {e}")
            return None

    async def _store_proposer_balance(
        self, block_number: int, miner_address: str | None
    ) -> dict[str, Any] | None:
        """Calculate and store proposer balance change.

        Args:
            block_number: Block number.
            miner_address: Proposer/miner address.

        Returns:
            Dictionary with balance data if successful, None otherwise.
        """
        try:
            if not miner_address:
                logger.warning(f"No miner address for block #{block_number}")
                return None

            # Get balance before (at block N-1) and after (at block N)
            before_request = {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [miner_address, hex(block_number - 1)],
                "id": 1,
            }

            after_request = {
                "jsonrpc": "2.0",
                "method": "eth_getBalance",
                "params": [miner_address, hex(block_number)],
                "id": 2,
            }

            # Make parallel requests
            responses = await asyncio.gather(
                self.http_client.post(self.rpc_url, json=before_request),
                self.http_client.post(self.rpc_url, json=after_request),
            )

            balance_before = parse_hex_int(responses[0].json().get("result", "0x0"))
            balance_after = parse_hex_int(responses[1].json().get("result", "0x0"))

            # Calculate balance increase
            balance_increase = balance_after - balance_before

            # Create model
            proposer_balance = ProposerBalance(
                block_number=block_number,
                miner=miner_address,
                balance_before=balance_before,
                balance_after=balance_after,
                balance_increase=balance_increase,
            )

            # Store in database
            await upsert_model(
                db_model_class=ProposerBalancesDB,
                pydantic_model=proposer_balance,
                primary_key_value=block_number,
            )

            self.proposers_processed += 1
            logger.info(
                f"Stored proposer balance for block #{block_number}: "
                f"{wei_to_eth(balance_increase):.4f} ETH"
            )

            return {
                "balance_increase": balance_increase,
            }

        except Exception as e:
            logger.error(
                f"Failed to store proposer balance for block #{block_number}: {e}"
            )
            return None

    async def _store_relay_payloads_with_retry(
        self, block_number: int, timestamp: datetime
    ) -> list[dict[str, Any]]:
        """Store relay payloads with delayed retries only (no immediate fetch).

        Wait 5 minutes first, then try fetching every minute until 10 minutes total:
        - 5:00 - First fetch (after 5 min wait)
        - 6:00 - Retry (after 1 min wait)
        - 7:00 - Retry (after 1 min wait)
        - 8:00 - Retry (after 1 min wait)
        - 9:00 - Retry (after 1 min wait)
        - 10:00 - Final retry (after 1 min wait)
        - STOP after 10 minutes total

        Args:
            block_number: Block number.
            timestamp: Block timestamp (unused, kept for API compatibility).

        Returns:
            List of relay data dictionaries.
        """
        relay_data: list[dict[str, Any]] = []
        max_total_minutes = 10
        initial_wait_minutes = 5

        # Wait 5 minutes before first fetch attempt
        logger.info(
            f"Block #{block_number}: Waiting {initial_wait_minutes}m before fetching relay data"
        )
        await asyncio.sleep(initial_wait_minutes * 60)

        if self.should_shutdown:
            return relay_data

        elapsed_minutes = initial_wait_minutes

        # Try fetching every minute from 5min to 10min
        while (
            elapsed_minutes <= max_total_minutes
            and not relay_data
            and not self.should_shutdown
        ):
            relay_data = await self._store_relay_payloads(block_number)

            if relay_data:
                break

            # Don't wait if we've reached the time limit
            if elapsed_minutes >= max_total_minutes:
                break

            # Wait 1 minute before next retry
            logger.info(
                f"Block #{block_number}: No relay data found, "
                f"retrying in 1m (elapsed: {elapsed_minutes}m)"
            )
            await asyncio.sleep(60)
            elapsed_minutes += 1

        if not relay_data:
            logger.warning(
                f"Block #{block_number}: No relay data found after {elapsed_minutes}m of retries"
            )

        return relay_data

    async def _store_relay_payloads(self, block_number: int) -> list[dict[str, Any]]:
        """Fetch and store relay payloads for a block.

        Args:
            block_number: Block number.

        Returns:
            List of relay data dictionaries with relay name and value.
        """
        relay_data_list: list[dict[str, Any]] = []

        try:
            # Query all relays concurrently
            tasks = []
            for relay in RELAYS:
                url = (
                    f"https://{relay}/relay/v1/data/bidtraces/"
                    f"proposer_payload_delivered?block_number={block_number}"
                )
                tasks.append(self._fetch_relay_data(relay, url))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for i, result in enumerate(results):
                relay = RELAYS[i]
                if isinstance(result, Exception):
                    logger.debug(
                        f"Relay {relay} error for block #{block_number}: {result}"
                    )
                    continue

                if not result or not isinstance(result, list):
                    continue

                # Store each payload
                for payload_data in result:
                    try:
                        payload = RelaysPayloads(
                            slot=payload_data.get("slot", 0),
                            parent_hash=payload_data.get("parent_hash", ""),
                            block_hash=payload_data.get("block_hash", ""),
                            builder_pubkey=payload_data.get("builder_pubkey", ""),
                            proposer_pubkey=payload_data.get("proposer_pubkey", ""),
                            proposer_fee_recipient=payload_data.get(
                                "proposer_fee_recipient", ""
                            ),
                            gas_limit=payload_data.get("gas_limit", 0),
                            gas_used=payload_data.get("gas_used", 0),
                            value=int(payload_data.get("value", 0)),
                            block_number=payload_data.get("block_number", block_number),
                            num_tx=payload_data.get("num_tx", 0),
                        )

                        # Upsert with relay name
                        async with AsyncSessionLocal() as session:
                            stmt = pg_insert(RelaysPayloadsDB).values(
                                relay=relay, **payload.model_dump()
                            )
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["slot", "relay"],
                                set_={
                                    "parent_hash": stmt.excluded.parent_hash,
                                    "block_hash": stmt.excluded.block_hash,
                                    "builder_pubkey": stmt.excluded.builder_pubkey,
                                    "proposer_pubkey": stmt.excluded.proposer_pubkey,
                                    "proposer_fee_recipient": stmt.excluded.proposer_fee_recipient,
                                    "gas_limit": stmt.excluded.gas_limit,
                                    "gas_used": stmt.excluded.gas_used,
                                    "value": stmt.excluded.value,
                                    "block_number": stmt.excluded.block_number,
                                    "num_tx": stmt.excluded.num_tx,
                                },
                            )
                            await session.execute(stmt)
                            await session.commit()

                        # Track this relay data for analysis
                        relay_data_list.append(
                            {
                                "relay": relay,
                                "value": int(payload_data.get("value", 0)),
                            }
                        )

                    except Exception as e:
                        logger.error(
                            f"Failed to store relay payload from {relay} "
                            f"for block #{block_number}: {e}"
                        )

            if relay_data_list:
                self.relays_processed += 1
                logger.info(f"Stored relay payloads for block #{block_number}")

            return relay_data_list

        except Exception as e:
            logger.error(
                f"Failed to fetch relay payloads for block #{block_number}: {e}"
            )
            return []

    async def _fetch_relay_data(self, relay: str, url: str) -> list[dict] | None:
        """Fetch data from a single relay.

        Args:
            relay: Relay name.
            url: Full URL to fetch.

        Returns:
            List of payload dictionaries, or None if error.
        """
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None  # No data available yet
            logger.debug(f"HTTP error fetching {relay}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error fetching {relay}: {e}")
            return None

    async def _store_analysis_simple(
        self,
        block_number: int,
        block_timestamp: datetime,
        extra_data: str,
        balance_data: dict[str, Any] | None,
        relay_data: list[dict[str, Any]],
    ) -> None:
        """Compute and store PBS analysis using data from previous stages.

        No database queries - all data passed as parameters.

        Args:
            block_number: Block number.
            block_timestamp: Block timestamp.
            extra_data: Block extra_data for builder name parsing.
            balance_data: Balance data from proposer step.
            relay_data: Relay data from relay step.
        """
        try:
            # Process relay data
            relays_list = [r["relay"] for r in relay_data] if relay_data else None
            n_relays = len(relay_data) if relay_data else 0
            is_block_vanilla = n_relays == 0

            # Get max relay value (proposer subsidy)
            proposer_subsidy = 0.0
            if relay_data:
                max_value = max(r["value"] for r in relay_data)
                proposer_subsidy = wei_to_eth(max_value) or 0.0

            # Get builder balance increase
            builder_balance_increase = 0.0
            if balance_data:
                builder_balance_increase = (
                    wei_to_eth(balance_data["balance_increase"]) or 0.0
                )

            # Calculate total value
            total_value = builder_balance_increase + proposer_subsidy

            # Parse builder name from extra_data
            builder_name = parse_builder_name_from_extra_data(extra_data)

            # Create analysis model
            analysis = AnalysisPBSV2(
                block_number=block_number,
                block_timestamp=block_timestamp,
                builder_balance_increase=builder_balance_increase,
                proposer_subsidy=proposer_subsidy,
                total_value=total_value,
                is_block_vanilla=is_block_vanilla,
                n_relays=n_relays,
                relays=relays_list,
                builder_name=builder_name,
            )

            # Store in database
            await upsert_model(
                db_model_class=AnalysisPBSV2DB,
                pydantic_model=analysis,
                primary_key_value=analysis.block_number,
            )

            self.analysis_processed += 1
            logger.info(
                f"Stored PBS analysis for block #{block_number} "
                f"(builder: {builder_name})"
            )

        except Exception as e:
            logger.error(f"Failed to store analysis for block #{block_number}: {e}")

    def shutdown(self) -> None:
        """Gracefully shutdown the processor."""
        logger.info("Shutdown signal received, stopping...")
        self.should_shutdown = True

    async def cleanup(self) -> None:
        """Clean up resources."""
        await self.http_client.aclose()

    async def run(self) -> None:
        """Run the unified live processor."""
        # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown)

        try:
            # Start both WebSocket connection and queue consumer
            tasks = [
                asyncio.create_task(self.connect_and_subscribe()),
                asyncio.create_task(self.process_headers_from_queue()),
            ]

            # Wait for tasks, but allow graceful shutdown
            await asyncio.gather(*tasks, return_exceptions=True)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.should_shutdown = True
        finally:
            await self.cleanup()

        logger.info("Live processor stopped")


async def main() -> None:
    """Main entry point."""
    try:
        processor = LiveProcessor()
        await processor.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, exiting...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
