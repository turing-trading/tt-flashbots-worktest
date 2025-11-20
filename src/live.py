"""Unified Live Ethereum Block Processor.

This module handles real-time processing of Ethereum blocks using a queue-based
architecture where each block header is processed sequentially through all stages.

Processing flow:
1. WebSocket receives block headers → Queue
2. Consumer processes each header:
   - Fetch/insert block data
   - Fetch/insert builder balance
   - Fetch/insert relay payloads
   - Compute/insert PBS analysis

No SQL joins - all data is read from what was just inserted.

Usage:
    python src/live.py
"""

from datetime import UTC, datetime
import json
import os
import signal
import sys

from typing import TYPE_CHECKING, Any

import asyncio

from dotenv import load_dotenv
import httpx
from websockets.asyncio.client import connect

from src.analysis.builder_name import parse_builder_name_from_extra_data
from src.analysis.db import AnalysisPBSV3DB
from src.analysis.models import AnalysisPBSV3
from src.data.adjustments.db import UltrasoundAdjustmentDB
from src.data.blocks.db import BlockDB
from src.data.blocks.models import Block
from src.data.builders.db import BuilderBalancesDB, ExtraBuilderBalanceDB
from src.data.builders.known_builder_addresses import KNOWN_BUILDER_ADDRESSES
from src.data.builders.models import BuilderBalance, ExtraBuilderBalance
from src.data.relays.constants import RELAYS
from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.models import RelaysPayloads
from src.helpers.db import upsert_models
from src.helpers.logging import get_logger
from src.helpers.parsers import (
    parse_hex_block_number,
    parse_hex_int,
    parse_hex_timestamp,
    wei_to_eth,
)


if TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection


# Load environment variables
load_dotenv()

logger = get_logger(__name__)


class LiveProcessor:
    """Unified processor for live Ethereum blocks with queue-based architecture."""

    def __init__(self) -> None:
        """Initialize the live processor.

        Raises:
            ValueError: If ETH_WS_URL or ETH_RPC_URL environment variables are not set
        """
        ws_url = os.getenv("ETH_WS_URL")
        rpc_url = os.getenv("ETH_RPC_URL")

        if not ws_url:
            msg = "ETH_WS_URL environment variable is not set"
            raise ValueError(msg)
        if not rpc_url:
            msg = "ETH_RPC_URL environment variable is not set"
            raise ValueError(msg)

        self.ws_url: str = ws_url
        self.rpc_url: str = rpc_url

        # HTTP client for RPC and relay API calls
        self.http_client = httpx.AsyncClient(timeout=30.0)

        # Queue for block headers
        self.headers_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)

        # Stats
        self.blocks_received = 0
        self.blocks_processed = 0
        self.builders_processed = 0
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
                logger.info("Connecting to %s", self.ws_url)
                self.connection_status = "Connecting"

                async with connect(
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
                            "Successfully subscribed to newHeads: %s", subscription_id
                        )
                        self.connection_status = "Connected"
                        retry_delay = 1.0  # Reset retry delay on successful connection

                        # Stream blocks
                        await self._stream_blocks(websocket)
                    else:
                        logger.error("Subscription failed: %s", response_data)
                        self.connection_status = "Subscription failed"

            except ConnectionError as e:
                logger.warning("WebSocket connection closed: %s", e)
                self.connection_status = "Disconnected"
            except Exception as e:
                logger.exception("WebSocket error")
                self.connection_status = f"Error: {str(e)[:50]}"

            if not self.should_shutdown:
                # Exponential backoff
                self.reconnect_count += 1
                logger.info(
                    "Reconnecting in %s s (attempt %s)",
                    retry_delay,
                    self.reconnect_count,
                )
                self.connection_status = f"Reconnecting in {retry_delay}s"
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)

    async def _stream_blocks(self, websocket: ClientConnection) -> None:
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
                    self.last_block_time = datetime.now(UTC)

                    # Log block info
                    logger.info(
                        "New block #%s hash=%s...",
                        self.last_block_number,
                        block_header.get("hash", "N/A")[:10],
                    )

                    # Put header in queue (non-blocking with timeout)
                    await self.headers_queue.put(block_header)

            except json.JSONDecodeError:
                logger.exception("Failed to decode WebSocket message")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error processing block header")

    async def process_headers_from_queue(self) -> None:
        """Consume block headers from queue and process each one concurrently."""
        logger.info("Header queue consumer started")

        # Track active processing tasks to allow concurrent processing
        active_tasks: set[asyncio.Task[None]] = set()

        while not self.should_shutdown:
            try:
                # Clean up completed tasks
                done_tasks = {task for task in active_tasks if task.done()}
                for task in done_tasks:
                    try:
                        # Retrieve any exceptions that occurred
                        task.result()
                    except Exception:
                        logger.exception("Task failed")
                active_tasks -= done_tasks

                # Get header from queue with timeout to allow shutdown checks
                try:
                    header = await asyncio.wait_for(
                        self.headers_queue.get(), timeout=1.0
                    )
                except TimeoutError:
                    # No item in queue, continue to check shutdown flag
                    continue

                # Process this header in a concurrent task
                task = asyncio.create_task(self._process_header(header))
                active_tasks.add(task)

                # Mark queue item as done
                self.headers_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Header queue consumer cancelled")
                break
            except Exception:
                logger.exception("Error in queue consumer")

        # Cancel and wait for all active tasks to complete on shutdown
        if active_tasks:
            logger.info("Cancelling %s active tasks...", len(active_tasks))
            for task in active_tasks:
                task.cancel()
            # Wait for tasks to finish cancellation
            await asyncio.gather(*active_tasks, return_exceptions=True)
            logger.info("All tasks cancelled")

    async def _process_header(self, header: dict[str, Any]) -> None:
        """Process a single block header through all stages.

        Args:
            header: Block header dictionary from newHeads.
        """
        block_number = parse_hex_block_number(header)
        try:
            # Get miner address for later stages
            miner_address = header.get("miner", "")

            # Stage 1: Fetch/insert block
            block = await self._store_block(block_number)
            if not block:
                logger.error("Failed to store block #%s, skipping", block_number)
                return

            # Stage 2: Fetch/insert builder balance
            balance_data = await self._store_builder_balance(
                block_number, miner_address
            )

            # Stage 3: Fetch extra builder balances (for V3)
            extra_builder_data = await self._fetch_extra_builder_balances(
                block_number, miner_address
            )

            # Stage 4: Fetch/insert relay payloads (with smart waiting)
            relay_data = await self._store_relay_payloads_with_retry(block_number)

            # Stage 5: Fetch Ultrasound adjustment if applicable
            # First extract slot from relay data
            slot = relay_data[0].get("slot") if relay_data else None
            adjustment_data = await self._fetch_ultrasound_adjustment(slot, relay_data)

            # Stage 6: Compute/insert analysis (using data from previous stages)
            await self._store_analysis_simple(
                block_number,
                block.timestamp,
                block.extra_data,
                balance_data,
                relay_data,
                extra_builder_data,
                adjustment_data,
            )

        except asyncio.CancelledError:
            logger.debug("Header processing cancelled")
            return
        except Exception:
            logger.exception("Error processing block #%s", block_number)

    async def _store_block(self, block_number: int) -> Block | None:
        """Fetch and store full block data from RPC.

        Args:
            block_number: Block number to fetch.

        Returns:
            Block object if successful, None otherwise.
        """
        try:
            logger.debug("Fetching block #%s from RPC...", block_number)
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
                logger.error("Block #%s not found in RPC response", block_number)
                return None

            block_data = result["result"]
            logger.debug("Parsing block #%s data...", block_number)

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

            logger.debug("Storing block #%s in database...", block_number)
            # Store in database
            await upsert_models(
                db_model_class=BlockDB,
                pydantic_models=[block],
            )

            self.blocks_processed += 1
            logger.info("Stored block #%s", block_number)

            return block

        except Exception:
            logger.exception("Failed to store block #%s", block_number)
            return None

    async def _store_builder_balance(
        self, block_number: int, miner_address: str | None
    ) -> dict[str, Any] | None:
        """Calculate and store builder balance change.

        Args:
            block_number: Block number.
            miner_address: Builder/miner address.

        Returns:
            Dictionary with balance data if successful, None otherwise.
        """
        try:
            if not miner_address:
                logger.debug("No miner address for block #%s", block_number)
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
            builder_balance = BuilderBalance(
                block_number=block_number,
                miner=miner_address,
                balance_before=balance_before,
                balance_after=balance_after,
                balance_increase=balance_increase,
            )

            # Store in database
            await upsert_models(
                db_model_class=BuilderBalancesDB,
                pydantic_models=[builder_balance],
            )

            self.builders_processed += 1
            logger.info(
                "Stored builder balance for block #%s: %s ETH",
                block_number,
                f"{wei_to_eth(balance_increase):.4f}",
            )

            return {
                "balance_increase": balance_increase,
            }

        except Exception:
            logger.exception(
                "Failed to store builder balance for block #%s", block_number
            )
            return None

    async def _store_relay_payloads_with_retry(
        self, block_number: int
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

        Returns:
            List of relay data dictionaries.
        """
        relay_data: list[dict[str, Any]] = []
        max_total_minutes = 10
        initial_wait_minutes = 5

        # Wait 5 minutes before first fetch attempt
        logger.info(
            "Block #%s: Waiting %s m before fetching relay data",
            block_number,
            initial_wait_minutes,
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
                "Block #%s: No relay data found, retrying in 1m (elapsed: %s m)",
                block_number,
                elapsed_minutes,
            )
            await asyncio.sleep(60)
            elapsed_minutes += 1

        if not relay_data:
            logger.info(
                "Block #%s: No relay data found after %s m of retries",
                block_number,
                elapsed_minutes,
            )

        found_relays = [r["relay"] for r in relay_data] if relay_data else []
        if found_relays:
            self.relays_processed += len(found_relays)
            logger.info("Stored relay payloads for block #%s", block_number)
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
                        "Relay %s error for block #%s: %s", relay, block_number, result
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
                        await upsert_models(
                            db_model_class=RelaysPayloadsDB,
                            pydantic_models=[payload],
                            extra_fields={"relay": relay},
                        )

                        # Track this relay data for analysis
                        relay_data_list.append({
                            "relay": relay,
                            "value": int(payload_data.get("value", 0)),
                        })

                    except Exception:
                        logger.exception(
                            "Failed to store relay payload from %s for block #%s",
                            relay,
                            block_number,
                        )

            if relay_data_list:
                self.relays_processed += 1
                logger.info("Stored relay payloads for block #%s", block_number)

            return relay_data_list

        except Exception:
            logger.exception(
                "Failed to fetch relay payloads for block #%s", block_number
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
            logger.debug("HTTP error fetching %s: %s", relay, e)
            return None
        except Exception as e:
            logger.debug("Error fetching %s: %s", relay, e)
            return None

    async def _fetch_extra_builder_balances(
        self, block_number: int, miner_address: str
    ) -> list[dict[str, Any]]:
        """Fetch and store extra builder balance increases for known builder addresses.

        Args:
            block_number: Block number
            miner_address: The builder/miner address

        Returns:
            List of balance increase dicts for builder addresses
        """
        try:
            # Check if this miner has known builder addresses
            builder_addresses = KNOWN_BUILDER_ADDRESSES.get(miner_address)
            if not builder_addresses:
                return []

            # Fetch and store balances for all builder addresses
            balance_data = []
            for builder_address in builder_addresses:
                # Create RPC requests for balance before and after
                before_request = {
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [builder_address, hex(block_number - 1)],
                    "id": 1,
                }
                after_request = {
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [builder_address, hex(block_number)],
                    "id": 2,
                }

                # Execute both requests in parallel
                responses = await asyncio.gather(
                    self.http_client.post(self.rpc_url, json=before_request),
                    self.http_client.post(self.rpc_url, json=after_request),
                )

                balance_before = parse_hex_int(responses[0].json().get("result", "0x0"))
                balance_after = parse_hex_int(responses[1].json().get("result", "0x0"))
                balance_increase = balance_after - balance_before

                # Create model
                extra_builder_balance = ExtraBuilderBalance(
                    block_number=block_number,
                    builder_address=builder_address,
                    miner=miner_address,
                    balance_before=balance_before,
                    balance_after=balance_after,
                    balance_increase=balance_increase,
                )

                # Store in database
                await upsert_models(
                    db_model_class=ExtraBuilderBalanceDB,
                    pydantic_models=[extra_builder_balance],
                )

                balance_data.append({
                    "builder_address": builder_address,
                    "balance_increase": balance_increase,
                })

            logger.info(
                "Stored %s extra builder balances for block #%s",
                len(balance_data),
                block_number,
            )
            return balance_data

        except Exception:
            logger.exception(
                "Failed to fetch/store extra builder balances for block #%s",
                block_number,
            )
            return []

    async def _fetch_ultrasound_adjustment(
        self, slot: int | None, relay_data: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        """Fetch and store Ultrasound adjustment data if Ultrasound relay is present.

        Args:
            slot: Beacon chain slot number
            relay_data: List of relay payload dicts

        Returns:
            Dict with delta (relay fee) or None if not available
        """
        if slot is None or not relay_data:
            return None

        # Check if Ultrasound relay is in the relay data
        ultrasound_relay = "relay-analytics.ultrasound.money"
        has_ultrasound = any(r.get("relay") == ultrasound_relay for r in relay_data)

        if not has_ultrasound:
            return None

        try:
            # Fetch from Ultrasound API
            url = f"https://relay-analytics.ultrasound.money/ultrasound/v1/data/adjustments?slot={slot}"
            response = await self.http_client.get(url, timeout=10.0)
            response.raise_for_status()
            data = response.json()

            # API returns {"data": [...]}
            adjustment_data = None
            if data.get("data"):
                adjustment_data = data["data"][0]

            # Create adjustment record
            now = datetime.now(UTC)

            if adjustment_data is None:
                # No adjustment found
                adjustment_record = UltrasoundAdjustmentDB(
                    slot=slot,
                    fetched_at=now,
                    has_adjustment=False,
                )
                delta = None
            else:
                # Parse adjustment data
                adjusted_value = adjustment_data.get("adjusted_value")
                delta = adjustment_data.get("delta")
                submitted_value = adjustment_data.get("submitted_value")

                adjustment_record = UltrasoundAdjustmentDB(
                    slot=slot,
                    adjusted_block_hash=adjustment_data.get("adjusted_block_hash"),
                    adjusted_value=int(adjusted_value) if adjusted_value else None,
                    block_number=adjustment_data.get("block_number"),
                    builder_pubkey=adjustment_data.get("builder_pubkey"),
                    delta=int(delta) if delta else None,
                    submitted_block_hash=adjustment_data.get("submitted_block_hash"),
                    submitted_received_at=adjustment_data.get("submitted_received_at"),
                    submitted_value=int(submitted_value) if submitted_value else None,
                    fetched_at=now,
                    has_adjustment=True,
                )

            # Store in database
            await upsert_models(
                db_model_class=UltrasoundAdjustmentDB,
                pydantic_models=[adjustment_record],
            )

            # Return relay fee if available
            if adjustment_data and delta is not None:
                delta_value = int(delta)
                logger.info(
                    "Stored Ultrasound adjustment for slot %s: delta=%s ETH",
                    slot,
                    wei_to_eth(delta_value),
                )
                return {"delta": delta_value}

            return None

        except httpx.HTTPError as e:
            logger.debug("HTTP error fetching adjustment for slot %s: %s", slot, e)
            return None
        except Exception:
            logger.exception("Error fetching adjustment for slot %s", slot)
            return None

    async def _store_analysis_simple(
        self,
        block_number: int,
        block_timestamp: datetime,
        extra_data: str,
        balance_data: dict[str, Any] | None,
        relay_data: list[dict[str, Any]],
        extra_builder_data: list[dict[str, Any]],
        adjustment_data: dict[str, Any] | None,
    ) -> None:
        """Compute and store PBS analysis V3 using data from previous stages.

        No database queries - all data passed as parameters.

        Args:
            block_number: Block number.
            block_timestamp: Block timestamp.
            extra_data: Block extra_data for builder name parsing.
            balance_data: Balance data from builder step.
            relay_data: Relay data from relay step.
            extra_builder_data: Extra builder balance data.
            adjustment_data: Ultrasound adjustment data with delta (relay fee).
        """
        try:
            # Process relay data
            relays_list = [r["relay"] for r in relay_data] if relay_data else None
            n_relays = len(relay_data) if relay_data else 0
            is_block_vanilla = n_relays == 0

            # Get max relay value (proposer subsidy) and slot
            proposer_subsidy = 0.0
            slot = None
            if relay_data:
                max_value = max(r["value"] for r in relay_data)
                proposer_subsidy = wei_to_eth(max_value) or 0.0
                # Get slot from first relay (all relays have same slot for a block)
                slot = relay_data[0].get("slot")

            # Get builder balance increase
            builder_balance_increase = 0.0
            if balance_data:
                builder_balance_increase = (
                    wei_to_eth(balance_data["balance_increase"]) or 0.0
                )

            # Calculate builder extra transfers (only positive values)
            builder_extra_transfers = 0.0
            if extra_builder_data:
                positive_transfers = [
                    d["balance_increase"]
                    for d in extra_builder_data
                    if d["balance_increase"] > 0
                ]
                if positive_transfers:
                    builder_extra_transfers = sum(
                        wei_to_eth(t) or 0.0 for t in positive_transfers
                    )

            # Get relay fee from adjustment data (if available)
            relay_fee = None
            if adjustment_data and adjustment_data.get("delta") is not None:
                relay_fee = wei_to_eth(adjustment_data["delta"])

            # Calculate total value including all components
            total_value = (
                builder_balance_increase + proposer_subsidy + (relay_fee or 0.0)
            )

            if total_value < 0 and builder_extra_transfers > 0:
                total_value += builder_extra_transfers

            # Parse builder name from extra_data
            builder_name = parse_builder_name_from_extra_data(extra_data)

            # Create analysis model V3
            analysis = AnalysisPBSV3(
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

            # Store in database
            await upsert_models(
                db_model_class=AnalysisPBSV3DB,
                pydantic_models=[analysis],
            )

            self.analysis_processed += 1
            logger.info(
                "Stored PBS analysis V3 for block #%s (builder: %s, slot: %s)",
                block_number,
                builder_name,
                slot,
            )

        except Exception:
            logger.exception("Failed to store analysis for block #%s", block_number)

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
    except Exception:
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
