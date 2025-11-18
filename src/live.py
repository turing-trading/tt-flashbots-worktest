"""Live Ethereum Block Streaming Coordinator.

This module coordinates real-time block header streaming from an Ethereum
WebSocket endpoint and distributes headers to processing modules via asyncio queues.

Architecture:
    1. Connect to Ethereum WebSocket (ETH_WS_URL)
    2. Subscribe to newHeads events
    3. Distribute block headers to all module queues
    4. Each module processes headers independently
    5. Auto-reconnect with exponential backoff on disconnect

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

import websockets
from dotenv import load_dotenv
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from websockets.legacy.client import WebSocketClientProtocol

from src.helpers.logging import get_logger

# Load environment variables
load_dotenv()

logger = get_logger(__name__)
console = Console()


class LiveBlockStreamer:
    """Coordinates live block streaming and distribution to modules."""

    def __init__(self) -> None:
        """Initialize the live block streamer."""
        self.ws_url = os.getenv("ETH_WS_URL")

        # Queues for distributing block headers to modules
        self.blocks_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.relays_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.proposers_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.builders_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.analysis_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        # Stats
        self.blocks_received = 0
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

        if not self.ws_url:
            raise ValueError("ETH_WS_URL environment variable is not set")

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
        """Stream block headers from WebSocket and distribute to queues.

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

                    # Distribute to all module queues
                    await self._distribute_header(block_header)

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode WebSocket message: {e}")
            except Exception as e:
                logger.error(f"Error processing block header: {e}")

    async def _distribute_header(self, header: dict[str, Any]) -> None:
        """Distribute block header to all module queues.

        Args:
            header: Block header dictionary.
        """
        queues = [
            ("blocks", self.blocks_queue),
            ("relays", self.relays_queue),
            ("proposers", self.proposers_queue),
            ("builders", self.builders_queue),
            ("analysis", self.analysis_queue),
        ]

        for name, queue in queues:
            try:
                # Non-blocking put with timeout
                await asyncio.wait_for(queue.put(header), timeout=1.0)
            except TimeoutError:
                logger.warning(f"Queue {name} is full, dropping block header")
            except Exception as e:
                logger.error(f"Error distributing to {name} queue: {e}")

    def _create_status_display(self) -> Panel:
        """Create rich status display panel.

        Returns:
            Rich Panel with current status.
        """
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("Key", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Status", self.connection_status)
        table.add_row("Blocks Received", str(self.blocks_received))
        table.add_row("Last Block", f"#{self.last_block_number}")

        if self.last_block_time:
            elapsed = (datetime.now() - self.last_block_time).total_seconds()
            table.add_row("Last Block Time", f"{elapsed:.1f}s ago")

        table.add_row("Reconnect Count", str(self.reconnect_count))

        # Queue status
        table.add_row("", "")
        table.add_row("Queue Status", "")
        table.add_row("  Blocks", f"{self.blocks_queue.qsize()}/100")
        table.add_row("  Relays", f"{self.relays_queue.qsize()}/100")
        table.add_row("  Proposers", f"{self.proposers_queue.qsize()}/100")
        table.add_row("  Builders", f"{self.builders_queue.qsize()}/100")
        table.add_row("  Analysis", f"{self.analysis_queue.qsize()}/100")

        return Panel(
            table,
            title="[bold]Live Ethereum Block Streamer[/bold]",
            border_style="blue",
        )

    async def run_status_display(self) -> None:
        """Run the status display loop."""
        with Live(
            self._create_status_display(), console=console, refresh_per_second=1
        ) as live:
            while not self.should_shutdown:
                await asyncio.sleep(1.0)
                live.update(self._create_status_display())

    def shutdown(self) -> None:
        """Gracefully shutdown the streamer."""
        logger.info("Shutdown signal received, stopping...")
        self.should_shutdown = True

    async def run(self) -> None:
        """Run the live block streamer coordinator."""
        # Import module processors
        from src.analysis.live import LiveAnalysisProcessorV2
        from src.data.blocks.live import LiveBlockProcessor
        from src.data.builders.live import LiveBuilderProcessor
        from src.data.proposers.live import LiveProposerProcessor
        from src.data.relays.live import LiveRelayProcessor

        # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown)

        # Create module processors
        blocks_processor = LiveBlockProcessor(self.blocks_queue)
        relays_processor = LiveRelayProcessor(self.relays_queue)
        proposers_processor = LiveProposerProcessor(self.proposers_queue)
        builders_processor = LiveBuilderProcessor(self.builders_queue)
        analysis_processor = LiveAnalysisProcessorV2(self.analysis_queue)

        # Start all tasks
        await asyncio.gather(
            self.connect_and_subscribe(),
            self.run_status_display(),
            blocks_processor.run(),
            relays_processor.run(),
            proposers_processor.run(),
            builders_processor.run(),
            analysis_processor.run(),
        )

        logger.info("Live block streamer stopped")


async def main() -> None:
    """Main entry point."""
    try:
        streamer = LiveBlockStreamer()
        await streamer.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, exiting...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
