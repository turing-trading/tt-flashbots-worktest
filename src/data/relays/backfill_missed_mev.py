"""Backfill relay data for missed MEV blocks.

This script reads the missed MEV blocks JSON file (from detect_missed_mev.py)
and attempts to fetch relay data for those specific blocks from all relays.
"""

import json
from asyncio import gather, run
from pathlib import Path
from typing import Any

import httpx
from pydantic import TypeAdapter
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.relays.constants import (
    ENDPOINTS,
    RELAY_NAME_MAPPING,
    RELAYS,
)
from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.models import RelaysPayloads
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger


class BackfillMissedMEV:
    """Backfill relay data for missed MEV blocks."""

    def __init__(self, json_file: str = "missed_mev_blocks.json"):
        """Initialize backfill.

        Args:
            json_file: Path to JSON file with missed MEV block numbers
        """
        self.json_file = json_file
        self.endpoint = ENDPOINTS.get(
            "proposer_payload_delivered",
            "/relay/v1/data/bidtraces/proposer_payload_delivered",
        )
        self.logger = get_logger("backfill_missed_mev", log_level="INFO")
        self.console = Console()

    def _get_canonical_relay_name(self, relay: str) -> str:
        """Get the canonical relay name for database storage."""
        return RELAY_NAME_MAPPING.get(relay, relay)

    def _load_missed_blocks(self) -> dict[str, Any]:
        """Load missed MEV blocks from JSON file.

        Returns:
            Dictionary with block numbers and details
        """
        json_path = Path(self.json_file)
        if not json_path.exists():
            raise FileNotFoundError(
                f"Missed MEV blocks file not found: {self.json_file}\n"
                "Run detect_missed_mev.py first to generate this file."
            )

        with open(json_path) as f:
            data = json.load(f)

        self.logger.info(
            f"Loaded {data['total_missed_blocks']} missed blocks from {self.json_file}"
        )
        return data

    async def _fetch_block_from_relay(
        self,
        client: httpx.AsyncClient,
        relay: str,
        block_number: int,
    ) -> list[RelaysPayloads]:
        """Fetch data for a specific block from a relay with retry logic.

        Args:
            client: HTTP client
            relay: Relay URL
            block_number: Block number to fetch

        Returns:
            List of relay payloads (usually 0 or 1)
        """
        from asyncio import sleep

        url = f"https://{relay}{self.endpoint}"
        params = {"block_number": str(block_number)}

        max_retries = 5
        base_delay = 1  # Start with 1 second delay

        for attempt in range(max_retries):
            try:
                response = await client.get(url, params=params, timeout=30.0)
                response.raise_for_status()

                data = TypeAdapter(list[RelaysPayloads]).validate_json(response.text)

                # Filter to only the specific block we want
                matching = [item for item in data if item.block_number == block_number]
                return matching

            except httpx.TimeoutException:
                # Retry on timeout with exponential backoff
                retry_delay = base_delay * (2**attempt)
                self.logger.warning(
                    f"Timeout from {relay} for block {block_number}, "
                    f"retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                )
                if attempt < max_retries - 1:
                    await sleep(retry_delay)
                    continue
                else:
                    self.logger.error(
                        f"Failed to fetch block {block_number} from {relay} after {max_retries} attempts"
                    )
                    return []

            except httpx.HTTPStatusError as e:
                # Retry on 408 (Request Timeout) and 409 (Conflict)
                if e.response.status_code in [408, 409]:
                    retry_delay = base_delay * (2**attempt)
                    self.logger.warning(
                        f"HTTP {e.response.status_code} from {relay} for block {block_number}, "
                        f"retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        await sleep(retry_delay)
                        continue
                    else:
                        self.logger.error(
                            f"Failed to fetch block {block_number} from {relay} "
                            f"after {max_retries} attempts: {e}"
                        )
                        return []
                elif e.response.status_code in [404, 400]:
                    # Relay doesn't have this data or doesn't support block_number param
                    return []
                else:
                    self.logger.warning(
                        f"HTTP error fetching block {block_number} from {relay}: {e}"
                    )
                    return []

            except Exception as e:
                self.logger.error(
                    f"Error fetching block {block_number} from {relay}: {e}"
                )
                return []

        # Should not reach here, but just in case
        return []

    async def _store_payloads(
        self,
        session: AsyncSession,
        payloads: list[RelaysPayloads],
        relay: str,
    ) -> int:
        """Store relay payloads in database.

        Args:
            session: Database session
            payloads: List of payloads to store
            relay: Relay name

        Returns:
            Number of payloads stored
        """
        if not payloads:
            return 0

        canonical_name = self._get_canonical_relay_name(relay)

        # Deduplicate by slot (primary key is slot + relay)
        # Keep the first occurrence of each slot
        seen_slots = set()
        unique_payloads = []
        for p in payloads:
            if p.slot not in seen_slots:
                seen_slots.add(p.slot)
                unique_payloads.append(p)

        if not unique_payloads:
            return 0

        values = [{**p.model_dump(), "relay": canonical_name} for p in unique_payloads]

        stmt = pg_insert(RelaysPayloadsDB).values(values)
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=["slot", "relay"],
            set_={
                RelaysPayloadsDB.parent_hash: excluded.parent_hash,
                RelaysPayloadsDB.block_hash: excluded.block_hash,
                RelaysPayloadsDB.builder_pubkey: excluded.builder_pubkey,
                RelaysPayloadsDB.proposer_pubkey: excluded.proposer_pubkey,
                RelaysPayloadsDB.proposer_fee_recipient: excluded.proposer_fee_recipient,
                RelaysPayloadsDB.gas_limit: excluded.gas_limit,
                RelaysPayloadsDB.gas_used: excluded.gas_used,
                RelaysPayloadsDB.value: excluded.value,
                RelaysPayloadsDB.block_number: excluded.block_number,
                RelaysPayloadsDB.num_tx: excluded.num_tx,
            },
        )
        await session.execute(stmt)
        await session.commit()

        return len(payloads)

    async def _try_fetch_block(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        block_number: int,
        relays: list[str],
    ) -> dict[str, Any]:
        """Try to fetch a block from all relays.

        Args:
            client: HTTP client
            session: Database session
            block_number: Block number
            relays: List of relay URLs to try

        Returns:
            Dictionary with fetch results
        """
        found_relays = []
        total_stored = 0

        # Try each relay in parallel
        tasks = [
            self._fetch_block_from_relay(client, relay, block_number)
            for relay in relays
        ]
        results = await gather(*tasks, return_exceptions=True)

        for relay, result in zip(relays, results, strict=True):
            if isinstance(result, Exception):
                self.logger.error(
                    f"Exception fetching block {block_number} from {relay}: {result}"
                )
                continue

            # result is now guaranteed to be list[RelaysPayloads]
            if not isinstance(result, BaseException) and result:
                stored = await self._store_payloads(session, result, relay)
                if stored > 0:
                    found_relays.append(relay)
                    total_stored += stored
                    self.logger.debug(
                        f"Found and stored block {block_number} from {relay}"
                    )

        return {
            "block_number": block_number,
            "found_relays": found_relays,
            "total_stored": total_stored,
        }

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def run(self) -> dict[str, Any]:
        """Run the backfill process.

        Returns:
            Dictionary with backfill results
        """
        self.console.print("[bold blue]Backfill Missed MEV Blocks[/bold blue]\n")

        # Create tables
        await self.create_tables()

        # Load missed blocks
        missed_data = self._load_missed_blocks()
        block_numbers = missed_data["block_numbers"]

        if not block_numbers:
            self.console.print("[yellow]No missed blocks to process[/yellow]")
            return {"total_blocks": 0, "found_blocks": 0, "results": []}

        # Sort blocks by descending order (most recent first)
        block_numbers = sorted(block_numbers, reverse=True)

        self.console.print(
            f"[cyan]Processing {len(block_numbers):,} blocks "
            f"(most recent: {block_numbers[0]:,}, oldest: {block_numbers[-1]:,})[/cyan]\n"
        )

        # Get active relays
        relays = RELAYS
        self.console.print(
            f"[cyan]Querying {len(relays)} relays in parallel: {', '.join(relays)}[/cyan]\n"
        )

        # Process blocks
        results = []
        found_count = 0

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=self.console,
        )

        async with AsyncSessionLocal() as session:
            async with httpx.AsyncClient(timeout=30.0) as client:
                with progress:
                    task_id = progress.add_task(
                        "Fetching blocks from all relays",
                        total=len(block_numbers),
                    )

                    # Iterate over each block number
                    for block_number in block_numbers:
                        result = await self._try_fetch_block(
                            client, session, block_number, relays
                        )
                        results.append(result)

                        if result["found_relays"]:
                            found_count += 1

                        progress.update(task_id, advance=1)

        # Display results
        self._display_results(results, found_count, len(block_numbers))

        return {
            "total_blocks": len(block_numbers),
            "found_blocks": found_count,
            "results": results,
        }

    def _display_results(
        self, results: list[dict], found_count: int, total_count: int
    ) -> None:
        """Display backfill results.

        Args:
            results: List of result dictionaries
            found_count: Number of blocks found
            total_count: Total blocks processed
        """
        self.console.print("\n[bold green]Backfill Complete[/bold green]")
        self.console.print(f"Total blocks processed: {total_count:,}")
        self.console.print(f"Blocks found in relays: {found_count:,}")
        self.console.print(f"Success rate: {(found_count / total_count * 100):.1f}%\n")

        # Show sample of found blocks
        found_results = [r for r in results if r["found_relays"]]
        if found_results:
            self.console.print("[bold]Sample of found blocks:[/bold]")
            table = Table()
            table.add_column("Block", style="cyan")
            table.add_column("Found in Relays", style="green")
            table.add_column("Stored", style="yellow")

            for result in found_results[:20]:
                table.add_row(
                    str(result["block_number"]),
                    ", ".join(result["found_relays"]),
                    str(result["total_stored"]),
                )

            self.console.print(table)

            if len(found_results) > 20:
                self.console.print(
                    f"\n[yellow]Showing 20 of {len(found_results)} found blocks[/yellow]"
                )


async def main():
    """Main entry point."""
    import sys

    json_file = sys.argv[1] if len(sys.argv) > 1 else "missed_mev_blocks.json"

    backfill = BackfillMissedMEV(json_file=json_file)
    await backfill.run()


if __name__ == "__main__":
    run(main())
