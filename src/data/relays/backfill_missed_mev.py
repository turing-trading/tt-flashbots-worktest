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
)
from rich.table import Table
from sqlalchemy import select
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

    async def _check_existing_payloads(
        self, session: AsyncSession, block_numbers: list[int]
    ) -> dict[int, list[str]]:
        """Check which blocks already have relay payloads in database.

        Args:
            session: Database session
            block_numbers: List of block numbers to check

        Returns:
            Dictionary mapping block_number to list of relays that have it
        """
        # Query relays_payloads by block_number directly
        block_to_relays: dict[int, list[str]] = {}

        # Process in batches
        batch_size = 1000
        for i in range(0, len(block_numbers), batch_size):
            batch = block_numbers[i : i + batch_size]

            stmt = select(RelaysPayloadsDB.block_number, RelaysPayloadsDB.relay).where(
                RelaysPayloadsDB.block_number.in_(batch)
            )

            result = await session.execute(stmt)
            rows = result.fetchall()

            for block_number, relay in rows:
                if block_number not in block_to_relays:
                    block_to_relays[block_number] = []
                block_to_relays[block_number].append(relay)

        self.logger.info(f"Found {len(block_to_relays)} blocks already in database")
        return block_to_relays

    async def _fetch_block_from_relay(
        self, client: httpx.AsyncClient, relay: str, block_number: int
    ) -> list[RelaysPayloads]:
        """Fetch data for a specific block from a relay.

        Args:
            client: HTTP client
            relay: Relay URL
            block_number: Block number to fetch

        Returns:
            List of relay payloads (usually 0 or 1)
        """
        url = f"https://{relay}{self.endpoint}"

        # Query by block_number - fetch around this block and filter
        params = {"block_number": str(block_number)}

        try:
            response = await client.get(url, params=params, timeout=30.0)
            response.raise_for_status()

            data = TypeAdapter(list[RelaysPayloads]).validate_json(response.text)

            # Filter to only the specific block we want
            matching = [item for item in data if item.block_number == block_number]
            return matching

        except httpx.TimeoutException:
            self.logger.warning(f"Timeout fetching block {block_number} from {relay}")
            return []
        except httpx.HTTPStatusError as e:
            if e.response.status_code in [404, 400]:
                # Relay doesn't have this data or doesn't support block_number param
                return []
            self.logger.warning(
                f"HTTP error fetching block {block_number} from {relay}: {e}"
            )
            return []
        except Exception as e:
            self.logger.error(f"Error fetching block {block_number} from {relay}: {e}")
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
        values = [{**p.model_dump(), "relay": canonical_name} for p in payloads]

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

            if result:
                stored = await self._store_payloads(session, result, relay)
                if stored > 0:
                    found_relays.append(relay)
                    total_stored += stored
                    self.logger.info(
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

        self.console.print(f"[cyan]Processing {len(block_numbers):,} blocks[/cyan]\n")

        # Get active relays
        relays = RELAYS
        self.console.print(
            f"[cyan]Trying {len(relays)} relays: {', '.join(relays)}[/cyan]\n"
        )

        async with AsyncSessionLocal() as session:
            # Check which blocks already exist in database
            self.console.print(
                "[yellow]Checking existing payloads in database...[/yellow]"
            )
            existing_payloads = await self._check_existing_payloads(
                session, block_numbers
            )

            # Filter to blocks we still need to fetch
            blocks_to_fetch = [
                bn for bn in block_numbers if bn not in existing_payloads
            ]

            self.console.print(
                f"[green]Already have {len(existing_payloads):,} blocks in database[/green]"
            )
            self.console.print(
                f"[yellow]Need to fetch {len(blocks_to_fetch):,} blocks from relays[/yellow]\n"
            )

            if not blocks_to_fetch:
                self.console.print("[green]All blocks already in database![/green]")
                return {
                    "total_blocks": len(block_numbers),
                    "found_blocks": len(existing_payloads),
                    "results": [],
                }

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
                console=self.console,
            )

            async with httpx.AsyncClient(timeout=30.0) as client:
                with progress:
                    task_id = progress.add_task(
                        "Fetching blocks from relays",
                        total=len(blocks_to_fetch),
                    )

                    for block_number in blocks_to_fetch:
                        result = await self._try_fetch_block(
                            client, session, block_number, relays
                        )
                        results.append(result)

                        if result["found_relays"]:
                            found_count += 1

                        progress.update(task_id, advance=1)

        # Display results
        self._display_results(results, found_count, len(blocks_to_fetch))

        return {
            "total_blocks": len(blocks_to_fetch),
            "found_blocks": found_count + len(existing_payloads),
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
        self.console.print(f"\n[bold green]Backfill Complete[/bold green]")
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
