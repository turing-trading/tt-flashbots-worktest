"""Retry fetching data for detected gaps in relay coverage."""

import json
import logging
from argparse import ArgumentParser
from asyncio import run, sleep
from datetime import datetime
from pathlib import Path

import httpx
from pydantic import TypeAdapter
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.relays.constants import (
    ENDPOINTS,
    RELAY_LIMITS,
    RELAY_NAME_MAPPING,
    RELAYS,
)
from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.gap_detection import estimate_missing_blocks
from src.data.relays.models import RelaysPayloads
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger


class GapRetryProcessor:
    """Process gaps by retrying API calls for missing data."""

    def __init__(self, gaps_file: str = "relay_gaps.json", log_file: str | None = None):
        """Initialize gap retry processor.

        Args:
            gaps_file: Path to gaps JSON file
            log_file: Optional path to log file (default: src/data/relays/retry_gaps.log)
        """
        self.gaps_file = gaps_file
        self.endpoint = ENDPOINTS.get(
            "proposer_payload_delivered",
            "/relay/v1/data/bidtraces/proposer_payload_delivered",
        )
        self.default_limit = 200

        # Set up logging to both console and file
        self.logger = get_logger("gap_retry", log_level="INFO")

        # Add file handler
        if log_file is None:
            log_file = str(Path(__file__).parent / "retry_gaps.log")

        self.log_file = log_file
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        # Log session start
        self.logger.info("=" * 80)
        self.logger.info(f"Gap Retry Session Started: {datetime.now().isoformat()}")
        self.logger.info(f"Gaps file: {gaps_file}")
        self.logger.info(f"Log file: {log_file}")
        self.logger.info("=" * 80)

        self.console = Console()

    def _get_limit_for_relay(self, relay: str) -> int:
        """Get the appropriate limit for a specific relay."""
        return RELAY_LIMITS.get(relay, self.default_limit)

    def _get_canonical_relay_name(self, relay: str) -> str:
        """Get the canonical relay name for database storage."""
        return RELAY_NAME_MAPPING.get(relay, relay)

    def _load_gaps(self) -> list[dict]:
        """Load gaps from JSON file.

        Returns:
            List of gap dictionaries
        """
        gaps_path = Path(self.gaps_file)

        if not gaps_path.exists():
            error_msg = f"Gaps file not found: {gaps_path}"
            self.logger.error(error_msg)
            self.console.print(f"[red]Error: {error_msg}[/red]")
            return []

        with open(gaps_path) as f:
            gaps = json.load(f)

        self.logger.info(f"Loaded {len(gaps)} gap(s) from {gaps_path}")
        return gaps

    async def _fetch_data(
        self,
        client: httpx.AsyncClient,
        relay: str,
        cursor: int,
    ) -> list[RelaysPayloads]:
        """Fetch data from the relay endpoint with retry logic."""
        url = f"https://{relay}{self.endpoint}"
        limit = self._get_limit_for_relay(relay)
        params = {"cursor": str(cursor), "limit": str(limit)}

        max_retries = 5
        base_delay = 1  # Start with 1 second delay

        for attempt in range(max_retries):
            try:
                response = await client.get(url, params=params, timeout=60.0)
                response.raise_for_status()

                data = TypeAdapter(list[RelaysPayloads]).validate_json(response.text)
                # Deduplicate by slot
                result = list({item.slot: item for item in data}.values())

                if result:
                    self.logger.info(
                        f"{relay}: Fetched {len(result)} payloads at cursor {cursor}"
                    )

                await sleep(1)  # Sleep for 1 second to avoid rate limiting
                return result

            except httpx.TimeoutException:
                # retry_delay = base_delay * (2**attempt)
                # self.logger.warning(
                #     f"{relay}: Timeout at cursor {cursor}, retrying in {retry_delay}s "
                #     f"(attempt {attempt + 1}/{max_retries})"
                # )
                # await sleep(retry_delay)
                # continue
                return []

            except Exception as e:
                if response.status_code == 408:
                    return []
                retry_delay = base_delay * (2**attempt)
                self.logger.warning(
                    f"{relay}: Error at cursor {cursor}: {e} "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                await sleep(retry_delay)

        # If all retries failed, return empty list
        self.logger.error(
            f"{relay}: All {max_retries} retries failed at cursor {cursor}"
        )
        return []

    async def _store_payloads(
        self,
        session: AsyncSession,
        payloads: list[RelaysPayloads],
        relay: str,
    ) -> None:
        """Store relay payloads in the database."""
        if not payloads:
            return

        # Convert Pydantic models to dicts and add canonical relay name
        canonical_name = self._get_canonical_relay_name(relay)
        values = [
            {**payload.model_dump(), "relay": canonical_name} for payload in payloads
        ]

        # Upsert payloads into the database using ON CONFLICT
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

        # self.logger.info(
        #     f"{relay}: Stored {len(payloads)} payloads to database from slots {min(p.slot for p in payloads)} to {max(p.slot for p in payloads)}"
        # )

    async def _retry_gap(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        gap: dict,
        progress: Progress,
        task_id: TaskID,
    ) -> dict:
        """Retry fetching data for a specific gap.

        Args:
            client: HTTP client
            session: Database session
            gap: Gap dictionary with relay, from_slot, to_slot
            progress: Progress display
            task_id: Progress task ID

        Returns:
            Dictionary with gap results
        """
        relay = gap["relay"]
        from_slot = gap["from_slot"]
        to_slot = gap["to_slot"]
        estimated_blocks = estimate_missing_blocks(from_slot, to_slot)

        self.logger.info(
            f"Retrying {relay}: slots {from_slot}-{to_slot} (~{estimated_blocks:,} blocks)"
        )

        # Set up progress
        progress.update(task_id, total=estimated_blocks, completed=0)

        current_cursor = to_slot  # Start from the end and work backwards
        total_fetched = 0
        consecutive_empty = 0
        max_consecutive_empty = 5  # Allow more empty responses for sparse relays
        empty_jump_size = 1000  # Jump back by this many slots when hitting empties
        last_cursor = None  # Track last cursor to detect stuck loops
        max_attempts = 500  # Maximum number of fetch attempts to prevent infinite loops

        attempts = 0
        while current_cursor >= from_slot and attempts < max_attempts:
            attempts += 1

            # Detect infinite loop - if cursor hasn't changed, we're stuck
            if last_cursor is not None and current_cursor == last_cursor:
                self.logger.warning(
                    f"{relay}: Cursor stuck at {current_cursor}, stopping"
                )
                break
            last_cursor = current_cursor

            payloads = await self._fetch_data(client, relay, current_cursor)

            if not payloads:
                consecutive_empty += 1
                self.logger.debug(
                    f"{relay}: Empty response at cursor {current_cursor} "
                    f"({consecutive_empty}/{max_consecutive_empty})"
                )

                if consecutive_empty >= max_consecutive_empty:
                    # Jump back to skip potential gaps in relay data
                    jump_back = min(empty_jump_size, current_cursor - from_slot)
                    if jump_back > 0:
                        self.logger.info(
                            f"{relay}: {consecutive_empty} consecutive empties, "
                            f"jumping back {jump_back} slots from {current_cursor}"
                        )
                        current_cursor = current_cursor - jump_back
                        consecutive_empty = 0  # Reset counter after jump
                        continue
                    else:
                        # Can't jump back anymore, we're done
                        self.logger.warning(
                            f"{relay}: {consecutive_empty} consecutive empty responses "
                            f"and can't jump back, stopping"
                        )
                        break
                else:
                    # Move cursor back by 1 and continue
                    current_cursor = max(current_cursor - 1, from_slot - 1)
                    continue

            # Reset consecutive empty counter - we got data!
            consecutive_empty = 0

            # Filter payloads to only include those in the gap range
            filtered_payloads = [p for p in payloads if from_slot <= p.slot <= to_slot]

            if filtered_payloads:
                # Store in database
                await self._store_payloads(session, filtered_payloads, relay)
                total_fetched += len(filtered_payloads)

                # Update progress
                progress.update(task_id, advance=len(filtered_payloads))

                # Move cursor to before the earliest slot we just fetched
                min_slot = min(p.slot for p in filtered_payloads)
                current_cursor = min_slot - 1

                self.logger.debug(
                    f"{relay}: Stored {len(filtered_payloads)} payloads, "
                    f"moving cursor to {current_cursor}"
                )
            else:
                # No payloads in range - all payloads are outside the gap
                # Move cursor back by the oldest payload we got
                min_slot_in_response = min(p.slot for p in payloads)
                if min_slot_in_response < from_slot:
                    # All payloads are before our gap, we're done
                    self.logger.info(
                        f"{relay}: All payloads before gap range "
                        f"(got slot {min_slot_in_response} < {from_slot}), stopping"
                    )
                    break
                else:
                    # Payloads are after our gap range, move cursor back
                    current_cursor = min_slot_in_response - 1

            # Stop if we've gone before the start of the gap
            if current_cursor < from_slot:
                break

        if attempts >= max_attempts:
            self.logger.warning(
                f"{relay}: Reached maximum attempts ({max_attempts}), stopping"
            )

        result = {
            "relay": relay,
            "from_slot": from_slot,
            "to_slot": to_slot,
            "fetched": total_fetched,
            "estimated": estimated_blocks,
            "success": total_fetched > 0,
        }

        self.logger.info(
            f"{relay}: Completed gap [{from_slot}-{to_slot}] - "
            f"Fetched {total_fetched}/{estimated_blocks} blocks "
            f"({'SUCCESS' if result['success'] else 'FAILED'})"
        )

        return result

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _process_relay_gaps(
        self,
        relay: str,
        relay_gaps: list[dict],
        client: httpx.AsyncClient,
        progress: Progress,
    ) -> list[dict]:
        """Process all gaps for a single relay sequentially (one request at a time).

        Args:
            relay: Relay name
            relay_gaps: List of gaps for this relay (sorted most recent first)
            client: HTTP client (shared across relays)
            progress: Progress display

        Returns:
            List of result dictionaries for this relay
        """
        total_blocks = sum(
            estimate_missing_blocks(g["from_slot"], g["to_slot"]) for g in relay_gaps
        )
        self.logger.info(
            f"{relay}: Starting to process {len(relay_gaps)} gap(s) "
            f"(~{total_blocks:,} blocks)"
        )

        results = []

        # Create dedicated session for this relay to avoid concurrent DB access issues
        async with AsyncSessionLocal() as session:
            # Process gaps sequentially - ONE request at a time per relay
            for i, gap in enumerate(relay_gaps, 1):
                from_slot = gap["from_slot"]
                to_slot = gap["to_slot"]
                estimated = estimate_missing_blocks(from_slot, to_slot)

                self.logger.info(
                    f"{relay}: Processing gap {i}/{len(relay_gaps)} "
                    f"[{from_slot}-{to_slot}]"
                )

                task_id = progress.add_task(
                    f"{relay} [{from_slot}-{to_slot}]",
                    total=estimated,
                )

                result = await self._retry_gap(client, session, gap, progress, task_id)
                results.append(result)

        # Summary for this relay
        successful = sum(1 for r in results if r["success"])
        total_fetched = sum(r["fetched"] for r in results)
        self.logger.info(
            f"{relay}: Completed all gaps - "
            f"{successful}/{len(relay_gaps)} successful, "
            f"{total_fetched:,} total blocks fetched"
        )

        return results

    async def process_gaps(self) -> list[dict]:
        """Process all gaps from the JSON file in parallel per relay.

        Gaps are processed:
        - In parallel across different relays
        - Sequentially within each relay (most recent first)

        Returns:
            List of result dictionaries
        """
        from asyncio import gather

        # Load gaps
        gaps = self._load_gaps()

        if not gaps:
            self.console.print("[yellow]No gaps to process[/yellow]")
            return []

        # Group gaps by relay
        gaps_by_relay: dict[str, list[dict]] = {}
        for gap in gaps:
            relay = gap["relay"]
            if relay not in gaps_by_relay:
                gaps_by_relay[relay] = []
            gaps_by_relay[relay].append(gap)

        # Sort each relay's gaps by to_slot descending (most recent first)
        for relay in list(gaps_by_relay.keys()):
            if relay not in RELAYS:
                del gaps_by_relay[relay]
                continue
            gaps_by_relay[relay].sort(key=lambda g: g["to_slot"], reverse=True)

        total_gaps = len(gaps)
        num_relays = len(gaps_by_relay)

        self.console.print(
            f"[bold blue]Processing {total_gaps} gap(s) across {num_relays} relay(s)[/bold blue]"
        )
        self.console.print(
            "[cyan]Strategy: Parallel per relay, most recent gaps first[/cyan]\n"
        )

        # Display relay breakdown
        for relay, relay_gaps in sorted(gaps_by_relay.items()):
            total_blocks = sum(
                estimate_missing_blocks(g["from_slot"], g["to_slot"])
                for g in relay_gaps
            )
            self.console.print(
                f"  {relay}: {len(relay_gaps)} gap(s), ~{total_blocks:,} blocks"
            )

        self.console.print()

        # Create tables
        await self.create_tables()

        # Set up progress display
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

        all_results = []

        async with httpx.AsyncClient() as client:
            with progress:
                # Process each relay in parallel
                # Each relay processes its gaps sequentially (one request at a time)
                tasks = [
                    self._process_relay_gaps(relay, relay_gaps, client, progress)
                    for relay, relay_gaps in gaps_by_relay.items()
                ]

                # Gather results from all relays running in parallel
                relay_results = await gather(*tasks, return_exceptions=True)

                # Flatten results and handle exceptions
                for relay, result in zip(
                    gaps_by_relay.keys(), relay_results, strict=True
                ):
                    if isinstance(result, Exception):
                        self.logger.error(f"Error processing {relay}: {result}")
                    elif isinstance(result, list):
                        all_results.extend(result)

        # Log session summary
        total_fetched = sum(r["fetched"] for r in all_results)
        total_estimated = sum(r["estimated"] for r in all_results)
        successful = sum(1 for r in all_results if r["success"])

        self.logger.info("=" * 80)
        self.logger.info("Session Summary:")
        self.logger.info(f"  Total gaps processed: {len(all_results)}")
        self.logger.info(f"  Successful: {successful}")
        self.logger.info(f"  Failed: {len(all_results) - successful}")
        self.logger.info(f"  Total blocks fetched: {total_fetched:,}")
        self.logger.info(f"  Total estimated: {total_estimated:,}")
        self.logger.info(f"Gap Retry Session Ended: {datetime.now().isoformat()}")
        self.logger.info("=" * 80)

        return all_results

    def _display_results(self, results: list[dict]) -> None:
        """Display processing results.

        Args:
            results: List of result dictionaries
        """
        from rich.table import Table

        if not results:
            return

        table = Table(title="Gap Retry Results")
        table.add_column("Relay", style="cyan")
        table.add_column("Slot Range", style="magenta")
        table.add_column("Fetched", justify="right", style="green")
        table.add_column("Estimated", justify="right", style="yellow")
        table.add_column("Success", justify="center", style="bold")

        for result in results:
            success_icon = "✓" if result["success"] else "✗"
            success_color = "green" if result["success"] else "red"

            table.add_row(
                result["relay"],
                f"{result['from_slot']}-{result['to_slot']}",
                f"{result['fetched']:,}",
                f"{result['estimated']:,}",
                f"[{success_color}]{success_icon}[/{success_color}]",
            )

        self.console.print("\n")
        self.console.print(table)

        # Summary
        total_fetched = sum(r["fetched"] for r in results)
        total_estimated = sum(r["estimated"] for r in results)
        successful = sum(1 for r in results if r["success"])

        self.console.print("\n[bold]Summary:[/bold]")
        self.console.print(f"  Total gaps processed: {len(results)}")
        self.console.print(f"  Successful: {successful}")
        self.console.print(f"  Failed: {len(results) - successful}")
        self.console.print(f"  Total blocks fetched: {total_fetched:,}")
        self.console.print(f"  Total estimated: {total_estimated:,}")


async def main(gaps_file: str):
    """Run gap retry processor.

    Args:
        gaps_file: Path to gaps JSON file
    """
    processor = GapRetryProcessor(gaps_file=gaps_file)

    # Display log file location
    processor.console.print(f"[dim]Logging to: {processor.log_file}[/dim]\n")

    results = await processor.process_gaps()

    if results:
        processor._display_results(results)
        processor.console.print("\n[bold green]✓ Gap retry complete[/bold green]")
        processor.console.print(f"[dim]Full logs saved to: {processor.log_file}[/dim]")
    else:
        processor.console.print("\n[yellow]No gaps were processed[/yellow]")
        processor.console.print(f"[dim]Logs saved to: {processor.log_file}[/dim]")


if __name__ == "__main__":
    parser = ArgumentParser(description="Retry fetching data for detected gaps")
    parser.add_argument(
        "--gaps-file",
        default="relay_gaps.json",
        help="Path to gaps JSON file (default: relay_gaps.json)",
    )
    args = parser.parse_args()

    run(main(args.gaps_file))
