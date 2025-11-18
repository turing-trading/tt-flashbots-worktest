"""Debug and fix titanrelay.xyz specific slot ranges that are failing to backfill.

This script investigates why certain slot ranges are not being filled properly
and implements alternative strategies to recover the data.
"""

from asyncio import run
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
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
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.relays.db import RelaysPayloadsDB
from src.data.relays.models import RelaysPayloads
from src.helpers.db import AsyncSessionLocal
from src.helpers.logging import get_logger

# Specific problematic slot ranges for titanrelay.xyz
TITAN_GAPS = [
    {"from_slot": 11818198, "to_slot": 11861398, "estimated": 43201},
    {"from_slot": 11976598, "to_slot": 12149398, "estimated": 172801},
    {"from_slot": 12163798, "to_slot": 12300598, "estimated": 136801},
]

RELAY_URL = "https://titanrelay.xyz"
RELAY_NAME = "titanrelay.xyz"


class TitanGapDebugger:
    """Debug and fix titanrelay.xyz specific gaps."""

    def __init__(self, log_file: str | None = None):
        """Initialize debugger.

        Args:
            log_file: Path to log file (defaults to debug_titan_gaps.log)
        """
        self.logger = get_logger("titan_gap_debugger", log_level="INFO")
        self.console = Console()

        # Set up file logging
        if log_file is None:
            log_file = str(Path(__file__).parent / "debug_titan_gaps.log")

        import logging

        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        # Log session start
        self.logger.info("=" * 80)
        self.logger.info(
            f"Titan Gap Debug Session Started: {datetime.now().isoformat()}"
        )
        self.logger.info(f"Relay: {RELAY_NAME}")
        self.logger.info(f"Gap count: {len(TITAN_GAPS)}")
        self.logger.info("=" * 80)

    async def _check_existing_coverage(
        self, session: AsyncSession, from_slot: int, to_slot: int
    ) -> dict[str, Any]:
        """Check what data already exists in database for this range.

        Args:
            session: Database session
            from_slot: Start of range
            to_slot: End of range

        Returns:
            Dictionary with coverage statistics
        """
        stmt = (
            select(RelaysPayloadsDB)
            .where(RelaysPayloadsDB.relay == RELAY_NAME)
            .where(RelaysPayloadsDB.slot >= from_slot)
            .where(RelaysPayloadsDB.slot <= to_slot)
        )

        result = await session.execute(stmt)
        existing = result.fetchall()

        if existing:
            slots = [row[0].slot for row in existing]
            return {
                "count": len(existing),
                "min_slot": min(slots),
                "max_slot": max(slots),
                "slots": sorted(slots),
            }
        else:
            return {"count": 0, "min_slot": None, "max_slot": None, "slots": []}

    async def _fetch_with_cursor(
        self, client: httpx.AsyncClient, cursor: int, limit: int = 20
    ) -> list[RelaysPayloads]:
        """Fetch data from relay API with specific cursor and limit.

        Args:
            client: HTTP client
            cursor: Slot cursor to fetch from
            limit: Number of records to fetch

        Returns:
            List of relay payloads
        """
        url = f"{RELAY_URL}/relay/v1/data/bidtraces/proposer_payload_delivered"
        params = {"cursor": cursor, "limit": limit}

        try:
            response = await client.get(url, params=params, timeout=30.0)
            response.raise_for_status()
            data = response.json()

            if not data:
                return []

            payloads = [RelaysPayloads(**item) for item in data]
            return payloads

        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP error at cursor {cursor}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching cursor {cursor}: {e}")
            return []

    async def _sample_slot_range(
        self,
        client: httpx.AsyncClient,
        from_slot: int,
        to_slot: int,
        sample_points: int = 10,
    ) -> dict[str, Any]:
        """Sample different points in the slot range to understand data distribution.

        Args:
            client: HTTP client
            from_slot: Start of range
            to_slot: End of range
            sample_points: Number of points to sample

        Returns:
            Dictionary with sampling results
        """
        self.console.print(
            f"\n[cyan]Sampling {sample_points} points in range {from_slot}-{to_slot}...[/cyan]"
        )

        # Calculate sample points evenly distributed across range
        range_size = to_slot - from_slot
        step = range_size // (sample_points - 1) if sample_points > 1 else 0
        cursors = [to_slot - (i * step) for i in range(sample_points)]

        results = []
        for cursor in cursors:
            payloads = await self._fetch_with_cursor(client, cursor, limit=50)
            results.append(
                {
                    "cursor": cursor,
                    "count": len(payloads),
                    "has_data": len(payloads) > 0,
                    "min_slot": min(p.slot for p in payloads) if payloads else None,
                    "max_slot": max(p.slot for p in payloads) if payloads else None,
                }
            )
            self.logger.info(
                f"Sample cursor {cursor}: {len(payloads)} payloads "
                f"(range: {results[-1]['min_slot']}-{results[-1]['max_slot']})"
            )

        # Analyze results
        data_points = sum(1 for r in results if r["has_data"])
        total_payloads = sum(r["count"] for r in results)

        return {
            "sample_points": sample_points,
            "data_points": data_points,
            "coverage_pct": (data_points / sample_points * 100)
            if sample_points > 0
            else 0,
            "total_payloads": total_payloads,
            "avg_per_point": total_payloads / sample_points if sample_points > 0 else 0,
            "results": results,
        }

    async def _exhaustive_fetch(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        from_slot: int,
        to_slot: int,
        strategy: str = "adaptive",
    ) -> dict[str, Any]:
        """Exhaustively fetch all available data in range using specified strategy.

        Args:
            client: HTTP client
            session: Database session
            from_slot: Start of range
            to_slot: End of range
            strategy: Fetching strategy ('adaptive', 'small_limit', 'large_jumps')

        Returns:
            Dictionary with fetch results
        """
        self.console.print(
            f"\n[cyan]Starting exhaustive fetch with strategy: {strategy}[/cyan]"
        )

        current_cursor = to_slot
        total_fetched = 0
        total_stored = 0
        requests_made = 0
        consecutive_empty = 0

        # Strategy parameters
        if strategy == "small_limit":
            limit = 50
            max_consecutive_empty = 3
            jump_size = 500
        elif strategy == "large_jumps":
            limit = 200
            max_consecutive_empty = 2
            jump_size = 2000
        else:  # adaptive
            limit = 20
            max_consecutive_empty = 5
            jump_size = 1000

        self.logger.info(
            f"Strategy: {strategy}, limit={limit}, "
            f"max_consecutive_empty={max_consecutive_empty}, jump_size={jump_size}"
        )

        # Progress tracking
        estimated_blocks = to_slot - from_slot + 1
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=self.console,
        )

        with progress:
            task_id = progress.add_task(
                f"Fetching {from_slot}-{to_slot}",
                total=estimated_blocks,
            )

            while current_cursor >= from_slot and requests_made < 1000:
                requests_made += 1

                # Fetch data
                payloads = await self._fetch_with_cursor(
                    client, current_cursor, limit=limit
                )

                if not payloads:
                    consecutive_empty += 1
                    self.logger.debug(
                        f"Empty response at cursor {current_cursor} ({consecutive_empty} consecutive)"
                    )

                    if consecutive_empty >= max_consecutive_empty:
                        # Jump back
                        jump_back = min(jump_size, current_cursor - from_slot)
                        if jump_back > 0:
                            self.logger.info(
                                f"Jumping back {jump_back} slots from {current_cursor} "
                                f"after {consecutive_empty} empties"
                            )
                            current_cursor = current_cursor - jump_back
                            consecutive_empty = 0
                            continue
                        else:
                            self.logger.info("Reached start of range, stopping")
                            break
                    else:
                        current_cursor = max(current_cursor - 1, from_slot - 1)
                        continue

                # Reset consecutive empty counter
                consecutive_empty = 0

                # Filter payloads to range
                filtered = [p for p in payloads if from_slot <= p.slot <= to_slot]

                if filtered:
                    # Store in database
                    stored = await self._store_payloads(session, filtered)
                    total_fetched += len(filtered)
                    total_stored += stored
                    progress.update(task_id, advance=len(filtered))

                    min_slot = min(p.slot for p in filtered)
                    self.logger.info(
                        f"Cursor {current_cursor}: fetched {len(filtered)}, "
                        f"stored {stored}, min_slot={min_slot}"
                    )
                    current_cursor = min_slot - 1
                else:
                    # All payloads outside range
                    min_slot_in_response = min(p.slot for p in payloads)
                    if min_slot_in_response < from_slot:
                        self.logger.info("All payloads below range, stopping")
                        break
                    else:
                        current_cursor = min_slot_in_response - 1

                if current_cursor < from_slot:
                    break

        return {
            "strategy": strategy,
            "total_fetched": total_fetched,
            "total_stored": total_stored,
            "requests_made": requests_made,
            "final_cursor": current_cursor,
        }

    async def _store_payloads(
        self, session: AsyncSession, payloads: list[RelaysPayloads]
    ) -> int:
        """Store payloads in database using session merge.

        Args:
            session: Database session
            payloads: List of payloads to store

        Returns:
            Number of new records stored
        """
        if not payloads:
            return 0

        stored_count = 0
        for payload in payloads:
            # Check if record exists (primary key is slot, relay)
            primary_key = (payload.slot, RELAY_NAME)
            existing = await session.get(RelaysPayloadsDB, primary_key)

            if not existing:
                # Create new record
                db_record = RelaysPayloadsDB(
                    slot=payload.slot,
                    block_number=payload.block_number,
                    block_hash=payload.block_hash,
                    builder_pubkey=payload.builder_pubkey,
                    proposer_pubkey=payload.proposer_pubkey,
                    proposer_fee_recipient=payload.proposer_fee_recipient,
                    value=payload.value,
                    relay=RELAY_NAME,
                    parent_hash=payload.parent_hash,
                    gas_limit=payload.gas_limit,
                    gas_used=payload.gas_used,
                    num_tx=payload.num_tx,
                )
                session.add(db_record)
                stored_count += 1

        await session.commit()
        return stored_count

    async def analyze_gap(
        self, gap: dict[str, Any], client: httpx.AsyncClient, session: AsyncSession
    ) -> dict[str, Any]:
        """Analyze a single gap to understand the issue.

        Args:
            gap: Gap dictionary with from_slot, to_slot, estimated
            client: HTTP client
            session: Database session

        Returns:
            Analysis results
        """
        from_slot = gap["from_slot"]
        to_slot = gap["to_slot"]
        estimated = gap["estimated"]

        self.console.print(
            f"\n[bold blue]Analyzing Gap: {from_slot}-{to_slot}[/bold blue]"
        )
        self.console.print(f"[cyan]Estimated blocks: {estimated:,}[/cyan]")

        # Check existing coverage
        self.console.print(
            "\n[yellow]1. Checking existing database coverage...[/yellow]"
        )
        coverage = await self._check_existing_coverage(session, from_slot, to_slot)
        self.console.print(f"Found {coverage['count']:,} existing blocks in database")
        if coverage["count"] > 0:
            self.console.print(f"Range: {coverage['min_slot']}-{coverage['max_slot']}")

        # Sample the range
        self.console.print("\n[yellow]2. Sampling slot range...[/yellow]")
        sampling = await self._sample_slot_range(
            client, from_slot, to_slot, sample_points=20
        )
        self.console.print(
            f"Data coverage: {sampling['coverage_pct']:.1f}% "
            f"({sampling['data_points']}/{sampling['sample_points']} sample points have data)"
        )
        self.console.print(
            f"Average payloads per sample: {sampling['avg_per_point']:.1f}"
        )

        return {
            "gap": gap,
            "existing_coverage": coverage,
            "sampling": sampling,
        }

    async def fix_gap(
        self,
        gap: dict[str, Any],
        client: httpx.AsyncClient,
        session: AsyncSession,
        strategy: str = "adaptive",
    ) -> dict[str, Any]:
        """Attempt to fix a gap using exhaustive fetching.

        Args:
            gap: Gap dictionary
            client: HTTP client
            session: Database session
            strategy: Fetching strategy to use

        Returns:
            Fix results
        """
        from_slot = gap["from_slot"]
        to_slot = gap["to_slot"]

        self.console.print(
            f"\n[bold green]Fixing Gap: {from_slot}-{to_slot}[/bold green]"
        )

        # Get initial coverage
        initial_coverage = await self._check_existing_coverage(
            session, from_slot, to_slot
        )
        initial_count = initial_coverage["count"]

        # Exhaustive fetch
        fetch_result = await self._exhaustive_fetch(
            client, session, from_slot, to_slot, strategy=strategy
        )

        # Get final coverage
        final_coverage = await self._check_existing_coverage(
            session, from_slot, to_slot
        )
        final_count = final_coverage["count"]

        new_blocks = final_count - initial_count

        self.console.print("\n[green]✓ Gap fix complete[/green]")
        self.console.print(f"Initial blocks: {initial_count:,}")
        self.console.print(f"Final blocks: {final_count:,}")
        self.console.print(f"New blocks added: {new_blocks:,}")
        self.console.print(f"Requests made: {fetch_result['requests_made']}")

        return {
            "gap": gap,
            "initial_count": initial_count,
            "final_count": final_count,
            "new_blocks": new_blocks,
            "fetch_result": fetch_result,
        }

    async def run_analysis(self) -> None:
        """Run analysis on all titan gaps."""
        self.console.print(
            "[bold blue]Titan Gap Debugger - Analysis Mode[/bold blue]\n"
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            async with AsyncSessionLocal() as session:
                results = []

                for gap in TITAN_GAPS:
                    result = await self.analyze_gap(gap, client, session)
                    results.append(result)

                # Display summary table
                self.console.print("\n[bold]Analysis Summary:[/bold]")
                table = Table()
                table.add_column("Slot Range", style="cyan")
                table.add_column("Estimated", justify="right", style="yellow")
                table.add_column("Existing", justify="right", style="green")
                table.add_column("Coverage %", justify="right", style="magenta")
                table.add_column("Sample Data %", justify="right", style="blue")

                for result in results:
                    gap = result["gap"]
                    coverage = result["existing_coverage"]
                    sampling = result["sampling"]

                    slot_range = f"{gap['from_slot']}-{gap['to_slot']}"
                    estimated = gap["estimated"]
                    existing = coverage["count"]
                    coverage_pct = (existing / estimated * 100) if estimated > 0 else 0
                    sample_pct = sampling["coverage_pct"]

                    table.add_row(
                        slot_range,
                        f"{estimated:,}",
                        f"{existing:,}",
                        f"{coverage_pct:.1f}%",
                        f"{sample_pct:.1f}%",
                    )

                self.console.print(table)

    async def run_fix(self, strategy: str = "adaptive") -> None:
        """Run fix on all titan gaps.

        Args:
            strategy: Fetching strategy ('adaptive', 'small_limit', 'large_jumps')
        """
        self.console.print(
            f"[bold blue]Titan Gap Debugger - Fix Mode ({strategy})[/bold blue]\n"
        )

        async with httpx.AsyncClient(timeout=30.0) as client:
            async with AsyncSessionLocal() as session:
                results = []

                for gap in TITAN_GAPS:
                    result = await self.fix_gap(gap, client, session, strategy=strategy)
                    results.append(result)

                # Display summary
                self.console.print("\n[bold]Fix Summary:[/bold]")
                table = Table()
                table.add_column("Slot Range", style="cyan")
                table.add_column("Initial", justify="right", style="yellow")
                table.add_column("Final", justify="right", style="green")
                table.add_column("Added", justify="right", style="magenta")
                table.add_column("Requests", justify="right", style="blue")

                total_added = 0
                for result in results:
                    gap = result["gap"]
                    slot_range = f"{gap['from_slot']}-{gap['to_slot']}"

                    table.add_row(
                        slot_range,
                        f"{result['initial_count']:,}",
                        f"{result['final_count']:,}",
                        f"{result['new_blocks']:,}",
                        f"{result['fetch_result']['requests_made']}",
                    )
                    total_added += result["new_blocks"]

                self.console.print(table)
                self.console.print(
                    f"\n[bold green]Total blocks added: {total_added:,}[/bold green]"
                )


async def main():
    """Main entry point."""
    import sys

    debugger = TitanGapDebugger()

    # Parse command line argument
    mode = sys.argv[1] if len(sys.argv) > 1 else "analyze"
    strategy = sys.argv[2] if len(sys.argv) > 2 else "adaptive"

    if mode == "analyze":
        await debugger.run_analysis()
        debugger.console.print("\n[cyan]Next steps:[/cyan]")
        debugger.console.print(
            "  Run fix: poetry run python src/data/relays/debug_titan_gaps.py fix"
        )
        debugger.console.print(
            "  Strategies: adaptive (default), small_limit, large_jumps"
        )
    elif mode == "fix":
        await debugger.run_fix(strategy=strategy)
    else:
        debugger.console.print(f"[red]Unknown mode: {mode}[/red]")
        debugger.console.print(
            "Usage: python debug_titan_gaps.py [analyze|fix] [strategy]"
        )
        debugger.console.print("Strategies: adaptive, small_limit, large_jumps")


if __name__ == "__main__":
    run(main())
