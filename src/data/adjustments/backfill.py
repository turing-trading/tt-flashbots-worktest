"""Backfill Ultrasound relay adjustments for blocks with negative total_value."""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.adjustments.db import UltrasoundAdjustmentDB
from src.helpers.db import AsyncSessionLocal, Base, async_engine

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Reduce noise from httpx
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
console = Console()


async def fetch_adjustment_from_api(
    slot: int, client: httpx.AsyncClient
) -> tuple[bool, dict[str, Any] | None]:
    """
    Fetch adjustment data from Ultrasound relay API.

    Returns:
        (success, data) tuple where:
        - (True, data): Successfully fetched (data may be None if no adjustment exists)
        - (False, None): API error occurred, should retry later
    """
    url = f"https://relay-analytics.ultrasound.money/ultrasound/v1/data/adjustments?slot={slot}"

    try:
        response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        data = response.json()

        # API returns {"data": [...]}
        if "data" in data:
            adjustments = data["data"]
            if adjustments:
                # Return first adjustment if multiple exist
                return (True, adjustments[0])
        # Success but no adjustment found
        return (True, None)
    except httpx.HTTPError as e:
        logger.warning(f"HTTP error fetching adjustment for slot {slot}: {e}")
        return (False, None)
    except Exception as e:
        logger.error(f"Error fetching adjustment for slot {slot}: {e}")
        return (False, None)


def create_adjustment_record(
    slot: int, adjustment_data: dict[str, Any] | None
) -> UltrasoundAdjustmentDB:
    """Create adjustment database record from API response."""
    now = datetime.now(UTC)

    if adjustment_data is None:
        # No adjustment found - create record indicating absence
        return UltrasoundAdjustmentDB(
            slot=slot,
            fetched_at=now,
            has_adjustment=False,
        )

    # Parse adjustment data from API response
    # Convert string Wei values to integers
    adjusted_value = adjustment_data.get("adjusted_value")
    delta = adjustment_data.get("delta")
    submitted_value = adjustment_data.get("submitted_value")

    return UltrasoundAdjustmentDB(
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


async def create_tables() -> None:
    """Create tables if they don't exist."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_ultrasound_slots_to_process(
    session: AsyncSession,
) -> list[tuple[int, int]]:
    """
    Get Ultrasound relay slots that haven't been processed yet.

    Returns slots in descending order (most recent first).
    """
    query = text("""
        SELECT DISTINCT
            rp.slot,
            rp.block_number
        FROM relays_payloads rp
        LEFT JOIN ultrasound_adjustments ua ON rp.slot = ua.slot
        WHERE rp.relay = 'relay-analytics.ultrasound.money'
            AND ua.slot IS NULL
        ORDER BY rp.slot DESC
    """)

    result = await session.execute(query)
    rows = result.fetchall()
    return [(row[0], row[1]) for row in rows]


async def process_batch(
    slots_batch: list[tuple[int, int]],
    client: httpx.AsyncClient,
) -> list[tuple[int, bool, dict[str, Any] | None]]:
    """
    Process a batch of slots concurrently.

    Returns:
        List of (slot, success, data) tuples
    """
    tasks = [fetch_adjustment_from_api(slot, client) for slot, _ in slots_batch]
    results = await asyncio.gather(*tasks)
    return [
        (slots_batch[i][0], results[i][0], results[i][1])
        for i in range(len(slots_batch))
    ]


async def backfill_adjustments(
    limit: int | None = None,
    skip_existing: bool = True,
    batch_size: int = 100,
) -> None:
    """
    Backfill adjustments for Ultrasound relay blocks.

    Args:
        limit: Maximum number of slots to process (None for all)
        skip_existing: Skip slots that already have adjustment records (deprecated, always skipped via query)
    """
    console.print("[bold cyan]Starting Ultrasound adjustments backfill[/bold cyan]")

    # Ensure tables exist
    await create_tables()

    async with AsyncSessionLocal() as session:
        # Get Ultrasound relay slots that haven't been processed
        console.print("Querying Ultrasound relay slots to process...")
        slots_data = await get_ultrasound_slots_to_process(session)

        if not slots_data:
            console.print("[yellow]No new Ultrasound relay slots to process[/yellow]")
            return

        total_slots = len(slots_data)
        console.print(
            f"[green]Found {total_slots:,} Ultrasound relay slots to process[/green]"
        )

        # Apply limit if specified
        if limit:
            slots_data = slots_data[:limit]
            total_slots = len(slots_data)
            console.print(f"[yellow]Processing first {total_slots:,} slots[/yellow]")

        # Create HTTP client for API requests
        async with httpx.AsyncClient() as client:
            found_adjustments = 0
            api_errors = 0
            processed_count = 0

            # Create progress bar
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
                TextColumn("• [cyan]{task.fields[adjustments_found]} adj"),
                TextColumn("• [red]{task.fields[errors]} err"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    "[cyan]Processing slots...",
                    total=total_slots,
                    adjustments_found=0,
                    errors=0,
                )

                # Process in batches
                for batch_start in range(0, total_slots, batch_size):
                    batch_end = min(batch_start + batch_size, total_slots)
                    batch = slots_data[batch_start:batch_end]

                    # Fetch all adjustments for this batch concurrently
                    batch_results = await process_batch(batch, client)

                    # Create and insert records (skip API errors)
                    for slot_data, (_, success, adjustment_data) in zip(
                        batch,
                        batch_results,
                        strict=True,
                    ):
                        slot, _ = slot_data

                        if not success:
                            # API error - skip this slot, will retry later
                            api_errors += 1
                            continue

                        # Create record (success=True, data may be None)
                        adjustment_record = create_adjustment_record(
                            slot, adjustment_data
                        )
                        session.add(adjustment_record)
                        processed_count += 1

                        if adjustment_data:
                            found_adjustments += 1

                    # Update progress
                    batch_num = batch_start // batch_size + 1
                    total_batches = (total_slots + batch_size - 1) // batch_size
                    progress.update(
                        task,
                        advance=len(batch),
                        description=f"[cyan]Batch {batch_num}/{total_batches}",
                        adjustments_found=found_adjustments,
                        errors=api_errors,
                    )

                    # Commit after each batch
                    await session.commit()

            # Summary
            console.print("\n[bold green]Backfill complete![/bold green]")
            console.print(f"  Total slots attempted: [cyan]{total_slots:,}[/cyan]")
            console.print(
                f"  Successfully processed: [green]{processed_count:,}[/green]"
            )
            console.print(f"  Adjustments found: [green]{found_adjustments:,}[/green]")
            console.print(
                f"  No adjustments: [yellow]{processed_count - found_adjustments:,}[/yellow]"
            )
            if api_errors > 0:
                console.print(
                    f"  [red]API errors (will retry later): {api_errors:,}[/red]"
                )


async def main():
    """Main entry point for backfill script."""
    import sys

    # Parse command line arguments
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            print(f"Usage: {sys.argv[0]} [limit]")
            sys.exit(1)

    await backfill_adjustments(limit=limit)


if __name__ == "__main__":
    asyncio.run(main())
