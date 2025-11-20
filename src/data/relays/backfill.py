"""Backfill data from relays."""

from typing import TYPE_CHECKING, cast

from asyncio import CancelledError, create_task, gather, run, sleep

from sqlalchemy import Column, select

import httpx
from pydantic import TypeAdapter
from rich.console import Console

from src.data.relays.constants import (
    BEACON_ENDPOINT,
    ENDPOINTS,
    LIMITS,
    RELAY_LIMITS,
    RELAY_NAME_MAPPING,
    RELAYS,
)
from src.data.relays.db import (
    RelaysPayloadsCheckpointsDB,
    RelaysPayloadsDB,
)
from src.data.relays.models import RelaysPayloads
from src.helpers.db import AsyncSessionLocal, upsert_models
from src.helpers.progress import create_standard_progress


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from rich.progress import TaskID


class BackfillRelayPayloadDelivered:
    """Backfill relay payload delivered data."""

    def __init__(self) -> None:
        """Initialize backfill with relay and endpoint."""
        self.endpoint = ENDPOINTS.get(
            "proposer_payload_delivered",
            "/relay/v1/data/bidtraces/proposer_payload_delivered",
        )
        self.default_limit = LIMITS.get("proposer_payload_delivered", 200)
        self.console = Console()
        self.progress = None
        self.tasks: dict[str, TaskID] = {}  # Track progress task IDs per relay

    def _get_limit_for_relay(self, relay: str) -> int:
        """Get the appropriate limit for a specific relay."""
        return RELAY_LIMITS.get(relay, self.default_limit)

    def _get_canonical_relay_name(self, relay: str) -> str:
        """Get the canonical relay name for database storage.

        Some relays use different URLs for fetching (e.g., eu-global.titanrelay.xyz)
        but should be stored under a canonical name (e.g., titanrelay.xyz).
        """
        return RELAY_NAME_MAPPING.get(relay, relay)

    async def _get_latest_slot(self) -> int:
        """Get the latest slot from the beacon endpoint."""
        url = f"{BEACON_ENDPOINT}/eth/v1/beacon/headers/finalized"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=30.0)
        response.raise_for_status()
        return int(response.json()["data"]["header"]["message"]["slot"])

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
                return list({item.slot: item for item in data}.values())

            except httpx.TimeoutException:
                # Retry on timeout with exponential backoff
                retry_delay = base_delay * (2**attempt)
                await sleep(retry_delay)
                continue

            except Exception:
                retry_delay = base_delay * (2**attempt)  # Exponential backoff
                await sleep(retry_delay)

        # If all retries failed, return empty list
        return []

    async def _get_checkpoint(
        self, session: AsyncSession, relay: str
    ) -> tuple[int, int] | None:
        """Get the latest checkpoint for this relay."""
        canonical_name = self._get_canonical_relay_name(relay)
        stmt = select(RelaysPayloadsCheckpointsDB).where(
            RelaysPayloadsCheckpointsDB.relay == canonical_name
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()
        if checkpoint:
            return checkpoint.from_slot, checkpoint.to_slot
        return None

    async def _update_checkpoint(
        self,
        session: AsyncSession,
        relay: str,
        from_slot: int,
        to_slot: int,
    ) -> None:
        """Update or create checkpoint for this relay."""
        canonical_name = self._get_canonical_relay_name(relay)
        stmt = select(RelaysPayloadsCheckpointsDB).where(
            RelaysPayloadsCheckpointsDB.relay == canonical_name
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()
        if checkpoint:
            checkpoint.from_slot = cast("Column[int]", from_slot)
            checkpoint.to_slot = cast("Column[int]", to_slot)
        else:
            checkpoint = RelaysPayloadsCheckpointsDB(
                relay=canonical_name,
                from_slot=from_slot,
                to_slot=to_slot,
            )
            session.add(checkpoint)
        await session.commit()

    async def _store_registrations(
        self,
        registrations: list[RelaysPayloads],
        relay: str,
    ) -> None:
        """Store validator registrations in the database."""
        if not registrations:
            return

        # Convert Pydantic models to dicts and add canonical relay name
        canonical_name = self._get_canonical_relay_name(relay)

        await upsert_models(
            db_model_class=RelaysPayloadsDB,
            pydantic_models=list(registrations),
            extra_fields={"relay": canonical_name},
        )

    async def _backfill_range(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        relay: str,
        start_slot: int,
        end_slot: int,
        task_id: TaskID,
        phase_name: str,
        from_slot: int,
        to_slot: int,
    ) -> tuple[int, int]:
        """Backfill a specific range going backwards from start_slot to end_slot.

        Args:
            client: httpx.AsyncClient
            session: AsyncSession
            relay: str
            start_slot: int
            end_slot: int
            task_id: TaskID
            phase_name: str
            from_slot: int
            to_slot: int

        Returns:
            tuple[int, int]: Updated (from_slot, to_slot) after backfilling

        Raises:
            ValueError: If progress is not initialized
        """
        current_cursor = start_slot
        total_registrations = 0
        consecutive_empty_responses = 0
        max_consecutive_empty = 2

        if self.progress is None:
            msg = "Progress is not initialized"
            raise ValueError(msg)

        while current_cursor > end_slot:
            registrations = await self._fetch_data(client, relay, current_cursor)

            if not registrations:
                consecutive_empty_responses += 1

                # If we've hit too many consecutive empty responses, stop
                if consecutive_empty_responses >= max_consecutive_empty:
                    break

                # Jump back by a larger amount to find where data might exist
                # Use a large jump to handle relays with sparse historical data
                jump_size = 50_000  # ~1.7 days worth of slots (12 sec per slot)
                current_cursor = max(current_cursor - jump_size, end_slot)
                continue

            # Reset consecutive empty counter when we get data
            consecutive_empty_responses = 0

            await self._store_registrations(registrations, relay)
            total_registrations += len(registrations)

            min_slot_in_batch = min(reg.slot for reg in registrations)
            max_slot_in_batch = max(reg.slot for reg in registrations)

            # Update checkpoint based on phase
            if phase_name == "new":
                # Phase 1: Updating to_slot as we fetch newer data
                to_slot = max(to_slot, max_slot_in_batch)
            else:  # historical
                # Phase 2: Updating from_slot as we go backwards
                from_slot = min(from_slot, min_slot_in_batch)

            # Save checkpoint after each batch
            await self._update_checkpoint(session, relay, from_slot, to_slot)

            # Update progress
            slots_processed = start_slot - min_slot_in_batch
            self.progress.update(
                task_id,
                completed=slots_processed,
                description=(
                    f"{relay[:25]:25} [{phase_name}] slot={min_slot_in_batch:,}"
                ),
            )

            current_cursor = min_slot_in_batch

            if min_slot_in_batch <= end_slot:
                break

        return from_slot, to_slot

    async def backfill(
        self,
        relay: str,
        latest_slot: int,
        target_end_slot: int = 0,
        ignore_checkpoints: bool = False,
    ) -> None:
        """Backfill builder payload delivered data.

        Two-phase backfill strategy:
        1. Phase 1: Always fetch latest_slot -> to_slot (new data since last run)
        2. Phase 2: If from_slot != target_end_slot, fetch from_slot -> target_end_slot
        (historical data)

        Args:
            relay: Relay to backfill
            latest_slot: Latest slot to backfill to
            target_end_slot: Target end slot (default: 0)
            ignore_checkpoints: If True, ignore existing checkpoints and force backfill
            (default: False)

        Raises:
            ValueError: If progress is not initialized
        """
        if self.progress is None:
            msg = "Progress is not initialized"
            raise ValueError(msg)
        async with AsyncSessionLocal() as session:
            checkpoint = (
                await self._get_checkpoint(session, relay)
                if not ignore_checkpoints
                else None
            )

            if checkpoint is None or ignore_checkpoints:
                # No checkpoint or ignoring checkpoints: force backfill for the range
                from_slot = latest_slot
                to_slot = target_end_slot
                phase1_needed = False  # Skip phase 1 when doing custom range
                phase2_needed = (
                    True  # Always backfill from latest_slot to target_end_slot
                )
            else:
                from_slot, to_slot = checkpoint
                phase1_needed = to_slot < latest_slot
                phase2_needed = from_slot > target_end_slot

            # Create progress task for this relay
            # Calculate actual remaining slots based on checkpoint and phases
            total_remaining_slots = 0
            if phase1_needed:
                total_remaining_slots += latest_slot - to_slot
            if phase2_needed:
                total_remaining_slots += from_slot - target_end_slot

            # If no work needed, set a minimal total to show completion
            if total_remaining_slots == 0:
                total_remaining_slots = 1

            task_id = self.progress.add_task(
                f"{relay[:25]:25}",
                total=total_remaining_slots,
            )
            self.tasks[relay] = task_id

            # Configure client with extended timeouts for slow relays
            timeout = httpx.Timeout(10.0, connect=3.0)
            limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)

            try:
                async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
                    # Phase 1: Fetch new data (latest_slot -> to_slot)
                    if phase1_needed:
                        from_slot, to_slot = await self._backfill_range(
                            client,
                            session,
                            relay,
                            latest_slot,
                            to_slot,
                            task_id,
                            "new",
                            from_slot,
                            to_slot,
                        )

                    # Phase 2: Continue historical backfill (from_slot -> target_end)
                    if phase2_needed:
                        from_slot, to_slot = await self._backfill_range(
                            client,
                            session,
                            relay,
                            from_slot,
                            target_end_slot,
                            task_id,
                            "historical",
                            from_slot,
                            to_slot,
                        )

                    # Mark as complete
                    self.progress.update(
                        task_id,
                        description=f"{relay[:25]:25} [green]✓[/green] done",
                        completed=total_remaining_slots,
                    )
            except Exception:
                # Mark as failed but don't raise - let other relays continue
                self.progress.update(
                    task_id,
                    description=f"{relay[:25]:25} [red]✗[/red] failed",
                )
                raise  # Re-raise to be caught by gather(return_exceptions=True)

    async def run(
        self, start_slot: int | None = None, end_slot: int | None = None
    ) -> None:
        """Run the backfill.

        Args:
            start_slot: Optional start slot (defaults to latest slot)
            end_slot: Optional end slot (defaults to 0)
        """
        if start_slot is None:
            latest_slot = await self._get_latest_slot()
            latest_slot = int(
                latest_slot - (10 * 60 / 12)
            )  # Security buffer of 10 minutes
        else:
            latest_slot = start_slot

        self.console.print(
            f"[bold blue]Running backfill for {len(RELAYS)} relays[/bold blue]"
        )
        self.console.print(f"[cyan]Latest slot: {latest_slot:,}[/cyan]")
        if end_slot is not None:
            self.console.print(f"[cyan]End slot: {end_slot:,}[/cyan]")
        self.console.print()

        # Create progress display
        self.progress = create_standard_progress(console=self.console, expand=True)

        with self.progress:
            # Create tasks for each relay
            target_end = end_slot if end_slot is not None else 0
            # Ignore checkpoints when custom slots are provided
            ignore_checkpoints = start_slot is not None
            tasks = [
                create_task(
                    self.backfill(relay, latest_slot, target_end, ignore_checkpoints)
                )
                for relay in RELAYS
            ]

            try:
                # Use return_exceptions=True so one failing relay doesn't cancel others
                results = await gather(*tasks, return_exceptions=True)

                # Check results and report any failures
                for relay, result in zip(RELAYS, results, strict=True):
                    if isinstance(result, Exception):
                        self.console.print(f"[red]✗ {relay} failed: {result}[/red]")
            except CancelledError:
                self.console.print("[yellow]Backfill cancelled[/yellow]")
            finally:
                self.console.print("\n[bold green]✓ Backfill completed[/bold green]")


if __name__ == "__main__":
    import sys

    start_slot = int(sys.argv[1]) if len(sys.argv) > 1 else None
    end_slot = int(sys.argv[2]) if len(sys.argv) > 2 else None

    backfill = BackfillRelayPayloadDelivered()
    run(backfill.run(start_slot=start_slot, end_slot=end_slot))
