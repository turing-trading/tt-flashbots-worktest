"""Backfill data from relays."""

from asyncio import CancelledError, create_task, gather, run, sleep

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
)
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.relays.constants import (
    BEACON_ENDPOINT,
    ENDPOINTS,
    LIMITS,
    RELAY_LIMITS,
    RELAYS,
)
from src.data.relays.db import (
    RelaysPayloadsCheckpoints,
    RelaysPayloadsDB,
)
from src.data.relays.models import RelaysPayloads
from src.helpers.db import AsyncSessionLocal, Base, async_engine
from src.helpers.logging import get_logger


class BackfillProposerPayloadDelivered:
    """Backfill proposer payload delivered data."""

    def __init__(self):
        """Initialize backfill with relay and endpoint."""
        self.endpoint = ENDPOINTS.get(
            "proposer_payload_delivered",
            "/relay/v1/data/bidtraces/proposer_payload_delivered",
        )
        self.default_limit = LIMITS.get("proposer_payload_delivered", 200)
        self.logger = get_logger("backfill_payloads", log_level="WARNING")
        self.console = Console()
        self.progress = None
        self.tasks = {}  # Track progress task IDs per relay

    def _get_limit_for_relay(self, relay: str) -> int:
        """Get the appropriate limit for a specific relay."""
        return RELAY_LIMITS.get(relay, self.default_limit)

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

            except httpx.TimeoutException as e:
                # Retry on timeout with exponential backoff
                retry_delay = base_delay * (2**attempt)
                self.logger.warning(
                    f"Timeout from {relay}, retrying in {retry_delay}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                await sleep(retry_delay)
                continue

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limit
                    retry_delay = base_delay * (2**attempt)  # Exponential backoff
                    self.logger.warning(
                        f"Rate limited by {relay}, retrying in {retry_delay}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    await sleep(retry_delay)
                    continue
                else:
                    self.logger.error(
                        f"HTTP {e.response.status_code} from {relay}: {e}"
                    )
                    raise

            except httpx.HTTPError as e:
                self.logger.error(f"HTTP error fetching from {relay}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Error fetching from {relay}: {e}")
                raise

        # Max retries exceeded
        error_msg = f"Max retries ({max_retries}) exceeded for {relay}"
        self.logger.error(error_msg)
        raise Exception(error_msg)

    async def _get_checkpoint(
        self, session: AsyncSession, relay: str
    ) -> tuple[int, int] | None:
        """Get the latest checkpoint for this relay."""
        stmt = select(RelaysPayloadsCheckpoints).where(
            RelaysPayloadsCheckpoints.relay == relay
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()
        if (
            checkpoint
            and checkpoint.from_slot is not None
            and checkpoint.to_slot is not None
        ):
            return (checkpoint.from_slot, checkpoint.to_slot)  # type: ignore[return-value]
        return None

    async def _update_checkpoint(
        self,
        session: AsyncSession,
        relay: str,
        from_slot: int,
        to_slot: int,
    ) -> None:
        """Update or create checkpoint for this relay."""
        stmt = select(RelaysPayloadsCheckpoints).where(
            RelaysPayloadsCheckpoints.relay == relay
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()
        if checkpoint:
            checkpoint.from_slot = from_slot  # type: ignore[assignment]
            checkpoint.to_slot = to_slot  # type: ignore[assignment]
        else:
            checkpoint = RelaysPayloadsCheckpoints(
                relay=relay,
                from_slot=from_slot,
                to_slot=to_slot,
            )
            session.add(checkpoint)
        await session.commit()

    async def _store_registrations(
        self,
        session: AsyncSession,
        registrations: list[RelaysPayloads],
        relay: str,
    ) -> None:
        """Store validator registrations in the database."""
        if not registrations:
            return

        # Convert Pydantic models to dicts and add relay
        values = [{**reg.model_dump(), "relay": relay} for reg in registrations]

        # Upsert registrations into the database using ON CONFLICT
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

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _backfill_range(
        self,
        client: httpx.AsyncClient,
        session: AsyncSession,
        relay: str,
        start_slot: int,
        end_slot: int,
        relay_limit: int,
        task_id: TaskID,
        phase_name: str,
        from_slot: int,
        to_slot: int,
    ) -> tuple[int, int]:
        """Backfill a specific range going backwards from start_slot to end_slot.

        Returns:
            tuple[int, int]: Updated (from_slot, to_slot) after backfilling
        """
        current_cursor = start_slot
        total_registrations = 0

        if self.progress is None:
            raise ValueError("Progress is not initialized")

        while current_cursor > end_slot:
            registrations = await self._fetch_data(client, relay, current_cursor)

            if not registrations:
                break

            await self._store_registrations(session, registrations, relay)
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
                description=f"{relay[:25]:25} [{phase_name}] slot={min_slot_in_batch:,}",
            )

            self.logger.debug(
                f"[{phase_name}] Fetched {len(registrations)} from {relay} "
                f"(slots {min_slot_in_batch} to {max_slot_in_batch})"
            )

            current_cursor = min_slot_in_batch

            if len(registrations) < relay_limit or min_slot_in_batch <= end_slot:
                break

        return from_slot, to_slot

    async def backfill(self, relay: str, latest_slot: int) -> None:
        """Backfill proposer payload delivered data.

        Two-phase backfill strategy:
        1. Phase 1: Always fetch latest_slot -> to_slot (new data since last run)
        2. Phase 2: If from_slot != 0, fetch from_slot -> 0 (historical data)
        """
        if self.progress is None:
            raise ValueError("Progress is not initialized")
        async with AsyncSessionLocal() as session:
            checkpoint = await self._get_checkpoint(session, relay)

            if checkpoint is None:
                from_slot = 0
                to_slot = latest_slot
                phase1_needed = True
                phase2_needed = False
            else:
                from_slot, to_slot = checkpoint
                phase1_needed = to_slot < latest_slot
                phase2_needed = from_slot > 0

            # Create progress task for this relay
            estimated_total_slots = latest_slot - 5000000
            task_id = self.progress.add_task(
                f"{relay[:25]:25}",
                total=estimated_total_slots,
            )
            self.tasks[relay] = task_id

            # Configure client with extended timeouts for slow relays
            timeout = httpx.Timeout(60.0, connect=10.0)
            limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
            async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
                relay_limit = self._get_limit_for_relay(relay)

                # Phase 1: Fetch new data (latest_slot -> to_slot)
                if phase1_needed:
                    from_slot, to_slot = await self._backfill_range(
                        client,
                        session,
                        relay,
                        latest_slot,
                        to_slot,
                        relay_limit,
                        task_id,
                        "new",
                        from_slot,
                        to_slot,
                    )

                # Phase 2: Continue historical backfill (from_slot -> 0)
                if phase2_needed:
                    from_slot, to_slot = await self._backfill_range(
                        client,
                        session,
                        relay,
                        from_slot,
                        0,
                        relay_limit,
                        task_id,
                        "historical",
                        from_slot,
                        to_slot,
                    )

                # Mark as complete
                self.progress.update(
                    task_id,
                    description=f"{relay[:25]:25} [green]✓[/green] done",
                    completed=estimated_total_slots,
                )

    async def run(self) -> None:
        """Run the backfill."""
        await self.create_tables()

        latest_slot = await self._get_latest_slot()
        latest_slot = int(latest_slot - (60 * 60 / 12))  # Security buffer of 1 hour

        self.console.print(
            f"[bold blue]Running backfill for {len(RELAYS)} relays[/bold blue]"
        )
        self.console.print(f"[cyan]Latest slot: {latest_slot:,}[/cyan]\n")

        # Create progress display
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=self.console,
            expand=True,
        )

        with self.progress:
            # Create tasks for each relay
            tasks = [create_task(self.backfill(relay, latest_slot)) for relay in RELAYS]

            try:
                await gather(*tasks)
            except CancelledError:
                self.console.print("[yellow]Backfill cancelled[/yellow]")
            except Exception as e:
                self.console.print(f"[red]Error running backfill: {e}[/red]")
                raise e
            finally:
                self.console.print("\n[bold green]✓ Backfill completed[/bold green]")


if __name__ == "__main__":
    backfill = BackfillProposerPayloadDelivered()
    run(backfill.run())
