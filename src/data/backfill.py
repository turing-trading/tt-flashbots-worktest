"""Backfill data from relays."""

from asyncio import CancelledError, Lock, create_task, gather, run, sleep

import httpx
from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from tqdm.asyncio import tqdm

from src.data.constants import (
    BEACON_ENDPOINT,
    ENDPOINTS,
    LIMITS,
    RELAY_LIMITS,
    RELAYS,
)
from src.data.db import (
    AsyncSessionLocal,
    Base,
    SignedValidatorRegistrationCheckpoints,
    SignedValidatorRegistrationDB,
    async_engine,
)
from src.data.models import SignedValidatorRegistration
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
        self.pbars = {}  # One progress bar per relay
        self.pbar_lock = Lock()  # Lock for thread-safe progress bar updates

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
    ) -> list[SignedValidatorRegistration]:
        """Fetch data from the relay endpoint with retry logic."""
        url = f"https://{relay}{self.endpoint}"
        limit = self._get_limit_for_relay(relay)
        params = {"cursor": str(cursor), "limit": str(limit)}

        max_retries = 5
        base_delay = 1  # Start with 1 second delay

        for attempt in range(max_retries):
            try:
                response = await client.get(url, params=params, timeout=30.0)
                response.raise_for_status()

                data = TypeAdapter(list[SignedValidatorRegistration]).validate_json(
                    response.text
                )
                return list({item.slot: item for item in data}.values())

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
                    return []

            except httpx.HTTPError as e:
                self.logger.error(f"HTTP error fetching from {relay}: {e}")
                return []
            except Exception as e:
                self.logger.error(f"Error fetching from {relay}: {e}")
                return []

        self.logger.error(f"Max retries ({max_retries}) exceeded for {relay}")
        return []

    async def _get_checkpoint(
        self, session: AsyncSession, relay: str
    ) -> tuple[int, int] | None:
        """Get the latest checkpoint for this relay."""
        stmt = select(SignedValidatorRegistrationCheckpoints).where(
            SignedValidatorRegistrationCheckpoints.relay == relay
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
        stmt = select(SignedValidatorRegistrationCheckpoints).where(
            SignedValidatorRegistrationCheckpoints.relay == relay
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()
        if checkpoint:
            checkpoint.from_slot = from_slot  # type: ignore[assignment]
            checkpoint.to_slot = to_slot  # type: ignore[assignment]
        else:
            checkpoint = SignedValidatorRegistrationCheckpoints(
                relay=relay,
                from_slot=from_slot,
                to_slot=to_slot,
            )
            session.add(checkpoint)
        await session.commit()

    async def _store_registrations(
        self,
        session: AsyncSession,
        registrations: list[SignedValidatorRegistration],
        relay: str,
    ) -> None:
        """Store validator registrations in the database."""
        if not registrations:
            return

        # Convert Pydantic models to dicts and add relay
        values = [{**reg.model_dump(), "relay": relay} for reg in registrations]

        # Upsert registrations into the database using ON CONFLICT
        stmt = pg_insert(SignedValidatorRegistrationDB).values(values)
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=["slot", "relay"],
            set_={
                SignedValidatorRegistrationDB.parent_hash: excluded.parent_hash,
                SignedValidatorRegistrationDB.block_hash: excluded.block_hash,
                SignedValidatorRegistrationDB.builder_pubkey: excluded.builder_pubkey,
                SignedValidatorRegistrationDB.proposer_pubkey: excluded.proposer_pubkey,
                SignedValidatorRegistrationDB.proposer_fee_recipient: excluded.proposer_fee_recipient,
                SignedValidatorRegistrationDB.gas_limit: excluded.gas_limit,
                SignedValidatorRegistrationDB.gas_used: excluded.gas_used,
                SignedValidatorRegistrationDB.value: excluded.value,
                SignedValidatorRegistrationDB.block_number: excluded.block_number,
                SignedValidatorRegistrationDB.num_tx: excluded.num_tx,
            },
        )
        await session.execute(stmt)
        await session.commit()

    async def create_tables(self) -> None:
        """Create tables if they don't exist."""
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def backfill(self, relay: str, latest_slot: int, pbar_position: int) -> None:
        """Backfill proposer payload delivered data.

        The API returns data in descending slot order (newest first).
        The cursor parameter acts as an upper bound (exclusive) - it returns slots < cursor.
        Without cursor, it returns the most recent slots.
        """
        async with AsyncSessionLocal() as session:
            # Get existing checkpoint if available
            checkpoint = await self._get_checkpoint(session, relay)
            if checkpoint is None:
                # First run: start from the latest slot and work backwards
                current_cursor = latest_slot
                min_slot_seen = latest_slot
            else:
                # Continue from where we left off (go further back in history)
                current_cursor = checkpoint[0]
                min_slot_seen = checkpoint[0]

            # Create progress bar for this relay
            estimated_total_slots = latest_slot - 5000000
            pbar = tqdm(
                total=estimated_total_slots,
                desc=f"{relay[:30]:30}",
                unit="slots",
                unit_scale=True,
                dynamic_ncols=True,
                position=pbar_position,
                leave=True,
                mininterval=0.5,  # Update at most every 0.5s
                maxinterval=2.0,   # Update at least every 2s
            )
            self.pbars[relay] = pbar

            # Use async httpx client for requests
            async with httpx.AsyncClient() as client:
                total_registrations = 0
                relay_limit = self._get_limit_for_relay(relay)
                last_progress = 0

                # Fetch data from relay, paginating backwards through history
                while True:
                    registrations = await self._fetch_data(
                        client, relay, current_cursor
                    )

                    if not registrations:
                        # No more data available
                        break

                    await self._store_registrations(session, registrations, relay)
                    total_registrations += len(registrations)

                    # Find the minimum slot in this batch
                    min_slot_in_batch = min(reg.slot for reg in registrations)
                    max_slot_in_batch = max(reg.slot for reg in registrations)

                    # Update checkpoint with the range we've covered
                    min_slot_seen = min(min_slot_seen, min_slot_in_batch)
                    await self._update_checkpoint(
                        session,
                        relay,
                        min_slot_seen,
                        latest_slot,
                    )

                    # Update progress bar (with lock to prevent concurrent updates)
                    slots_processed = latest_slot - min_slot_in_batch
                    progress_delta = slots_processed - last_progress
                    if progress_delta > 0:
                        async with self.pbar_lock:
                            pbar.update(progress_delta)
                            pbar.set_postfix(
                                {
                                    "slot": min_slot_in_batch,
                                    "records": total_registrations,
                                },
                                refresh=True,
                            )
                            last_progress = slots_processed

                    self.logger.debug(
                        f"Fetched {len(registrations)} registrations from {relay} "
                        f"(slots {min_slot_in_batch} to {max_slot_in_batch})"
                    )

                    # Move cursor to continue pagination backwards
                    current_cursor = min_slot_in_batch

                    # Stop if we got fewer results than limit (likely end of available data)
                    if len(registrations) < relay_limit:
                        break

                # Close progress bar for this relay
                pbar.close()

                if total_registrations > 0:
                    self.logger.debug(
                        f"Backfilled {total_registrations} total registrations from {relay}"
                    )
                else:
                    self.logger.debug(f"No new registrations from {relay}")

    async def run(self) -> None:
        """Run the backfill."""
        await self.create_tables()

        latest_slot = await self._get_latest_slot()
        latest_slot = int(latest_slot - (60 * 60 / 12))  # Security buffer of 1 hour

        tqdm.write(f"Running backfill for {len(RELAYS)} relays")
        tqdm.write(f"Latest slot: {latest_slot}")

        # Create tasks with position parameter for each relay
        tasks = [
            create_task(self.backfill(relay, latest_slot, idx))
            for idx, relay in enumerate(RELAYS)
        ]

        try:
            await gather(*tasks)
        except CancelledError:
            tqdm.write("Backfill cancelled")
        except Exception as e:
            tqdm.write(f"Error running backfill: {e}")
            raise e
        finally:
            # Close any remaining progress bars
            for pbar in self.pbars.values():
                pbar.close()
            tqdm.write("Backfill completed")


if __name__ == "__main__":
    backfill = BackfillProposerPayloadDelivered()
    run(backfill.run())
