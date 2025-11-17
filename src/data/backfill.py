"""Backfill data from relays."""

from asyncio import CancelledError, create_task, gather, run

import httpx
from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.constants import BEACON_ENDPOINT, ENDPOINTS, LIMITS, RELAYS
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
        self.limit = LIMITS.get("proposer_payload_delivered", 200)
        self.logger = get_logger("backfill_payloads")

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
        """Fetch data from the relay endpoint."""
        url = f"https://{relay}{self.endpoint}"
        params = {"cursor": str(cursor), "limit": str(self.limit)}

        try:
            response = await client.get(url, params=params, timeout=30.0)
            response.raise_for_status()

            return TypeAdapter(list[SignedValidatorRegistration]).validate_json(
                response.text
            )
        except httpx.HTTPError as e:
            self.logger.error(f"HTTP error fetching from {relay}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching from {relay}: {e}")
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

    async def backfill(self, relay: str, latest_slot: int) -> None:
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

            # Use async httpx client for requests
            async with httpx.AsyncClient() as client:
                total_registrations = 0

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

                    self.logger.info(
                        f"Fetched {len(registrations)} registrations from {relay} "
                        f"(slots {min_slot_in_batch} to {max_slot_in_batch})"
                    )

                    # Move cursor to continue pagination backwards
                    current_cursor = min_slot_in_batch

                    # Stop if we got fewer results than limit (likely end of available data)
                    if len(registrations) < self.limit:
                        break

                if total_registrations > 0:
                    self.logger.info(
                        f"Backfilled {total_registrations} total registrations from {relay}"
                    )
                else:
                    self.logger.info(f"No new registrations from {relay}")

    async def run(self) -> None:
        """Run the backfill."""
        await self.create_tables()

        latest_slot = await self._get_latest_slot()
        latest_slot = int(latest_slot - (60 * 60 / 12))  # Security buffer of 1 hour

        self.logger.info(f"Running backfill for {len(RELAYS)} relays")
        self.logger.info(f"Latest slot: {latest_slot}")
        self.logger.info(f"Limit: {self.limit}")
        tasks = [create_task(self.backfill(relay, latest_slot)) for relay in RELAYS]

        try:
            await gather(*tasks)
        except CancelledError:
            self.logger.info("Backfill cancelled")
        except Exception as e:
            self.logger.error(f"Error running backfill: {e}")
            raise e
        finally:
            self.logger.info("Backfill completed")


if __name__ == "__main__":
    backfill = BackfillProposerPayloadDelivered()
    run(backfill.run())
