"""Backfill data from relays."""

import logging
from asyncio import create_task, gather
from typing import Any

import httpx
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.constants import ENDPOINTS, LIMITS, RELAYS
from src.data.db import (
    AsyncSessionLocal,
    Base,
    SignedValidatorRegistrationCheckpoints,
    SignedValidatorRegistrationDB,
    async_engine,
)
from src.data.models import SignedValidatorRegistration

logger = logging.getLogger(__name__)


class BackfillProposerPayloadDelivered:
    """Backfill proposer payload delivered data."""

    def __init__(self):
        """Initialize backfill with relay and endpoint."""
        self.endpoint = ENDPOINTS.get(
            "proposer_payload_delivered",
            "/relay/v1/data/bidtraces/proposer_payload_delivered",
        )
        self.limit = LIMITS.get("proposer_payload_delivered", 200)

    async def _fetch_data(
        self,
        client: httpx.AsyncClient,
        relay: str,
        slot_from: int | None = None,
        slot_to: int | None = None,
    ) -> list[SignedValidatorRegistration]:
        """Fetch data from the relay endpoint."""
        url = f"https://{relay}{self.endpoint}"
        params: dict[str, Any] = {"limit": self.limit}

        if slot_from is not None:
            params["slot"] = slot_from
        if slot_to is not None:
            params["slot"] = slot_to

        try:
            response = await client.get(url, params=params, timeout=30.0)
            response.raise_for_status()
            data: list[SignedValidatorRegistration] = [
                SignedValidatorRegistration(**item) for item in response.json()
            ]
            return data
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching from {relay}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching from {relay}: {e}")
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
    ) -> None:
        """Store validator registrations in the database."""
        # Upsert registrations into the database
        stmt = insert(SignedValidatorRegistrationDB).values(registrations)
        await session.execute(stmt)
        await session.commit()

    async def backfill(
        self,
        relay: str,
        slot_from: int | None = None,
        slot_to: int | None = None,
    ) -> None:
        """Backfill proposer payload delivered data."""
        # Create tables if they don't exist
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with AsyncSessionLocal() as session:
            # Get existing checkpoint if available
            checkpoint = await self._get_checkpoint(session, relay)
            if checkpoint and slot_from is None:
                slot_from, slot_to = checkpoint

            # Use async httpx client for requests
            async with httpx.AsyncClient() as client:
                # Fetch data from relay
                registrations = await self._fetch_data(
                    client, relay, slot_from, slot_to
                )

                if not registrations:
                    logger.warning(f"No data fetched from {relay}")
                    return

                # Store registrations
                await self._store_registrations(session, registrations)

                # Update checkpoint
                if slot_from is not None and slot_to is not None:
                    await self._update_checkpoint(session, relay, slot_from, slot_to)

                logger.info(
                    f"Backfilled {len(registrations)} registrations from {relay} "
                    f"(slots {slot_from} to {slot_to})"
                )

    async def run(self) -> None:
        """Run the backfill."""
        tasks = [create_task(self.backfill(relay)) for relay in RELAYS]
        await gather(*tasks)
