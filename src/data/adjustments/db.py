"""Database models for Ultrasound adjustments."""

from datetime import datetime
from decimal import Decimal

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, DateTime, Numeric, String, select
from sqlalchemy.orm import Mapped, mapped_column

from src.helpers.db import Base


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class UltrasoundAdjustmentDB(Base):
    """Ultrasound relay adjustment record - SQLAlchemy model."""

    __tablename__ = "ultrasound_adjustments"

    # Primary key
    slot: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # From API response
    adjusted_block_hash: Mapped[str | None] = mapped_column(String(66), nullable=True)
    adjusted_value: Mapped[Decimal | None] = mapped_column(
        Numeric, nullable=True
    )  # Wei value as numeric
    block_number: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, index=True
    )
    builder_pubkey: Mapped[str | None] = mapped_column(
        String(98), nullable=True, index=True
    )
    delta: Mapped[Decimal | None] = mapped_column(
        Numeric, nullable=True
    )  # Adjustment delta in Wei
    submitted_block_hash: Mapped[str | None] = mapped_column(String(66), nullable=True)
    submitted_received_at: Mapped[str | None] = mapped_column(
        String(30), nullable=True
    )  # ISO timestamp
    submitted_value: Mapped[Decimal | None] = mapped_column(
        Numeric, nullable=True
    )  # Original Wei value

    # Metadata
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    has_adjustment: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    def __repr__(self) -> str:
        """Return string representation of UltrasoundAdjustment."""
        return (
            f"<UltrasoundAdjustment(slot={self.slot}, "
            f"block_number={self.block_number}, delta={self.delta})>"
        )


async def get_adjustment_by_slot(
    session: AsyncSession, slot: int
) -> UltrasoundAdjustmentDB | None:
    """Get adjustment record by slot number."""
    stmt = select(UltrasoundAdjustmentDB).where(UltrasoundAdjustmentDB.slot == slot)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def adjustment_exists(session: AsyncSession, slot: int) -> bool:
    """Check if adjustment record already exists for slot."""
    adjustment = await get_adjustment_by_slot(session, slot)
    return adjustment is not None
