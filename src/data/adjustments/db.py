"""Database models for Ultrasound adjustments."""

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, Numeric, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.helpers.db import Base


class UltrasoundAdjustmentDB(Base):
    """Ultrasound relay adjustment record - SQLAlchemy model."""

    __tablename__ = "ultrasound_adjustments"

    # Primary key
    slot = Column(BigInteger, primary_key=True)

    # From API response
    adjusted_block_hash = Column(String(66), nullable=True)
    adjusted_value = Column(Numeric, nullable=True)  # Wei value as numeric
    block_number = Column(BigInteger, nullable=True, index=True)
    builder_pubkey = Column(String(98), nullable=True, index=True)
    delta = Column(Numeric, nullable=True)  # Adjustment delta in Wei
    submitted_block_hash = Column(String(66), nullable=True)
    submitted_received_at = Column(String(30), nullable=True)  # ISO timestamp
    submitted_value = Column(Numeric, nullable=True)  # Original Wei value

    # Metadata
    fetched_at = Column(DateTime, nullable=False)
    has_adjustment = Column(Boolean, nullable=False, default=True)

    def __repr__(self) -> str:
        return f"<UltrasoundAdjustment(slot={self.slot}, block_number={self.block_number}, delta={self.delta})>"


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
