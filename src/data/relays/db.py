"""Relay database models."""

from decimal import Decimal

from sqlalchemy import BigInteger, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from src.helpers.db import Base


class RelaysPayloadsCheckpointsDB(Base):
    """Relay payloads checkpoints."""

    __tablename__ = "relays_payloads_checkpoints"

    relay: Mapped[str] = mapped_column(String(255), primary_key=True, index=True)
    from_slot: Mapped[int] = mapped_column(Integer, primary_key=False)
    to_slot: Mapped[int] = mapped_column(Integer, primary_key=False)


class RelaysPayloadsDB(Base):
    """Signed validator registration database model."""

    __tablename__ = "relays_payloads"

    slot: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    relay: Mapped[str] = mapped_column(String(255), primary_key=True, index=True)
    parent_hash: Mapped[str] = mapped_column(String(66), primary_key=False)
    block_hash: Mapped[str] = mapped_column(String(66), primary_key=False)
    builder_pubkey: Mapped[str] = mapped_column(String(98), primary_key=False)
    proposer_pubkey: Mapped[str] = mapped_column(String(98), primary_key=False)
    proposer_fee_recipient: Mapped[str] = mapped_column(String(42), primary_key=False)
    gas_limit: Mapped[Decimal] = mapped_column(Numeric, primary_key=False)
    gas_used: Mapped[Decimal] = mapped_column(Numeric, primary_key=False)
    value: Mapped[Decimal] = mapped_column(Numeric, primary_key=False)
    block_number: Mapped[int] = mapped_column(BigInteger, primary_key=False)
    num_tx: Mapped[int] = mapped_column(Integer, primary_key=False)
