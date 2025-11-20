"""Relay database models."""

from sqlalchemy import BigInteger, Column, Integer, Numeric, String

from src.helpers.db import Base


class RelaysPayloadsCheckpointsDB(Base):
    """Relay payloads checkpoints."""

    __tablename__ = "relays_payloads_checkpoints"

    relay = Column(String(255), primary_key=True, index=True)
    from_slot = Column(Integer, primary_key=False)
    to_slot = Column(Integer, primary_key=False)


class RelaysPayloadsDB(Base):
    """Signed validator registration database model."""

    __tablename__ = "relays_payloads"

    slot = Column(BigInteger, primary_key=True)
    relay = Column(String(255), primary_key=True, index=True)
    parent_hash = Column(String(66), primary_key=False)
    block_hash = Column(String(66), primary_key=False)
    builder_pubkey = Column(String(98), primary_key=False)
    proposer_pubkey = Column(String(98), primary_key=False)
    proposer_fee_recipient = Column(String(42), primary_key=False)
    gas_limit = Column(Numeric, primary_key=False)
    gas_used = Column(Numeric, primary_key=False)
    value = Column(Numeric, primary_key=False)
    block_number = Column(BigInteger, primary_key=False)
    num_tx = Column(Integer, primary_key=False)
