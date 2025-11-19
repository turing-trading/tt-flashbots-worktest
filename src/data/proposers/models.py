"""Pydantic models for miner balance data."""

from pydantic import BaseModel


class ProposerBalance(BaseModel):
    """Miner balance increase for a specific block."""

    block_number: int
    miner: str
    balance_before: int  # Wei at block N-1
    balance_after: int  # Wei at block N
    balance_increase: int  # Wei increase (balance_after - balance_before)


class ExtraBuilderBalance(BaseModel):
    """Balance increase for a known builder address for a specific block."""

    block_number: int
    builder_address: str  # The builder address from KNOWN_BUILDER_ADDRESSES
    miner: str  # The proposer/miner address (key in KNOWN_BUILDER_ADDRESSES)
    balance_before: int  # Wei at block N-1
    balance_after: int  # Wei at block N
    balance_increase: int  # Wei increase (balance_after - balance_before)
