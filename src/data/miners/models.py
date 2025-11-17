"""Pydantic models for miner balance data."""

from pydantic import BaseModel


class MinerBalance(BaseModel):
    """Miner balance increase for a specific block."""

    block_number: int
    miner: str
    balance_before: int  # Wei at block N-1
    balance_after: int  # Wei at block N
    balance_increase: int  # Wei increase (balance_after - balance_before)
