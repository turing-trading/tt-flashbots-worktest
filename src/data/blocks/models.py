"""Pydantic models for Ethereum blocks."""

# Pydantic needs this at runtime to validate the datetime field
from datetime import datetime

from pydantic import BaseModel


class Block(BaseModel):
    """Ethereum block model."""

    number: int
    hash: str
    parent_hash: str
    nonce: str
    sha3_uncles: str
    transactions_root: str
    state_root: str
    receipts_root: str
    miner: str
    size: int
    extra_data: str
    gas_limit: int
    gas_used: int
    timestamp: datetime
    transaction_count: int
    base_fee_per_gas: float | None = None
