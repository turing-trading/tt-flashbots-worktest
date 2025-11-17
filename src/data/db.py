"""Database for the project."""

import os

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# Database URL - defaults to SQLite if not set
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./flashbots_data.db")

# Convert sync SQLite URL to async if needed
if DATABASE_URL.startswith("sqlite:///"):
    DATABASE_URL = DATABASE_URL.replace("sqlite:///", "sqlite+aiosqlite:///", 1)

# Create async engine
async_engine = create_async_engine(DATABASE_URL, echo=False)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


class SignedValidatorRegistrationCheckpoints(Base):
    """Signed validator registration checkpoints."""

    __tablename__ = "signed_validator_registrations_checkpoints"

    relay = Column(String, primary_key=True, index=True)
    from_slot = Column(Integer, primary_key=False)
    to_slot = Column(Integer, primary_key=False)


class SignedValidatorRegistrationDB(Base):
    """Signed validator registration database model."""

    __tablename__ = "signed_validator_registrations"

    slot = Column(Integer, primary_key=True)
    relay = Column(String, primary_key=True, index=True)
    parent_hash = Column(String, primary_key=False)
    block_hash = Column(String, primary_key=False)
    builder_pubkey = Column(String, primary_key=False)
    proposer_pubkey = Column(String, primary_key=False)
    proposer_fee_recipient = Column(String, primary_key=False)
    gas_limit = Column(Integer, primary_key=False)
    gas_used = Column(Integer, primary_key=False)
    value = Column(Integer, primary_key=False)
    block_number = Column(Integer, primary_key=False)
    num_tx = Column(Integer, primary_key=False)
