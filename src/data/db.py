"""Database for the project."""

import os

from dotenv import load_dotenv
from sqlalchemy import BigInteger, Column, Integer, Numeric, String
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

# Load environment variables from .env file
load_dotenv()

Base = declarative_base()

# Database URL
POSTGRE_HOST = os.getenv("POSTGRE_HOST")
if not POSTGRE_HOST:
    raise ValueError("POSTGRE_HOST is not set")

POSTGRE_PORT = os.getenv("POSTGRE_PORT", "5432")

POSTGRE_USER = os.getenv("POSTGRE_USER")
if not POSTGRE_USER:
    raise ValueError("POSTGRE_USER is not set")

POSTGRE_PASSWORD = os.getenv("POSTGRE_PASSWORD")
if not POSTGRE_PASSWORD:
    raise ValueError("POSTGRE_PASSWORD is not set")

POSTGRE_DB = os.getenv("POSTGRE_DB")
if not POSTGRE_DB:
    raise ValueError("POSTGRE_DB is not set")

# Use psycopg (version 3) as the async PostgreSQL driver
DATABASE_URL = (
    "postgresql+psycopg://"
    f"{POSTGRE_USER}:{POSTGRE_PASSWORD}"
    f"@{POSTGRE_HOST}:{POSTGRE_PORT}"
    f"/{POSTGRE_DB}"
)

async_engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class SignedValidatorRegistrationCheckpoints(Base):
    """Signed validator registration checkpoints."""

    __tablename__ = "signed_validator_registrations_checkpoints"

    relay = Column(String(255), primary_key=True, index=True)
    from_slot = Column(Integer, primary_key=False)
    to_slot = Column(Integer, primary_key=False)


class SignedValidatorRegistrationDB(Base):
    """Signed validator registration database model."""

    __tablename__ = "signed_validator_registrations"

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
