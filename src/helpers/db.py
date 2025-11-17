"""Database connection helpers."""

import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

# Load environment variables from .env file
load_dotenv()

Base = declarative_base()


def get_database_url() -> str:
    """Get the database URL from environment variables.

    Returns:
        str: PostgreSQL database URL

    Raises:
        ValueError: If required environment variables are not set
    """
    postgre_host = os.getenv("POSTGRE_HOST")
    if not postgre_host:
        raise ValueError("POSTGRE_HOST is not set")

    postgre_port = os.getenv("POSTGRE_PORT", "5432")

    postgre_user = os.getenv("POSTGRE_USER")
    if not postgre_user:
        raise ValueError("POSTGRE_USER is not set")

    postgre_password = os.getenv("POSTGRE_PASSWORD")
    if not postgre_password:
        raise ValueError("POSTGRE_PASSWORD is not set")

    postgre_db = os.getenv("POSTGRE_DB")
    if not postgre_db:
        raise ValueError("POSTGRE_DB is not set")

    # Use psycopg (version 3) as the async PostgreSQL driver
    return (
        "postgresql+psycopg://"
        f"{postgre_user}:{postgre_password}"
        f"@{postgre_host}:{postgre_port}"
        f"/{postgre_db}"
    )


# Create async engine and session factory
DATABASE_URL = get_database_url()
async_engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
