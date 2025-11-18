"""Database connection helpers."""

import os
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel
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


async def upsert_model[DBModelType](
    db_model_class: type[DBModelType],
    pydantic_model: BaseModel,
    primary_key_value: Any,
    extra_fields: dict[str, Any] | None = None,
) -> None:
    """Generic upsert helper to insert or update a database record.

    This function eliminates the need for manual attribute assignment and
    type: ignore comments when working with SQLAlchemy models.

    Args:
        db_model_class: The SQLAlchemy model class (e.g., BlockDB, AnalysisPBSDB)
        pydantic_model: The Pydantic model instance with data to upsert
        primary_key_value: The primary key value (or tuple for composite keys)
        extra_fields: Additional fields not in the Pydantic model (e.g., relay name)

    Examples:
        # Single primary key
        await upsert_model(
            db_model_class=AnalysisPBSDB,
            pydantic_model=analysis,
            primary_key_value=analysis.block_number,
        )

        # Composite primary key with extra fields
        await upsert_model(
            db_model_class=RelaysPayloadsDB,
            pydantic_model=payload,
            primary_key_value=(payload.slot, relay),
            extra_fields={"relay": relay},
        )
    """
    async with AsyncSessionLocal() as session:
        # Check if record already exists
        existing = await session.get(db_model_class, primary_key_value)

        if existing:
            # Update existing record with all fields from Pydantic model
            for key, value in pydantic_model.model_dump().items():
                setattr(existing, key, value)
            # Also update extra fields if provided
            if extra_fields:
                for key, value in extra_fields.items():
                    setattr(existing, key, value)
        else:
            # Insert new record using all fields from Pydantic model plus extra fields
            data = pydantic_model.model_dump()
            if extra_fields:
                data.update(extra_fields)
            session.add(db_model_class(**data))

        await session.commit()
