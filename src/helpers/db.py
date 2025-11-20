"""Database connection helpers."""

import os

from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from dotenv import load_dotenv


if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic import BaseModel


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
        msg = "POSTGRE_HOST is not set"
        raise ValueError(msg)

    postgre_port = os.getenv("POSTGRE_PORT", "5432")

    postgre_user = os.getenv("POSTGRE_USER")
    if not postgre_user:
        msg = "POSTGRE_USER is not set"
        raise ValueError(msg)

    postgre_password = os.getenv("POSTGRE_PASSWORD")
    if not postgre_password:
        msg = "POSTGRE_PASSWORD is not set"
        raise ValueError(msg)

    postgre_db = os.getenv("POSTGRE_DB")
    if not postgre_db:
        msg = "POSTGRE_DB is not set"
        raise ValueError(msg)

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
    extra_fields: dict[str, Any] | None = None,
) -> None:
    """Upsert a single model using PostgreSQL INSERT ... ON CONFLICT DO UPDATE.

    This is a convenience wrapper around upsert_models for single model operations.

    Args:
        db_model_class: The SQLAlchemy model class (e.g., BlockDB, AnalysisPBSDB)
        pydantic_model: A single Pydantic model instance with data to upsert
        extra_fields: Additional fields not in the Pydantic model (e.g., relay name)

    Examples:
        await upsert_model(
            db_model_class=BlockDB,
            pydantic_model=block,
        )

    Raises:
        ValueError: If the database model class cannot be inspected
    """
    await upsert_models(
        db_model_class=db_model_class,
        pydantic_models=[pydantic_model],
        extra_fields=extra_fields,
    )


async def upsert_models[DBModelType](
    db_model_class: type[DBModelType],
    pydantic_models: Sequence[BaseModel],
    extra_fields: dict[str, Any] | None = None,
) -> None:
    """Upsert multiple models using PostgreSQL INSERT ... ON CONFLICT DO UPDATE.

    This function uses atomic database-level upsert to avoid race conditions
    in concurrent environments.

    Args:
        db_model_class: The SQLAlchemy model class (e.g., BlockDB, AnalysisPBSDB)
        pydantic_models: List of Pydantic model instances with data to upsert
        extra_fields: Additional fields not in the Pydantic model (e.g., relay name)

    Examples:
        await upsert_models(
            db_model_class=BlockDB,
            pydantic_models=[block1, block2, block3],
        )

    Raises:
        ValueError: If the database model class cannot be inspected
    """
    async with AsyncSessionLocal() as session:
        try:
            # Prepare data for insert
            data = [model.model_dump() for model in pydantic_models]

            # Add extra fields to each item if provided
            if extra_fields:
                for item in data:
                    item.update(extra_fields)

            # Get primary key column names using SQLAlchemy inspection
            mapper = inspect(db_model_class)
            if not mapper:
                msg = f"Cannot inspect {db_model_class}"
                raise ValueError(msg)  # noqa: TRY301
            pk_columns = [col.name for col in mapper.primary_key]

            # Get all column names from the first data item
            if not data:
                return

            all_columns = set(data[0].keys())

            # Create INSERT statement with ON CONFLICT DO UPDATE
            stmt = pg_insert(db_model_class).values(data)

            # Build the update dict (all columns except primary keys)
            update_dict = {
                col: stmt.excluded[col] for col in all_columns if col not in pk_columns
            }

            # Apply ON CONFLICT DO UPDATE
            stmt = stmt.on_conflict_do_update(
                index_elements=pk_columns,
                set_=update_dict,
            )

            await session.execute(stmt)
            await session.commit()
        except Exception:
            await session.rollback()
            raise
