"""Pytest configuration and shared fixtures for data integrity tests."""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from src.helpers.db import AsyncSessionLocal


@pytest_asyncio.fixture
async def async_session() -> AsyncSession:
    """Provide async database session for tests.

    Note: This uses the same database connection as the application.
    For production tests, consider using a separate test database.

    Yields:
        AsyncSession: Database session for testing
    """
    async with AsyncSessionLocal() as session:
        yield session


@pytest.fixture
def tolerance() -> float:
    """Provide tolerance for floating point comparisons.

    Returns:
        float: Maximum acceptable difference for float equality
    """
    return 0.0001


@pytest.fixture
def max_violations() -> int:
    """Maximum number of violations to report in test failures.

    Returns:
        int: Limit for error reporting to avoid overwhelming output
    """
    return 100
