"""Pytest configuration and shared fixtures for data integrity tests."""

import os
from pathlib import Path

import pytest
import pytest_asyncio

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from src.helpers.db import AsyncSessionLocal, Base


if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from pytest_docker.plugin import Services


# Apply timeout to integration tests with extra time for Docker setup
def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Add timeout marker to integration tests."""
    for item in items:
        if "integration" in item.keywords:
            # Integration tests need more time for Docker setup (pulling images, etc.)
            item.add_marker(pytest.mark.timeout(120))


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig: pytest.Config) -> str:
    """Provide the path to docker-compose.yml for pytest-docker."""
    from typing import cast

    # pytestconfig.rootdir exists at runtime but not in type stubs
    # Use getattr to avoid pyright error about unknown attribute
    rootdir = cast("Path", getattr(pytestconfig, "rootdir"))  # noqa: B009
    return str(rootdir / "tests" / "docker-compose.test.yml")


@pytest.fixture(scope="session")
def timescaledb_service(docker_services: Services) -> None:
    """Ensure TimescaleDB service is up and responsive.

    Args:
        docker_services: pytest-docker services fixture
    """

    def is_responsive() -> bool:
        """Check if TimescaleDB is responsive."""
        import psycopg

        try:
            conn = psycopg.connect(
                host="localhost",
                port=docker_services.port_for("timescaledb", 5432),
                dbname="test_flashbots",
                user="test_user",
                password="test_password",
            )
            conn.close()
            return True
        except Exception:
            return False

    # Wait up to 30 seconds for TimescaleDB to become responsive
    docker_services.wait_until_responsive(
        timeout=30.0,
        pause=0.5,
        check=is_responsive,
    )


@pytest.fixture(scope="session")
def timescaledb_url(docker_services: Services, timescaledb_service: None) -> str:
    """Get TimescaleDB connection URL.

    Args:
        docker_services: pytest-docker services fixture
        timescaledb_service: Ensures TimescaleDB is ready

    Returns:
        Database connection URL
    """
    port = docker_services.port_for("timescaledb", 5432)
    return (
        f"postgresql+psycopg://test_user:test_password@localhost:{port}/test_flashbots"
    )


@pytest_asyncio.fixture
async def test_db_engine(timescaledb_url: str) -> AsyncGenerator[AsyncSession]:
    """Create test database engine with all tables.

    Args:
        timescaledb_url: TimescaleDB connection URL from docker

    Yields:
        AsyncSession: Test database session
    """
    # Create async engine for test database
    engine = create_async_engine(timescaledb_url, echo=False)

    # Cleanup first - drop all tables if they exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create session
    from sqlalchemy.ext.asyncio import async_sessionmaker

    async_session_maker = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with async_session_maker() as session:
        yield session

    # Cleanup - drop all tables after test
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    # Cleanup
    await engine.dispose()


@pytest_asyncio.fixture
async def async_session() -> AsyncGenerator[AsyncSession]:
    """Provide async database session for integration tests.

    Note: This uses the same database connection as the application.
    Only use this for integration tests against real database.

    Yields:
        AsyncSession: Database session for testing
    """
    # Check if we should skip integration tests
    if os.getenv("SKIP_INTEGRATION_TESTS") == "1":
        pytest.skip("Integration tests disabled")

    try:
        async with AsyncSessionLocal() as session:
            # Quick connectivity check
            await session.execute(text("SELECT 1"))
            yield session
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


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
    return 10


@pytest.fixture
def sample_size() -> int:
    """Sample size for quick integration tests.

    Returns:
        int: Number of records to sample for fast testing
    """
    return 100
