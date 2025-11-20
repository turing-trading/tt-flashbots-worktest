"""Tests for backfill functionality."""

import pytest
from rich.console import Console

from src.helpers.backfill import BackfillBase


class ConcreteBackfill(BackfillBase):
    """Concrete implementation of BackfillBase for testing."""

    async def run(self, *args: object, **kwargs: object) -> None:
        """Simple run implementation for testing."""
        return None


class TestBackfillBase:
    """Tests for BackfillBase abstract class."""

    def test_initialization(self) -> None:
        """Test that BackfillBase initializes correctly."""
        backfill = ConcreteBackfill(batch_size=100)

        assert backfill.batch_size == 100
        assert isinstance(backfill.console, Console)

    def test_custom_batch_size(self) -> None:
        """Test initialization with custom batch size."""
        backfill = ConcreteBackfill(batch_size=500)

        assert backfill.batch_size == 500

    def test_large_batch_size(self) -> None:
        """Test initialization with large batch size."""
        backfill = ConcreteBackfill(batch_size=10_000)

        assert backfill.batch_size == 10_000

    def test_console_exists(self) -> None:
        """Test that console is initialized."""
        backfill = ConcreteBackfill(batch_size=100)

        assert backfill.console is not None
        assert hasattr(backfill.console, "print")

    @pytest.mark.asyncio
    async def test_create_tables_is_callable(self) -> None:
        """Test that create_tables method exists and is callable."""
        backfill = ConcreteBackfill(batch_size=100)

        # create_tables should be callable but may fail due to DB not existing
        # Just verify the method exists
        assert hasattr(backfill, "create_tables")
        assert callable(backfill.create_tables)

    @pytest.mark.asyncio
    async def test_run_is_implemented(self) -> None:
        """Test that run method is implemented in concrete class."""
        backfill = ConcreteBackfill(batch_size=100)

        result = await backfill.run()
        assert result is None

    def test_cannot_instantiate_abstract_class(self) -> None:
        """Test that BackfillBase cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BackfillBase(batch_size=100)  # type: ignore[abstract]


class TestBackfillConstants:
    """Tests for backfill-related constants."""

    def test_batch_size_constants_exist(self) -> None:
        """Test that batch size constants are defined."""
        from src.helpers.constants import DEFAULT_BATCH_SIZE, LARGE_BATCH_SIZE

        assert isinstance(DEFAULT_BATCH_SIZE, int)
        assert isinstance(LARGE_BATCH_SIZE, int)
        assert DEFAULT_BATCH_SIZE > 0
        assert LARGE_BATCH_SIZE > DEFAULT_BATCH_SIZE

    def test_parallel_batch_constants_exist(self) -> None:
        """Test that parallel batch constants are defined."""
        from src.helpers.constants import (
            DEFAULT_PARALLEL_BATCHES,
            HIGH_PARALLEL_BATCHES,
        )

        assert isinstance(DEFAULT_PARALLEL_BATCHES, int)
        assert isinstance(HIGH_PARALLEL_BATCHES, int)
        assert DEFAULT_PARALLEL_BATCHES > 0
        assert HIGH_PARALLEL_BATCHES > DEFAULT_PARALLEL_BATCHES

    def test_timeout_constants_exist(self) -> None:
        """Test that timeout constants are defined."""
        from src.helpers.constants import (
            CONNECTION_TIMEOUT,
            DEFAULT_TIMEOUT,
            EXTENDED_TIMEOUT,
        )

        assert isinstance(DEFAULT_TIMEOUT, float)
        assert isinstance(EXTENDED_TIMEOUT, float)
        assert isinstance(CONNECTION_TIMEOUT, float)
        assert DEFAULT_TIMEOUT > 0
        assert EXTENDED_TIMEOUT > DEFAULT_TIMEOUT
        assert CONNECTION_TIMEOUT > 0

    def test_retry_constants_exist(self) -> None:
        """Test that retry constants are defined."""
        from src.helpers.constants import (
            MAX_RETRIES,
            RETRY_BASE_DELAY,
            RETRY_MAX_DELAY,
        )

        assert isinstance(MAX_RETRIES, int)
        assert isinstance(RETRY_BASE_DELAY, float)
        assert isinstance(RETRY_MAX_DELAY, float)
        assert MAX_RETRIES > 0
        assert RETRY_BASE_DELAY > 0
        assert RETRY_MAX_DELAY > RETRY_BASE_DELAY

    def test_connection_constants_exist(self) -> None:
        """Test that connection constants are defined."""
        from src.helpers.constants import (
            MAX_CONNECTIONS,
            MAX_KEEPALIVE_CONNECTIONS,
        )

        assert isinstance(MAX_CONNECTIONS, int)
        assert isinstance(MAX_KEEPALIVE_CONNECTIONS, int)
        assert MAX_CONNECTIONS > 0
        assert MAX_KEEPALIVE_CONNECTIONS > 0
        assert MAX_CONNECTIONS >= MAX_KEEPALIVE_CONNECTIONS

    def test_slot_jump_size_exists(self) -> None:
        """Test that slot jump size constant is defined."""
        from src.helpers.constants import SLOT_JUMP_SIZE

        assert isinstance(SLOT_JUMP_SIZE, int)
        assert SLOT_JUMP_SIZE > 0


class TestRelayConstants:
    """Tests for relay-related constants."""

    def test_relay_list_exists(self) -> None:
        """Test that relay list is defined."""
        from src.data.relays.constants import RELAYS

        assert isinstance(RELAYS, list)
        assert len(RELAYS) > 0
        assert all(isinstance(relay, str) for relay in RELAYS)

    def test_relay_endpoints_exist(self) -> None:
        """Test that relay endpoints are defined."""
        from src.data.relays.constants import ENDPOINTS

        assert isinstance(ENDPOINTS, dict)
        assert len(ENDPOINTS) > 0
        assert all(isinstance(k, str) for k in ENDPOINTS.keys())
        assert all(isinstance(v, str) for v in ENDPOINTS.values())

    def test_relay_limits_exist(self) -> None:
        """Test that relay limits are defined."""
        from src.data.relays.constants import LIMITS

        assert isinstance(LIMITS, dict)
        assert len(LIMITS) > 0
        assert all(isinstance(k, str) for k in LIMITS.keys())
        assert all(isinstance(v, int) for v in LIMITS.values())

    def test_relay_name_mapping_exists(self) -> None:
        """Test that relay name mapping is defined."""
        from src.data.relays.constants import RELAY_NAME_MAPPING

        assert isinstance(RELAY_NAME_MAPPING, dict)
        assert all(isinstance(k, str) for k in RELAY_NAME_MAPPING.keys())
        assert all(isinstance(v, str) for v in RELAY_NAME_MAPPING.values())

    def test_beacon_endpoint_exists(self) -> None:
        """Test that beacon endpoint is defined."""
        from src.data.relays.constants import BEACON_ENDPOINT

        assert isinstance(BEACON_ENDPOINT, str)
        assert len(BEACON_ENDPOINT) > 0
        assert BEACON_ENDPOINT.startswith("http")
