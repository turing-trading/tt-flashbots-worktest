"""Tests for HTTP helpers including retry logic and error handling."""

from typing import TYPE_CHECKING

import httpx
import pytest

from src.helpers.http import (
    log_and_suppress_errors,
    retry_with_backoff,
)


if TYPE_CHECKING:
    from unittest.mock import AsyncMock


class TestRetryWithBackoff:
    """Tests for retry_with_backoff decorator."""

    @pytest.mark.asyncio
    async def test_succeeds_on_first_try(self) -> None:
        """Test function succeeds without retries."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def success_func() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = await success_func()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retries_on_http_error(self) -> None:
        """Test function retries on HTTP errors."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def failing_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.HTTPError("Network error")
            return "success"

        result = await failing_func()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retries_on_timeout(self) -> None:
        """Test function retries on timeout errors."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def timeout_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.TimeoutException("Timeout")
            return "success"

        result = await timeout_func()
        assert result == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self) -> None:
        """Test function raises after exhausting retries."""

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def always_fails() -> None:
            raise httpx.HTTPError("Persistent error")

        with pytest.raises(httpx.HTTPError, match="Persistent error"):
            await always_fails()

    @pytest.mark.asyncio
    async def test_exponential_backoff(self) -> None:
        """Test exponential backoff delay calculation."""
        import time

        call_times: list[float] = []

        @retry_with_backoff(max_retries=4, base_delay=0.1, max_delay=1.0, log_errors=False)
        async def failing_func() -> None:
            call_times.append(time.time())
            raise httpx.HTTPError("Error")

        with pytest.raises(httpx.HTTPError):
            await failing_func()

        # Verify delays increase exponentially
        assert len(call_times) == 4
        # First retry delay ~0.1s
        if len(call_times) >= 2:
            delay1 = call_times[1] - call_times[0]
            assert 0.08 < delay1 < 0.15
        # Second retry delay ~0.2s
        if len(call_times) >= 3:
            delay2 = call_times[2] - call_times[1]
            assert 0.18 < delay2 < 0.25

    @pytest.mark.asyncio
    async def test_max_delay_cap(self) -> None:
        """Test that delay is capped at max_delay."""
        import time

        call_times: list[float] = []

        @retry_with_backoff(max_retries=10, base_delay=1.0, max_delay=0.2, log_errors=False)
        async def failing_func() -> None:
            call_times.append(time.time())
            raise httpx.HTTPError("Error")

        with pytest.raises(httpx.HTTPError):
            await failing_func()

        # All delays should be capped at max_delay (0.2s)
        for i in range(1, len(call_times)):
            delay = call_times[i] - call_times[i - 1]
            assert delay <= 0.25  # Add small buffer for timing variability

    @pytest.mark.asyncio
    async def test_preserves_function_signature(self) -> None:
        """Test decorator preserves function name and docstring."""

        @retry_with_backoff(log_errors=False)
        async def documented_func() -> str:
            """This is a documented function."""
            return "result"

        assert documented_func.__name__ == "documented_func"
        assert documented_func.__doc__ == "This is a documented function."

    @pytest.mark.asyncio
    async def test_passes_arguments_correctly(self) -> None:
        """Test decorator passes args and kwargs correctly."""

        @retry_with_backoff(max_retries=2, base_delay=0.01, log_errors=False)
        async def func_with_args(a: int, b: str, *, c: float = 1.0) -> str:
            return f"{a}-{b}-{c}"

        result = await func_with_args(42, "test", c=3.14)
        assert result == "42-test-3.14"


class TestLogAndSuppressErrors:
    """Tests for log_and_suppress_errors context manager."""

    @pytest.mark.asyncio
    async def test_no_error_passes_through(self) -> None:
        """Test context manager works when no error occurs."""
        result = None
        async with log_and_suppress_errors("test operation"):
            result = "success"
        assert result == "success"

    @pytest.mark.asyncio
    async def test_suppresses_error_by_default(self) -> None:
        """Test context manager suppresses errors by default."""
        executed = False
        async with log_and_suppress_errors("test operation"):
            executed = True
            raise ValueError("Test error")

        # Error was suppressed, execution continued
        assert executed

    @pytest.mark.asyncio
    async def test_reraises_when_suppress_false(self) -> None:
        """Test context manager re-raises when suppress=False."""
        with pytest.raises(ValueError, match="Test error"):
            async with log_and_suppress_errors("test operation", suppress=False):
                raise ValueError("Test error")

    @pytest.mark.asyncio
    async def test_logs_different_levels(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test context manager logs at different levels."""
        import logging

        caplog.set_level(logging.DEBUG)

        # Test warning level (default)
        async with log_and_suppress_errors("warning op"):
            raise ValueError("Warning error")

        # Test error level
        async with log_and_suppress_errors("error op", log_level="error"):
            raise ValueError("Error error")

        # Test info level
        async with log_and_suppress_errors("info op", log_level="info"):
            raise ValueError("Info error")

        # Verify logs
        assert any("warning op" in record.message for record in caplog.records)
        assert any("error op" in record.message for record in caplog.records)
        assert any("info op" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_exception_details(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test context manager logs exception details."""
        import logging

        caplog.set_level(logging.WARNING)

        async with log_and_suppress_errors("database operation"):
            raise ConnectionError("Database connection failed")

        # Verify error details in log
        assert any(
            "database operation" in record.message and "failed" in record.message
            for record in caplog.records
        )


class TestHttpxIntegration:
    """Integration tests for HTTP operations."""

    @pytest.mark.asyncio
    async def test_retry_with_real_httpx_timeout(self) -> None:
        """Test retry decorator with real httpx timeout."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def fetch_with_timeout() -> str:
            nonlocal call_count
            call_count += 1

            # Simulate timeout on first 2 calls
            if call_count < 3:
                raise httpx.TimeoutException("Request timed out")

            return "success"

        result = await fetch_with_timeout()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_with_http_status_errors(self) -> None:
        """Test retry decorator with HTTP status errors."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def fetch_with_errors() -> str:
            nonlocal call_count
            call_count += 1

            # Simulate 500 error on first call
            if call_count == 1:
                raise httpx.HTTPStatusError(
                    "Server error",
                    request=httpx.Request("GET", "https://example.com"),
                    response=httpx.Response(500),
                )

            return "success"

        result = await fetch_with_errors()
        assert result == "success"
        assert call_count == 2
