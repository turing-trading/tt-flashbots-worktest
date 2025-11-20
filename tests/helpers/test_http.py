"""Tests for HTTP helpers including retry logic and error handling."""

from typing import TYPE_CHECKING

import httpx
import pytest

from src.helpers.http import (
    create_http_client,
    fetch_json,
    handle_http_errors,
    log_and_suppress_errors,
    post_json,
    retry_with_backoff,
)


if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock
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


class TestFetchJson:
    """Tests for fetch_json function using pytest-httpx."""

    @pytest.mark.asyncio
    async def test_fetch_json_success(self, httpx_mock: "HTTPXMock") -> None:
        """Test successful JSON fetch."""
        httpx_mock.add_response(
            url="https://api.example.com/data",
            json={"key": "value", "count": 42},
        )

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/data")

        assert result == {"key": "value", "count": 42}

    @pytest.mark.asyncio
    async def test_fetch_json_list_response(self, httpx_mock: "HTTPXMock") -> None:
        """Test JSON fetch with list response."""
        httpx_mock.add_response(
            url="https://api.example.com/items",
            json=[{"id": 1}, {"id": 2}, {"id": 3}],
        )

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/items")

        assert result == [{"id": 1}, {"id": 2}, {"id": 3}]

    @pytest.mark.asyncio
    async def test_fetch_json_404_returns_none(self, httpx_mock: "HTTPXMock") -> None:
        """Test 404 response returns None."""
        httpx_mock.add_response(
            url="https://api.example.com/notfound",
            status_code=404,
        )

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/notfound")

        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_json_500_returns_none(self, httpx_mock: "HTTPXMock") -> None:
        """Test 500 error returns None."""
        httpx_mock.add_response(
            url="https://api.example.com/error",
            status_code=500,
            text="Internal Server Error",
        )

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/error")

        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_json_timeout_returns_none(self, httpx_mock: "HTTPXMock") -> None:
        """Test timeout returns None."""
        httpx_mock.add_exception(httpx.TimeoutException("Request timed out"))

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/slow")

        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_json_with_custom_timeout(self, httpx_mock: "HTTPXMock") -> None:
        """Test fetch with custom timeout parameter."""
        httpx_mock.add_response(
            url="https://api.example.com/data",
            json={"status": "ok"},
        )

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/data", timeout=30.0)

        assert result == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_fetch_json_no_raise_on_error(self, httpx_mock: "HTTPXMock") -> None:
        """Test fetch with raise_for_status=False."""
        httpx_mock.add_response(
            url="https://api.example.com/error",
            status_code=400,
            json={"error": "bad request"},
        )

        async with httpx.AsyncClient() as client:
            result = await fetch_json(
                client,
                "https://api.example.com/error",
                raise_for_status=False,
            )

        assert result == {"error": "bad request"}


class TestPostJson:
    """Tests for post_json function using pytest-httpx."""

    @pytest.mark.asyncio
    async def test_post_json_success(self, httpx_mock: "HTTPXMock") -> None:
        """Test successful JSON POST."""
        httpx_mock.add_response(
            url="https://api.example.com/submit",
            method="POST",
            json={"id": 123, "status": "created"},
        )

        async with httpx.AsyncClient() as client:
            result = await post_json(
                client,
                "https://api.example.com/submit",
                {"name": "test", "value": 42},
            )

        assert result == {"id": 123, "status": "created"}

        # Verify the request payload
        request = httpx_mock.get_request()
        assert request.method == "POST"
        # JSON can be serialized with or without spaces
        import json
        assert json.loads(request.read()) == {"name": "test", "value": 42}

    @pytest.mark.asyncio
    async def test_post_json_error_returns_none(self, httpx_mock: "HTTPXMock") -> None:
        """Test POST error returns None."""
        httpx_mock.add_response(
            url="https://api.example.com/submit",
            method="POST",
            status_code=500,
        )

        async with httpx.AsyncClient() as client:
            result = await post_json(
                client,
                "https://api.example.com/submit",
                {"data": "test"},
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_post_json_with_custom_timeout(self, httpx_mock: "HTTPXMock") -> None:
        """Test POST with custom timeout."""
        httpx_mock.add_response(
            url="https://api.example.com/submit",
            method="POST",
            json={"success": True},
        )

        async with httpx.AsyncClient() as client:
            result = await post_json(
                client,
                "https://api.example.com/submit",
                {"data": "value"},
                timeout=60.0,
            )

        assert result == {"success": True}

    @pytest.mark.asyncio
    async def test_post_json_network_error_returns_none(
        self, httpx_mock: "HTTPXMock"
    ) -> None:
        """Test network error returns None."""
        httpx_mock.add_exception(httpx.ConnectError("Connection failed"))

        async with httpx.AsyncClient() as client:
            result = await post_json(
                client,
                "https://api.example.com/submit",
                {"data": "test"},
            )

        assert result is None


class TestCreateHttpClient:
    """Tests for create_http_client function."""

    def test_create_client_with_default_timeout(self) -> None:
        """Test creating client with default timeout."""
        client = create_http_client()
        assert isinstance(client, httpx.AsyncClient)
        assert client.timeout.connect is not None

    def test_create_client_with_custom_timeout(self) -> None:
        """Test creating client with custom timeout."""
        client = create_http_client(timeout=60.0)
        assert isinstance(client, httpx.AsyncClient)

    def test_create_client_with_custom_headers(self) -> None:
        """Test creating client with custom headers."""
        headers = {"Authorization": "Bearer token123"}
        client = create_http_client(headers=headers)
        assert "Authorization" in client.headers


class TestHandleHttpErrors:
    """Tests for handle_http_errors decorator using pytest-httpx."""

    @pytest.mark.asyncio
    async def test_handle_errors_returns_default_on_404(
        self, httpx_mock: "HTTPXMock"
    ) -> None:
        """Test decorator returns default value on 404."""
        httpx_mock.add_response(
            url="https://api.example.com/missing",
            status_code=404,
        )

        @handle_http_errors(default_return=[], log_errors=False)
        async def fetch_items(client: httpx.AsyncClient) -> list[dict]:
            response = await client.get("https://api.example.com/missing")
            response.raise_for_status()
            return response.json()

        async with httpx.AsyncClient() as client:
            result = await fetch_items(client)

        assert result == []

    @pytest.mark.asyncio
    async def test_handle_errors_returns_default_on_500(
        self, httpx_mock: "HTTPXMock"
    ) -> None:
        """Test decorator returns default value on server error."""
        httpx_mock.add_response(
            url="https://api.example.com/error",
            status_code=500,
            text="Internal Server Error",
        )

        @handle_http_errors(default_return={"error": True}, log_errors=False)
        async def fetch_data(client: httpx.AsyncClient) -> dict:
            response = await client.get("https://api.example.com/error")
            response.raise_for_status()
            return response.json()

        async with httpx.AsyncClient() as client:
            result = await fetch_data(client)

        assert result == {"error": True}

    @pytest.mark.asyncio
    async def test_handle_errors_success_returns_value(
        self, httpx_mock: "HTTPXMock"
    ) -> None:
        """Test decorator returns actual value on success."""
        httpx_mock.add_response(
            url="https://api.example.com/data",
            json={"status": "ok"},
        )

        @handle_http_errors(default_return=None, log_errors=False)
        async def fetch_status(client: httpx.AsyncClient) -> dict:
            response = await client.get("https://api.example.com/data")
            response.raise_for_status()
            return response.json()

        async with httpx.AsyncClient() as client:
            result = await fetch_status(client)

        assert result == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_handle_errors_on_timeout(self, httpx_mock: "HTTPXMock") -> None:
        """Test decorator handles timeout exceptions."""
        httpx_mock.add_exception(httpx.TimeoutException("Request timed out"))

        @handle_http_errors(default_return="timeout", log_errors=False)
        async def fetch_slow(client: httpx.AsyncClient) -> str:
            response = await client.get("https://api.example.com/slow")
            return response.text

        async with httpx.AsyncClient() as client:
            result = await fetch_slow(client)

        assert result == "timeout"


class TestHttpIntegrationWithDecorators:
    """Integration tests combining decorators with HTTP calls."""

    @pytest.mark.asyncio
    async def test_retry_with_fetch_json(self, httpx_mock: "HTTPXMock") -> None:
        """Test retry decorator with fetch_json."""
        # fetch_json returns None on error (doesn't raise), so retry won't be triggered
        httpx_mock.add_response(
            url="https://api.example.com/data",
            status_code=500,
        )

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def fetch_with_retry(client: httpx.AsyncClient) -> dict | None:
            return await fetch_json(client, "https://api.example.com/data")

        async with httpx.AsyncClient() as client:
            result = await fetch_with_retry(client)

        # fetch_json returns None on error without raising, so no retry happens
        assert result is None

    @pytest.mark.asyncio
    async def test_combined_decorators(self, httpx_mock: "HTTPXMock") -> None:
        """Test combining retry and error handling decorators."""
        call_count = 0

        @handle_http_errors(default_return={"default": True}, log_errors=False)
        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=False)
        async def fetch_with_both(client: httpx.AsyncClient, url: str) -> dict:
            nonlocal call_count
            call_count += 1
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

        # Mock 2 failures then success
        httpx_mock.add_response(status_code=500)
        httpx_mock.add_response(status_code=500)
        httpx_mock.add_response(json={"status": "ok"})

        async with httpx.AsyncClient() as client:
            result = await fetch_with_both(client, "https://api.example.com/data")

        assert result == {"status": "ok"}
        assert call_count == 3


class TestRetryWithBackoffLogging:
    """Tests for retry_with_backoff decorator with logging enabled."""

    @pytest.mark.asyncio
    async def test_logs_timeout_warnings(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that timeout errors are logged when log_errors=True."""
        import logging

        caplog.set_level(logging.WARNING)
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=True)
        async def timeout_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.TimeoutException("Timeout")
            return "success"

        result = await timeout_func()
        assert result == "success"

        # Verify logging occurred
        assert any("timeout" in record.message.lower() for record in caplog.records)
        assert any("attempt 1/3" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_http_error_warnings(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that HTTP errors are logged when log_errors=True."""
        import logging

        caplog.set_level(logging.WARNING)
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=True)
        async def failing_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.HTTPError("Network error")
            return "success"

        result = await failing_func()
        assert result == "success"

        # Verify logging occurred
        assert any("HTTP error" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_general_exception_warnings(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that general exceptions are logged when log_errors=True."""
        import logging

        caplog.set_level(logging.WARNING)
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, log_errors=True)
        async def exception_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Some error")
            return "success"

        result = await exception_func()
        assert result == "success"

        # Verify logging occurred
        assert any("error" in record.message.lower() for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_final_failure(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that final failure is logged when all retries exhausted."""
        import logging

        caplog.set_level(logging.ERROR)

        @retry_with_backoff(max_retries=2, base_delay=0.01, log_errors=True)
        async def always_fails() -> None:
            raise httpx.HTTPError("Persistent error")

        with pytest.raises(httpx.HTTPError):
            await always_fails()

        # Verify final error logging occurred
        assert any("failed after" in record.message for record in caplog.records)


class TestHandleHttpErrorsLogging:
    """Tests for handle_http_errors decorator logging paths."""

    @pytest.mark.asyncio
    async def test_logs_404_as_debug(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that 404 errors are logged at debug level."""
        import logging

        caplog.set_level(logging.DEBUG)

        httpx_mock.add_response(
            url="https://api.example.com/missing",
            status_code=404,
        )

        @handle_http_errors(default_return=None, log_errors=True)
        async def fetch_missing(client: httpx.AsyncClient) -> dict:
            response = await client.get("https://api.example.com/missing")
            response.raise_for_status()
            return response.json()

        async with httpx.AsyncClient() as client:
            result = await fetch_missing(client)

        assert result is None
        # 404s should be logged at debug level
        assert any("404" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_non_404_status_errors(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that non-404 HTTP status errors are logged with details."""
        import logging

        caplog.set_level(logging.WARNING)

        httpx_mock.add_response(
            url="https://api.example.com/error",
            status_code=500,
            text="Server Error Details",
        )

        @handle_http_errors(default_return=None, log_errors=True)
        async def fetch_error(client: httpx.AsyncClient) -> dict:
            response = await client.get("https://api.example.com/error")
            response.raise_for_status()
            return response.json()

        async with httpx.AsyncClient() as client:
            result = await fetch_error(client)

        assert result is None
        # Non-404 errors should be logged with status code
        assert any("500" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_general_http_errors(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that general HTTP errors are logged."""
        import logging

        caplog.set_level(logging.WARNING)

        httpx_mock.add_exception(httpx.ConnectError("Connection failed"))

        @handle_http_errors(default_return=None, log_errors=True)
        async def fetch_connect_error(client: httpx.AsyncClient) -> dict:
            response = await client.get("https://api.example.com/data")
            return response.json()

        async with httpx.AsyncClient() as client:
            result = await fetch_connect_error(client)

        assert result is None
        assert any("HTTP error" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_unexpected_exceptions(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that unexpected exceptions are logged."""
        import logging

        caplog.set_level(logging.ERROR)

        @handle_http_errors(default_return=None, log_errors=True)
        async def raises_unexpected() -> dict:
            raise RuntimeError("Unexpected error")

        result = await raises_unexpected()

        assert result is None
        assert any("unexpected error" in record.message.lower() for record in caplog.records)


class TestFetchJsonErrorPaths:
    """Tests for fetch_json error handling paths."""

    @pytest.mark.asyncio
    async def test_fetch_json_logs_404(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test fetch_json logs 404 at debug level."""
        import logging

        caplog.set_level(logging.DEBUG)

        httpx_mock.add_response(status_code=404)

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/missing")

        assert result is None
        assert any("not found" in record.message.lower() for record in caplog.records)

    @pytest.mark.asyncio
    async def test_fetch_json_logs_non_404_errors(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test fetch_json logs non-404 HTTP errors."""
        import logging

        caplog.set_level(logging.WARNING)

        httpx_mock.add_response(status_code=500)

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/error")

        assert result is None
        assert any("HTTP error" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_fetch_json_logs_connection_errors(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test fetch_json logs connection errors."""
        import logging

        caplog.set_level(logging.WARNING)

        httpx_mock.add_exception(httpx.ConnectError("Connection failed"))

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/data")

        assert result is None
        assert any("HTTP error" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_fetch_json_logs_unexpected_exceptions(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test fetch_json logs unexpected exceptions."""
        import logging

        caplog.set_level(logging.ERROR)

        # Mock JSON parsing error
        httpx_mock.add_response(text="not valid json")

        async with httpx.AsyncClient() as client:
            result = await fetch_json(client, "https://api.example.com/bad")

        assert result is None
        assert any("Error" in record.message for record in caplog.records)


class TestPostJsonErrorPaths:
    """Tests for post_json error handling paths."""

    @pytest.mark.asyncio
    async def test_post_json_logs_http_errors(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test post_json logs HTTP errors."""
        import logging

        caplog.set_level(logging.WARNING)

        httpx_mock.add_response(method="POST", status_code=500)

        async with httpx.AsyncClient() as client:
            result = await post_json(client, "https://api.example.com/submit", {})

        assert result is None
        assert any("HTTP error" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_post_json_logs_unexpected_exceptions(
        self, httpx_mock: "HTTPXMock", caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test post_json logs unexpected exceptions."""
        import logging

        caplog.set_level(logging.ERROR)

        # Mock invalid JSON response that will cause an exception during parsing
        httpx_mock.add_response(
            method="POST",
            text="not valid json",
        )

        async with httpx.AsyncClient() as client:
            result = await post_json(client, "https://api.example.com/submit", {})

        assert result is None
        assert any("Error" in record.message for record in caplog.records)


class TestRetryWithBackoffEdgeCases:
    """Tests for retry_with_backoff edge cases."""

    @pytest.mark.asyncio
    async def test_retry_with_zero_retries_raises_runtime_error(self) -> None:
        """Test retry decorator with max_retries=0 raises RuntimeError."""

        @retry_with_backoff(max_retries=0, log_errors=False)
        async def some_func() -> str:
            return "success"

        # With 0 retries, the loop never executes, triggering the RuntimeError
        with pytest.raises(RuntimeError, match="failed without exception"):
            await some_func()
