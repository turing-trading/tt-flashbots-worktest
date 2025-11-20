"""HTTP client utilities and helpers."""

from asyncio import sleep
from contextlib import asynccontextmanager
from functools import wraps

from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import httpx

from src.helpers.constants import (
    DEFAULT_TIMEOUT,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
    RETRY_MAX_DELAY,
)
from src.helpers.logging import get_logger


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


logger = get_logger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


def retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    base_delay: float = RETRY_BASE_DELAY,
    max_delay: float = RETRY_MAX_DELAY,
    *,
    log_errors: bool = True,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorator to retry async functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        base_delay: Initial delay in seconds (default: 1.0)
        max_delay: Maximum delay between retries (default: 60.0)
        log_errors: Whether to log retry attempts (default: True)

    Returns:
        Decorated function that retries on httpx.HTTPError and general exceptions

    Example:
        ```python
        from src.helpers.http import retry_with_backoff

        @retry_with_backoff(max_retries=3, base_delay=2.0)
        async def fetch_data(client: httpx.AsyncClient, url: str) -> dict:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

        # Will retry up to 3 times with delays of 2s, 4s, 8s
        ```
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Exception | None = None

            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except httpx.TimeoutException as e:
                    last_exception = e
                    if log_errors and attempt < max_retries - 1:
                        logger.warning(
                            "%s timeout (attempt %d/%d)",
                            func.__name__,
                            attempt + 1,
                            max_retries,
                        )
                except httpx.HTTPError as e:
                    last_exception = e
                    if log_errors and attempt < max_retries - 1:
                        logger.warning(
                            "%s HTTP error (attempt %d/%d): %s",
                            func.__name__,
                            attempt + 1,
                            max_retries,
                            e,
                        )
                except Exception as e:
                    last_exception = e
                    if log_errors and attempt < max_retries - 1:
                        logger.warning(
                            "%s error (attempt %d/%d): %s",
                            func.__name__,
                            attempt + 1,
                            max_retries,
                            e,
                        )

                # Don't sleep after the last attempt
                if attempt < max_retries - 1:
                    # Exponential backoff with max_delay cap
                    delay = min(base_delay * (2**attempt), max_delay)
                    await sleep(delay)

            # All retries exhausted, raise the last exception
            if last_exception:
                if log_errors:
                    logger.error(
                        "%s failed after %d attempts", func.__name__, max_retries
                    )
                raise last_exception

            # This should never happen, but satisfy type checker
            msg = f"{func.__name__} failed without exception"
            raise RuntimeError(msg)

        return wrapper

    return decorator


def create_http_client(
    timeout: float = DEFAULT_TIMEOUT, **kwargs: Any
) -> httpx.AsyncClient:
    """Create a configured httpx AsyncClient.

    Args:
        timeout: Default timeout in seconds (default: DEFAULT_TIMEOUT)
        **kwargs: Additional httpx.AsyncClient kwargs

    Returns:
        Configured AsyncClient instance

    Example:
        ```python
        from src.helpers.http import create_http_client

        async with create_http_client(timeout=60.0) as client:
            response = await client.get("https://example.com")
        ```
    """
    return httpx.AsyncClient(timeout=timeout, **kwargs)


def handle_http_errors(
    default_return: T | None = None,
    *,
    log_errors: bool = True,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T | None]]]:
    """Decorator to handle HTTP errors gracefully.

    Args:
        default_return: Value to return on error (default: None)
        log_errors: Whether to log errors (default: True)

    Returns:
        Decorated function that catches httpx.HTTPError and returns default_return

    Example:
        ```python
        from src.helpers.http import handle_http_errors

        @handle_http_errors(default_return=[])
        async def fetch_data(client: httpx.AsyncClient, url: str) -> list[dict]:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

        # If request fails, returns [] instead of raising
        ```
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T | None]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | None:
            try:
                return await func(*args, **kwargs)
            except httpx.HTTPStatusError as e:
                if log_errors:
                    if e.response.status_code == 404:
                        logger.debug("%s returned 404", func.__name__)
                    else:
                        logger.warning(
                            "%s HTTP error: %s %s",
                            func.__name__,
                            e.response.status_code,
                            e.response.text[:100] if e.response.text else "",
                        )
                return default_return
            except httpx.HTTPError as e:
                if log_errors:
                    logger.warning("%s HTTP error: %s", func.__name__, e)
                return default_return
            except Exception:
                if log_errors:
                    logger.exception("%s unexpected error", func.__name__)
                return default_return

        return wrapper

    return decorator


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    timeout: float | None = None,
    raise_for_status: bool = True,
) -> dict[str, Any] | list[Any] | None:
    """Fetch JSON data from a URL.

    Args:
        client: HTTP client instance
        url: URL to fetch
        timeout: Optional timeout override
        raise_for_status: Whether to raise on HTTP errors

    Returns:
        Parsed JSON data or None on error

    Example:
        ```python
        async with httpx.AsyncClient() as client:
            data = await fetch_json(client, "https://api.example.com/data")
            if data:
                print(data)
        ```
    """
    try:
        response = await client.get(url, timeout=timeout)
        if raise_for_status:
            response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.debug("URL not found: %s", url)
        else:
            logger.warning("HTTP error fetching %s: %s", url, e)
        return None
    except httpx.HTTPError as e:
        logger.warning("HTTP error fetching %s: %s", url, e)
        return None
    except Exception:
        logger.exception("Error fetching %s", url)
        return None


async def post_json(
    client: httpx.AsyncClient,
    url: str,
    data: dict[str, Any],
    *,
    timeout: float | None = None,
    raise_for_status: bool = True,
) -> dict[str, Any] | list[Any] | None:
    """Post JSON data to a URL and return JSON response.

    Args:
        client: HTTP client instance
        url: URL to post to
        data: JSON data to post
        timeout: Optional timeout override
        raise_for_status: Whether to raise on HTTP errors

    Returns:
        Parsed JSON response or None on error

    Example:
        ```python
        async with httpx.AsyncClient() as client:
            response = await post_json(
                client,
                "https://api.example.com/submit",
                {"key": "value"}
            )
        ```
    """
    try:
        response = await client.post(url, json=data, timeout=timeout)
        if raise_for_status:
            response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        logger.warning("HTTP error posting to %s: %s", url, e)
        return None
    except Exception:
        logger.exception("Error posting to %s", url)
        return None


@asynccontextmanager
async def log_and_suppress_errors(
    operation_name: str,
    *,
    log_level: str = "warning",
    suppress: bool = True,
) -> AsyncIterator[None]:
    """Context manager to log and optionally suppress errors.

    Args:
        operation_name: Description of the operation for logging
        log_level: Logging level ("debug", "info", "warning", "error")
        suppress: If True, suppress exceptions; if False, re-raise after logging

    Yields:
        None

    Example:
        ```python
        from src.helpers.http import log_and_suppress_errors

        # Suppress and log errors
        async with log_and_suppress_errors("fetch user data"):
            user = await fetch_user(user_id)
            # If error occurs, it's logged and suppressed

        # Log but don't suppress errors
        async with log_and_suppress_errors("critical operation", suppress=False):
            await critical_operation()
            # If error occurs, it's logged and then raised
        ```
    """
    try:
        yield
    except Exception as e:
        log_method = getattr(logger, log_level, logger.warning)
        log_method("%s failed: %s", operation_name, e)

        if not suppress:
            raise


__all__ = [
    "create_http_client",
    "fetch_json",
    "handle_http_errors",
    "log_and_suppress_errors",
    "post_json",
    "retry_with_backoff",
]
