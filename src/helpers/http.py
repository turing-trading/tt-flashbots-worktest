"""HTTP client utilities and helpers."""

from functools import wraps

from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import httpx

from src.helpers.logging import get_logger


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


logger = get_logger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


def create_http_client(timeout: float = 30.0, **kwargs: Any) -> httpx.AsyncClient:
    """Create a configured httpx AsyncClient.

    Args:
        timeout: Default timeout in seconds
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


__all__ = [
    "create_http_client",
    "fetch_json",
    "handle_http_errors",
    "post_json",
]
