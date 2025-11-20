"""Configuration management and environment variable utilities."""

import os

from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


def get_required_env(key: str) -> str:
    """Get a required environment variable.

    Args:
        key: Environment variable name

    Returns:
        Environment variable value

    Raises:
        ValueError: If the environment variable is not set

    Example:
        ```python
        from src.helpers.config import get_required_env

        rpc_url = get_required_env("ETH_RPC_URL")
        ```
    """
    value = os.getenv(key)
    if not value:
        msg = f"{key} environment variable is not set"
        raise ValueError(msg)
    return value


def get_optional_env(key: str, default: str | None = None) -> str | None:
    """Get an optional environment variable with a default value.

    Args:
        key: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value or default

    Example:
        ```python
        from src.helpers.config import get_optional_env

        batch_size = int(get_optional_env("BATCH_SIZE", "1000"))
        ```
    """
    return os.getenv(key, default)


def get_eth_rpc_url(rpc_url: str | None = None) -> str:
    """Get Ethereum RPC URL from parameter or environment.

    Args:
        rpc_url: Optional RPC URL to use directly

    Returns:
        Ethereum RPC URL

    Raises:
        ValueError: If RPC URL is not provided and ETH_RPC_URL env var is not set

    Example:
        ```python
        from src.helpers.config import get_eth_rpc_url

        # Get from environment
        rpc_url = get_eth_rpc_url()

        # Or provide explicitly
        rpc_url = get_eth_rpc_url("https://eth.llamarpc.com")
        ```
    """
    if rpc_url:
        return rpc_url

    env_rpc_url = os.getenv("ETH_RPC_URL")
    if not env_rpc_url:
        msg = "ETH_RPC_URL must be provided or set in environment variables"
        raise ValueError(msg)

    return env_rpc_url


def get_eth_ws_url(ws_url: str | None = None) -> str:
    """Get Ethereum WebSocket URL from parameter or environment.

    Args:
        ws_url: Optional WebSocket URL to use directly

    Returns:
        Ethereum WebSocket URL

    Raises:
        ValueError: If WebSocket URL is not provided and ETH_WS_URL env var is not set

    Example:
        ```python
        from src.helpers.config import get_eth_ws_url

        ws_url = get_eth_ws_url()
        ```
    """
    if ws_url:
        return ws_url

    env_ws_url = os.getenv("ETH_WS_URL")
    if not env_ws_url:
        msg = "ETH_WS_URL must be provided or set in environment variables"
        raise ValueError(msg)

    return env_ws_url


__all__ = [
    "get_eth_rpc_url",
    "get_eth_ws_url",
    "get_optional_env",
    "get_required_env",
]
