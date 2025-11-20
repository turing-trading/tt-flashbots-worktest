"""Parsing utilities for common data transformations."""

from datetime import UTC, datetime

from typing import Any


def parse_hex_block_number(header: dict[str, Any]) -> int:
    """Parse block number from hex string in block header.

    Args:
        header: Block header dictionary containing a "number" field

    Returns:
        int: Block number as integer

    Example:
        >>> header = {"number": "0x1234"}
        >>> parse_hex_block_number(header)
        4660
    """
    return int(header.get("number", "0x0"), 16)


def parse_hex_timestamp(hex_timestamp: str) -> datetime:
    """Parse Unix timestamp from hex string to datetime.

    Args:
        hex_timestamp: Hex-encoded Unix timestamp string

    Returns:
        datetime: Datetime object from the Unix timestamp

    Example:
        >>> parse_hex_timestamp("0x63a1b2c3")
        datetime.datetime(2022, 12, 20, ...)
    """
    return datetime.fromtimestamp(int(hex_timestamp, 16), tz=UTC)


def parse_hex_int(hex_value: str | None, default: int = 0) -> int:
    """Parse hex string to integer.

    Args:
        hex_value: Hex-encoded string or None
        default: Default value if hex_value is None

    Returns:
        int: Parsed integer value

    Example:
        >>> parse_hex_int("0xff")
        255
        >>> parse_hex_int(None, 0)
        0
    """
    if hex_value is None:
        return default
    return int(hex_value, 16)


def wei_to_eth(wei: int | None) -> float | None:
    """Convert Wei to ETH (divide by 1e18).

    Args:
        wei: Amount in Wei, or None

    Returns:
        float | None: Amount in ETH, or None if input was None

    Example:
        >>> wei_to_eth(1000000000000000000)
        1.0
        >>> wei_to_eth(None)
        None
    """
    return float(wei) / 1e18 if wei is not None else None


def eth_to_wei(eth: float | None) -> int | None:
    """Convert ETH to Wei (multiply by 1e18).

    Args:
        eth: Amount in ETH, or None

    Returns:
        int | None: Amount in Wei, or None if input was None

    Example:
        >>> eth_to_wei(1.0)
        1000000000000000000
        >>> eth_to_wei(None)
        None
    """
    return int(eth * 1e18) if eth is not None else None
