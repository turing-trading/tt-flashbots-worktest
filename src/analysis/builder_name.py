"""Builder name parsing and normalization utilities.

This module provides functions for:
- Parsing builder names from block extra_data
- Cleaning and normalizing builder names
- Mapping builder name variations to canonical names
"""

import binascii
import re


# Builder name remapping for consistent naming
# Maps various builder name variations to canonical names
BUILDER_NAME_MAPPING = {
    # Titan variants -> Titan
    "titanbuilder.xyz": "Titan",
    "Titan (titanbuilder.xyz)": "Titan",
    # BuilderNet variants
    "Flashbots": "BuilderNet (Flashbots)",
    "Beaver": "BuilderNet (Beaver)",
    "Nethermind": "BuilderNet (Nethermind)",
    "BuilderNet (Beaver)": "BuilderNet (Beaver)",  # Keep as-is
    # Quasar variants -> Quasar
    "quasar.win": "Quasar",
    # Beaver variants -> BuilderNet (Beaver)
    "beaverbuild.org": "BuilderNet (Beaver)",
    # Rsync variants -> Rsync
    "rsyncbuilder": "Rsync",
    # Bob The Builder variants
    "bobTheBuilder.xyz": "Bob The Builder",
    # 0x69 builder variants
    "by @builder": "0x69",
    "by builder": "0x69",
    # IO Builder variants
    "iobuilder.xyz": "IO Builder",
    # Eureka variants
    "eurekabuilder.xyz": "Eureka",
    # Bitget variants
    "www.bitget.com": "Bitget",
    # Turbo variants
    "rpc.turbobuilder.xyz": "Turbo",
    # BTCS variants
    "Builder+ btcs.com | ethgas.com": "BTCS",
    # DexPeer variants
    "DexPeer Builder": "DexPeer",
    # BuildAI variants
    "buildai.net": "BuildAI",
    # Snail variants
    "snailbuilder.sh": "Snail",
    # Besu variants (development versions)
    "besu-develop-": "besu",
    "besu-develop-e": "besu",
    # bloXroute -> bloXroute (standardize capitalization)
    # Generic/unknown builders
    "builder": "unknown",
    "MevRefund -": "unknown",  # Troll message, not a real builder
    "": "unknown",
}


def clean_builder_name(
    builder_name: str | None, apply_advanced_cleaning: bool = False
) -> str:
    """Clean and normalize builder name.

    This function combines simple mapping-based cleaning with optional
    advanced regex-based cleaning for more sophisticated normalization.

    Args:
        builder_name: Raw builder name from database
        apply_advanced_cleaning: If True, applies advanced regex-based cleaning
                                 before mapping lookup

    Returns:
        Cleaned/canonical builder name
    """
    if not builder_name:
        return "unknown"

    # Check for geth variants (case-insensitive)
    if "geth" in builder_name.lower():
        return "unknown"

    # Check for BTCS (case-insensitive)
    if "btcs" in builder_name.lower():
        return "BTCS"

    # Apply advanced cleaning if requested (used by backfill processes)
    if apply_advanced_cleaning:
        builder_name = _advanced_clean_builder_name(builder_name)

    # Apply direct mapping
    return BUILDER_NAME_MAPPING.get(builder_name, builder_name)


def _advanced_clean_builder_name(name: str) -> str:
    """Advanced cleaning with emoji removal and domain extraction.

    This is used primarily by backfill processes that need to extract
    clean builder names from raw extra_data fields.

    Args:
        name: Raw builder name string

    Returns:
        Cleaned builder name
    """
    # Remove emojis and other non-ASCII characters (keep only printable ASCII)
    cleaned = "".join(c for c in name if ord(c) < 128 and c.isprintable())

    # Strip whitespace
    cleaned = cleaned.strip()

    # Handle comma-separated phrases - take first part before comma
    if "," in cleaned:
        cleaned = cleaned.split(",")[0].strip()

    # First, try to extract content from parentheses (e.g., "Quasar (quasar.win)" -> "quasar.win")
    paren_match = re.search(r"\(([^)]+)\)", cleaned)
    if paren_match:
        cleaned = paren_match.group(1)

    # Extract domain/pool names from slash-separated patterns like "EU2/pool.binance.com/"
    if "/" in cleaned:
        parts = [p for p in cleaned.split("/") if p]
        if parts:
            cleaned = parts[-1]

    # For domain-like strings (containing dots), extract just the domain part
    if "." in cleaned:
        tld_pattern = r"([a-zA-Z0-9]+(?:[._-][a-zA-Z0-9]+)*\.(?:com|net|org|io|win|xyz|eth|pool|info|co|uk|de|fr|cn|jp))"
        domain_match = re.match(tld_pattern, cleaned)
        if domain_match:
            cleaned = domain_match.group(1)

    # Remove version patterns like "v1.34", "v1.35.0", etc.
    cleaned = re.sub(r"\s+v?\d+\.\d+(?:\.\d+)*\.?", "", cleaned, flags=re.IGNORECASE)

    # Remove trailing and leading special characters
    cleaned = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9.]+$", "", cleaned)

    # Remove trailing numbers and mixed alphanumeric suffixes
    cleaned = re.sub(r"[0-9]+[a-z0-9]*$", "", cleaned)

    # Final cleanup
    cleaned = cleaned.strip()

    # If the result is too short, return "unknown"
    if len(cleaned) <= 1:
        return "unknown"

    return cleaned


def parse_builder_name_from_extra_data(extra_data: str | None) -> str:
    """Parse builder name from block extra_data hex string.

    Decodes hex-encoded extra_data, extracts UTF-8 text, and applies
    advanced cleaning to normalize builder names.

    Args:
        extra_data: Hex string of extra_data from block (with or without '0x' prefix)

    Returns:
        Parsed and cleaned builder name, or 'unknown' if unparseable

    Example:
        >>> parse_builder_name_from_extra_data("0x6265617665726275696c642e6f7267")
        'BuilderNet (Beaver)'
        >>> parse_builder_name_from_extra_data(None)
        'unknown'
    """
    if not extra_data:
        return "unknown"

    # Remove '0x' prefix if present
    hex_str = extra_data.removeprefix("0x")

    try:
        # Convert hex to bytes
        bytes_data = binascii.unhexlify(hex_str)
        # Decode as UTF-8, strip null bytes
        builder_name = bytes_data.decode("utf-8", errors="ignore").strip("\x00")

        # Replace any remaining null bytes (PostgreSQL doesn't support them)
        builder_name = builder_name.replace("\x00", "")

        # Clean up the builder name using advanced cleaning
        builder_name = clean_builder_name(builder_name, apply_advanced_cleaning=True)

        # Return cleaned string or 'unknown' if empty
        return builder_name or "unknown"
    except Exception:
        # If parsing fails, return 'unknown'
        return "unknown"
