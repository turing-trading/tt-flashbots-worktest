"""Constants for analysis module including builder name cleanup."""

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
    "By @builder0x69": "Bob The Builder",
    "By builder0x69": "Bob The Builder",
    # bloXroute -> bloXroute (standardize capitalization)
    # Generic/unknown builders
    "builder": "unknown",
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
