"""Constants for analysis module including builder name cleanup."""

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


def clean_builder_name(builder_name: str | None) -> str:
    """Clean and normalize builder name.

    Args:
        builder_name: Raw builder name from database

    Returns:
        Cleaned/canonical builder name
    """
    if not builder_name:
        return "unknown"

    # Check for geth variants (case-insensitive)
    if "geth" in builder_name.lower():
        return "unknown"

    # Apply direct mapping
    return BUILDER_NAME_MAPPING.get(builder_name, builder_name)
