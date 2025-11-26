"""Color definitions for MEV-Boost dashboard entities.

Provides consistent color mappings for relays, builders, and other entities
across all dashboard panels.
"""

from typing import Any


# Relay color mappings
RELAY_COLORS: dict[str, str] = {
    "Ultrasound": "#37872D",  # Green
    "Bloxroute Max Profit": "#FF6B6B",  # Red
    "Bloxroute Regulated": "#FFA500",  # Orange
    "Agnostic": "#8B4513",  # Brown
    "Titan": "#4CAF50",  # Light Green
    "Flashbots": "#2196F3",  # Blue
    "Aestus": "#9C27B0",  # Purple
    "EthGas": "#FF9800",  # Amber
    "Secure RPC": "#00BCD4",  # Cyan
    "BTCS": "#795548",  # Brown-Grey
    "Wenmerge": "#E91E63",  # Pink
}

# Builder color mappings
BUILDER_COLORS: dict[str, str] = {
    "BuilderNet (Beaver)": "#FF6B6B",  # Red
    "Titan": "#4CAF50",  # Light Green (same as relay)
    "rsync-builder.xyz": "#2196F3",  # Blue
    "BuilderNet (Flashbots)": "#3F51B5",  # Indigo
    "Rsync": "#00BCD4",  # Cyan
    "BuilderNet (Nethermind)": "#009688",  # Teal
    "Quasar": "#9C27B0",  # Purple
    "Illuminate Dmocratize Dstribute": "#E91E63",  # Pink
    "Others": "#9E9E9E",  # Grey
    "jetbldr.xyz": "#FF9800",  # Orange
    "BTCS": "#795548",  # Brown (same as relay)
    "penguinbuild.org": "#607D8B",  # Blue Grey
    "gmbit.co": "#FFC107",  # Amber
    "rpc.tbuilder.xyz": "#8BC34A",  # Light Green
    "f1b.io": "#FFEB3B",  # Yellow
    "lokibuilder.xyz": "#CDDC39",  # Lime
    "Bob The Builder": "#FFD700",  # Gold
    "BuildAI": "#00E5FF",  # Light Cyan
    "blockbeelder.com": "#FF4081",  # Pink Accent
    "Illuminate Dmocrtz Dstrib Prtct": "#E040FB",  # Purple Accent
    "from f": "#536DFE",  # Indigo Accent
    "boba-builder.com": "#69F0AE",  # Green Accent
    "bloXroute": "#FF5252",  # Red Accent
    "iobuilder": "#FFD740",  # Amber Accent
    "Snail": "#B39DDB",  # Light Purple
    "Turbo": "#64B5F6",  # Light Blue
}

# Proposer color mappings (staking entities)
PROPOSER_COLORS: dict[str, str] = {
    "Lido": "#00A3FF",  # Lido Blue/Cyan
    "Coinbase": "#0052FF",  # Coinbase Blue
    "Binance": "#F3BA2F",  # Binance Yellow
    "Kraken": "#5741D9",  # Kraken Purple
    "Ether.fi": "#00D395",  # Teal
    "OKX": "#000000",  # Black
    "Everstake": "#26A69A",  # Teal-Green
    "Bitcoin Suisse": "#E53935",  # Bitcoin Suisse Red
    "Rocketpool": "#FF9A5C",  # Rocketpool Orange
    "Stakefish": "#1E88E5",  # Blue
    "Kiln": "#8E24AA",  # Purple
    "Figment": "#00897B",  # Teal
    "Staked.us": "#43A047",  # Green
    "P2P.org": "#5C6BC0",  # Indigo
    "Mantle": "#000000",  # Black
    "Renzo": "#7B1FA2",  # Purple
    "Swell": "#0097A7",  # Cyan
    "Frax Finance": "#000000",  # Black
    "Kelp DAO": "#2E7D32",  # Green
    "Liquid Collective": "#1565C0",  # Blue
    "Stader": "#EF5350",  # Red
    "Puffer": "#AB47BC",  # Purple
    "Abyss": "#37474F",  # Blue Grey
    "Unknown": "#9E9E9E",  # Grey
    "Others": "#757575",  # Dark Grey
}

# Special entity colors
SPECIAL_COLORS: dict[str, str] = {
    "vanilla": "#FF0000",  # Red
    "mev-boost": "#00FF00",  # Green
    "Proposer Profit": "#FFA500",  # Orange
    "Builder Profit": "#4CAF50",  # Green
    "Relay Fee": "#FF0000",  # Red
}


def get_relay_color_overrides() -> list[dict[str, Any]]:
    """Generate color overrides for all relays.

    Returns:
        List of Grafana field override objects for relay colors
    """
    return [
        {
            "matcher": {"id": "byName", "options": relay_name},
            "properties": [
                {"id": "color", "value": {"fixedColor": color, "mode": "fixed"}}
            ],
        }
        for relay_name, color in RELAY_COLORS.items()
    ]


def get_builder_color_overrides() -> list[dict[str, Any]]:
    """Generate color overrides for all builders.

    Returns:
        List of Grafana field override objects for builder colors
    """
    return [
        {
            "matcher": {"id": "byName", "options": builder_name},
            "properties": [
                {"id": "color", "value": {"fixedColor": color, "mode": "fixed"}}
            ],
        }
        for builder_name, color in BUILDER_COLORS.items()
    ]


def get_proposer_color_overrides() -> list[dict[str, Any]]:
    """Generate color overrides for all proposer entities.

    Returns:
        List of Grafana field override objects for proposer colors
    """
    return [
        {
            "matcher": {"id": "byName", "options": proposer_name},
            "properties": [
                {"id": "color", "value": {"fixedColor": color, "mode": "fixed"}}
            ],
        }
        for proposer_name, color in PROPOSER_COLORS.items()
    ]


def get_special_color_overrides(
    entities: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Generate color overrides for special entities.

    Args:
        entities: List of entity names to include. If None, includes all.

    Returns:
        List of Grafana field override objects for special entity colors
    """
    colors_to_use = (
        {k: v for k, v in SPECIAL_COLORS.items() if k in entities}
        if entities
        else SPECIAL_COLORS
    )

    return [
        {
            "matcher": {"id": "byName", "options": entity_name},
            "properties": [
                {"id": "color", "value": {"fixedColor": color, "mode": "fixed"}}
            ],
        }
        for entity_name, color in colors_to_use.items()
    ]


def get_combined_overrides(
    *override_lists: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Combine multiple override lists into a single list.

    Args:
        *override_lists: Variable number of override lists to combine

    Returns:
        Combined list of overrides
    """
    result: list[dict[str, Any]] = []
    for override_list in override_lists:
        result.extend(override_list)
    return result


def get_builder_color_overrides_with_hidden(
    visible_builders: list[str],
) -> list[dict[str, Any]]:
    """Generate color overrides for builders with some hidden by default.

    Args:
        visible_builders: List of builder names to show by default.
                         All others will be hidden from the visualization
                         but remain clickable in the legend.

    Returns:
        List of Grafana field override objects with colors and visibility
    """
    overrides: list[dict[str, Any]] = []

    for builder_name, color in BUILDER_COLORS.items():
        if builder_name in visible_builders:
            # Visible builder: just add color
            overrides.append({
                "matcher": {"id": "byName", "options": builder_name},
                "properties": [
                    {"id": "color", "value": {"fixedColor": color, "mode": "fixed"}}
                ],
            })
        else:
            # Hidden builder: add color and hide from viz only (still in legend)
            overrides.append({
                "matcher": {"id": "byName", "options": builder_name},
                "properties": [
                    {
                        "id": "color",
                        "value": {"fixedColor": color, "mode": "fixed"},
                    },
                    {
                        "id": "custom.hideFrom",
                        "value": {"tooltip": False, "viz": True, "legend": False},
                    },
                ],
            })

    return overrides


__all__ = [
    "BUILDER_COLORS",
    "PROPOSER_COLORS",
    "RELAY_COLORS",
    "SPECIAL_COLORS",
    "get_builder_color_overrides",
    "get_builder_color_overrides_with_hidden",
    "get_combined_overrides",
    "get_proposer_color_overrides",
    "get_relay_color_overrides",
    "get_special_color_overrides",
]
