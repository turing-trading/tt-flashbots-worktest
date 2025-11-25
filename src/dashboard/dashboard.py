"""MEV-Boost Relay Dashboard - Grafanalib implementation."""

from grafanalib.core import (
    Dashboard,
    Time,
)

from src.dashboard.colors import (
    get_builder_color_overrides,
    get_builder_color_overrides_with_hidden,
    get_proposer_color_overrides,
    get_relay_color_overrides,
    get_special_color_overrides,
)
from src.dashboard.panels import (
    create_bar_chart,
    create_pie_chart,
    create_row,
    create_stat,
    create_table,
    create_time_series,
)
from src.dashboard.queries import load_query


def generate_dashboard() -> Dashboard:
    """Generate the MEV-Boost Relay dashboard.

    Returns:
        Dashboard object that can be converted to JSON
    """
    # Define Y positions for rows and panels
    # Row 1: General (y=0)
    general_row_y = 0
    general_panels_y = 1

    # Row 2: Relay (y=16)
    relay_row_y = 16
    relay_panels_y = 17

    # Row 3: Builder (y=32)
    builder_row_y = 32
    builder_panels_y_1 = 33
    builder_panels_y_2 = 48

    # Row 4: Proposer (y=63)
    proposer_row_y = 63
    proposer_panels_y_1 = 64
    proposer_panels_y_2 = 79

    # Row 5: Value and Profitability (y=94)
    value_row_y = 94
    value_panels_y_1 = 95
    value_panels_y_2 = 102
    value_panels_y_3 = 110
    value_panels_y_4 = 125
    value_panels_y_5 = 140
    value_panels_y_6 = 155

    # Load SQL queries
    mev_boost_market_share_pie = load_query("A_general", "1_mev_boost_market_share.sql")
    mev_boost_market_share_ts = load_query("A_general", "2_mev_boost_market_share.sql")

    relay_market_share_pie = load_query("B_relay", "1_relay_market_share.sql")
    relay_market_share_ts = load_query("B_relay", "2_relay_market_share.sql")

    builder_ms_blocks_pie = load_query(
        "C_builder", "1_builder_market_share_number_of_blocks.sql"
    )
    builder_ms_blocks_ts = load_query(
        "C_builder", "2_builder_market_share_number_of_blocks.sql"
    )
    builder_ms_profit_pie = load_query(
        "C_builder", "3_builder_market_share_eth_profit.sql"
    )
    builder_ms_profit_ts = load_query(
        "C_builder", "4_builder_market_share_eth_profit.sql"
    )

    proposer_ms_blocks_pie = load_query("E_proposer", "1_proposer_market_share.sql")
    proposer_ms_blocks_ts = load_query("E_proposer", "2_proposer_market_share.sql")
    proposer_ms_profit_pie = load_query("E_proposer", "3_proposer_profit_eth.sql")
    proposer_ms_profit_ts = load_query("E_proposer", "4_proposer_profit_eth.sql")

    total_value_dist = load_query(
        "D_value_and_profitability", "1_total_value_distribution_percent.sql"
    )
    total_value_dist_2 = load_query(
        "D_value_and_profitability", "2_total_value_distribution_percent_1.sql"
    )
    avg_total_value = load_query(
        "D_value_and_profitability", "3_average_total_value.sql"
    )
    negative_total_value = load_query(
        "D_value_and_profitability", "4_negative_total_value.sql"
    )
    proposer_vs_builder = load_query(
        "D_value_and_profitability", "5_proposer_vs_builder_profit.sql"
    )
    proposer_share_per_builder = load_query(
        "D_value_and_profitability", "6_proposer_share_per_builder.sql"
    )
    overbid_dist = load_query("D_value_and_profitability", "7_overbid_distribution.sql")
    proposer_share_total = load_query(
        "D_value_and_profitability", "8_proposer_share_of_total_value.sql"
    )
    negative_blocks_mev = load_query(
        "D_value_and_profitability", "9_negative_total_value_blocks_mev_boost.sql"
    )
    negative_blocks_vanilla = load_query(
        "D_value_and_profitability", "10_negative_total_value_blocks_vanilla.sql"
    )
    negative_blocks_table = load_query(
        "D_value_and_profitability", "11_negative_total_value_blocks.sql"
    )

    # Create panels
    panels = [
        # Row 1: General
        create_row("General", general_row_y),
        create_pie_chart(
            title="MEV-Boost Market Share",
            description=(
                "Shows the percentage of Ethereum blocks built through "
                "MEV-Boost vs. vanilla blocks. High MEV-Boost share indicates "
                "strong builder-relay ecosystem usage and higher proposer rewards."
            ),
            query=mev_boost_market_share_pie,
            x=0,
            y=general_panels_y,
            w=12,
            h=15,
            reduce_fields="/^market_share_pct$/",
            overrides=get_special_color_overrides(["vanilla", "mev-boost"]),
        ),
        create_time_series(
            title="MEV-Boost Market Share",
            description=(
                "Tracks how the share of MEV-Boost blocks changes over time. "
                "Useful for spotting shifts in relay adoption, outages, or "
                "network-wide MEV dynamics."
            ),
            query=mev_boost_market_share_ts,
            x=12,
            y=general_panels_y,
            w=12,
            h=15,
            stacking_mode="normal",
            axisSoftMin=0,
            axisSoftMax=100,
            show_points="never",
            connect_null_values="always",
            fill_opacity=30,
            line_interpolation="smooth",
            transformations=[
                {
                    "id": "groupingToMatrix",
                    "options": {
                        "columnField": "block_type",
                        "rowField": "time",
                        "valueField": "market_share_pct",
                    },
                }
            ],
            overrides=get_special_color_overrides(["vanilla", "mev-boost"]),
        ),
        # Row 2: Relay
        create_row("Relay", relay_row_y),
        create_pie_chart(
            title="Relay Market Share",
            description=(
                "Displays each relay's share of delivered blocks. "
                "Helps identify dominant relays and diversification across "
                "the relay ecosystem."
            ),
            query=relay_market_share_pie,
            x=0,
            y=relay_panels_y,
            w=12,
            h=15,
            reduce_fields="/^market_share_pct$/",
            transformations=[
                {
                    "id": "sortBy",
                    "options": {
                        "fields": {},
                        "sort": [{"field": "relay"}],
                    },
                }
            ],
            overrides=get_relay_color_overrides(),
        ),
        create_time_series(
            title="Relay Market Share",
            description=(
                "Shows the relative performance and activity of relays "
                "over time. Reveals reliability issues, relay churn, or surges "
                "in usage by builders or validators."
            ),
            query=relay_market_share_ts,
            x=12,
            y=relay_panels_y,
            w=12,
            h=15,
            stacking_mode="normal",
            axis_max=100,
            axis_min=0,
            axisSoftMin=0,
            axisSoftMax=100,
            show_points="never",
            connect_null_values="always",
            fill_opacity=30,
            line_interpolation="smooth",
            transformations=[
                {
                    "id": "groupingToMatrix",
                    "options": {
                        "columnField": "relay",
                        "rowField": "time",
                        "valueField": "market_share_pct",
                    },
                }
            ],
            overrides=get_relay_color_overrides(),
        ),
        # Row 3: Builder
        create_row("Builder", builder_row_y),
        create_pie_chart(
            title="Builder Market Share (Number of blocks)",
            description="Breakdown of which builders are producing the most blocks. "
            "Highlights dominant builders and overall builder-level competition.",
            query=builder_ms_blocks_pie,
            x=0,
            y=builder_panels_y_1,
            w=12,
            h=15,
            reduce_fields="/^market_share_pct$/",
            overrides=get_builder_color_overrides(),
        ),
        create_time_series(
            title="Builder Market Share (Number of blocks)",
            description=(
                "Shows block-production trends over time for each builder. "
                "Useful for identifying new entrants, growth/decline in "
                "builder influence, and network events affecting activity."
            ),
            query=builder_ms_blocks_ts,
            x=12,
            y=builder_panels_y_1,
            w=12,
            h=15,
            stacking_mode="normal",
            axis_max=100,
            axis_min=0,
            axisSoftMin=0,
            axisSoftMax=100,
            show_points="never",
            connect_null_values="always",
            fill_opacity=30,
            line_interpolation="smooth",
            transformations=[
                {
                    "id": "groupingToMatrix",
                    "options": {
                        "columnField": "builder_name",
                        "rowField": "time",
                        "valueField": "market_share_pct",
                    },
                }
            ],
            overrides=get_builder_color_overrides(),
        ),
        create_pie_chart(
            title="Builder Market Share (ETH profit)",
            description=(
                "Distribution of total MEV/ETH profit captured by each "
                "builder. Shows not just who produces blocks—but who extracts "
                "the most value."
            ),
            query=builder_ms_profit_pie,
            x=0,
            y=builder_panels_y_2,
            w=12,
            h=15,
            unit="ETH",
            reduce_fields="/^total_profit_eth$/",
            overrides=get_builder_color_overrides(),
        ),
        create_time_series(
            title="Builder Market Share (ETH profit)",
            description=(
                "Tracks builder profitability over time. Profit spikes often "
                "correlate with MEV opportunities (liquidations, arbitrage, "
                "mempool spikes)."
            ),
            query=builder_ms_profit_ts,
            x=12,
            y=builder_panels_y_2,
            w=12,
            h=15,
            unit="ETH",
            axis_scale_type="log",
            show_points="never",
            connect_null_values="always",
            line_interpolation="smooth",
            transformations=[
                {
                    "id": "groupingToMatrix",
                    "options": {
                        "columnField": "builder_name",
                        "rowField": "time",
                        "valueField": "total_profit_eth",
                    },
                }
            ],
            overrides=get_builder_color_overrides_with_hidden([
                "Titan",
                "BuilderNet (Beaver)",
                "BuilderNet (Flashbots)",
                "BuilderNet (Nethermind)",
            ]),
        ),
        # Row 4: Proposer
        create_row("Proposer", proposer_row_y),
        create_pie_chart(
            title="Proposer Market Share (Number of blocks)",
            description=(
                "Breakdown of which staking entities are proposing the most blocks. "
                "Highlights dominant validators and network decentralization."
            ),
            query=proposer_ms_blocks_pie,
            x=0,
            y=proposer_panels_y_1,
            w=12,
            h=15,
            reduce_fields="/^market_share_pct$/",
            overrides=get_proposer_color_overrides(),
        ),
        create_time_series(
            title="Proposer Market Share (Number of blocks)",
            description=(
                "Shows block-production trends over time for each staking entity. "
                "Useful for tracking validator ecosystem changes and decentralization."
            ),
            query=proposer_ms_blocks_ts,
            x=12,
            y=proposer_panels_y_1,
            w=12,
            h=15,
            stacking_mode="normal",
            axis_max=100,
            axis_min=0,
            axisSoftMin=0,
            axisSoftMax=100,
            show_points="never",
            connect_null_values="always",
            fill_opacity=30,
            line_interpolation="smooth",
            transformations=[
                {
                    "id": "groupingToMatrix",
                    "options": {
                        "columnField": "proposer_name",
                        "rowField": "time",
                        "valueField": "market_share_pct",
                    },
                }
            ],
            overrides=get_proposer_color_overrides(),
        ),
        create_pie_chart(
            title="Proposer Profit",
            description=(
                "Distribution of total ETH received by each staking entity. "
                "Shows which validators capture the most MEV value from builders."
            ),
            query=proposer_ms_profit_pie,
            x=0,
            y=proposer_panels_y_2,
            w=12,
            h=15,
            unit="ETH",
            reduce_fields="/^total_profit_eth$/",
            overrides=get_proposer_color_overrides(),
        ),
        create_time_series(
            title="Proposer Profit",
            description=(
                "Tracks proposer subsidy earnings over time. "
                "Shows how MEV value flows to different staking entities."
            ),
            query=proposer_ms_profit_ts,
            x=12,
            y=proposer_panels_y_2,
            w=12,
            h=15,
            unit="ETH",
            axis_scale_type="log",
            show_points="never",
            connect_null_values="always",
            line_interpolation="smooth",
            transformations=[
                {
                    "id": "groupingToMatrix",
                    "options": {
                        "columnField": "proposer_name",
                        "rowField": "time",
                        "valueField": "total_profit_eth",
                    },
                }
            ],
            overrides=get_proposer_color_overrides(),
        ),
        # Row 5: Value and Profitability
        create_row("Value and Profitability", value_row_y),
        create_bar_chart(
            title="Total Value Distribution (percent)",
            description=(
                "Distribution of block values (proposer profit) for "
                "MEV-Boost vs vanilla blocks. Reveals how MEV-Boost increases "
                "value, and how often negative or low-value blocks occur."
            ),
            query=total_value_dist,
            query2=total_value_dist_2,
            x=0,
            y=value_panels_y_1,
            w=12,
            h=15,
            unit="none",
            x_field="bucket_min",
            orientation="vertical",
            transformations=[{"id": "merge", "options": {}}],
            overrides=get_special_color_overrides(["vanilla"]),
        ),
        create_stat(
            title="Average Total Value",
            description=(
                "Displays average ETH earned per block for MEV-Boost and "
                "vanilla. Useful high-level metric to quantify the value of "
                "MEV-Boost."
            ),
            query=avg_total_value,
            x=12,
            y=value_panels_y_1,
            w=12,
            h=7,
            unit="ETH",
            transformations=[{"id": "transpose", "options": {}}],
            color="green",
        ),
        create_stat(
            title="Negative Total Value",
            description=(
                "Shows the percentage of blocks with negative value for "
                "MEV-Boost and vanilla."
            ),
            query=negative_total_value,
            x=12,
            y=value_panels_y_2,
            w=12,
            h=8,
            unit="percent",
            transformations=[{"id": "transpose", "options": {}}],
            color="green",
        ),
        create_pie_chart(
            title="Proposer vs. Builder Profit",
            description=(
                "Breaks down MEV revenue split between proposers and "
                "builders. Useful for monitoring whether builders retain too "
                "much value or if proposer rewards are healthy."
            ),
            query=proposer_vs_builder,
            x=0,
            y=value_panels_y_3,
            w=12,
            h=15,
            unit="percentunit",
            overrides=get_special_color_overrides([
                "Proposer Profit",
                "Builder Profit",
                "Relay Fee",
            ]),
        ),
        create_time_series(
            title="Proposer share (per builder)",
            description="Proposer share of total value in block per builder over time.",
            query=proposer_share_per_builder,
            x=12,
            y=value_panels_y_3,
            w=12,
            h=15,
            unit="percent",
            interval="1h",
            max_data_points=100,
            spanNulls=True,
            line_interpolation="smooth",
            transformations=[
                {
                    "id": "groupingToMatrix",
                    "options": {
                        "columnField": "builder_name",
                        "rowField": "time",
                        "valueField": "proposer_profit_pct",
                    },
                }
            ],
            overrides=get_builder_color_overrides(),
        ),
        create_bar_chart(
            title="Overbid Distribution",
            description=(
                "Shows how much builders are overbidding relative to block "
                "value. Large overbids indicate competitive pressure among "
                "builders or strategies that prioritize winning over "
                "profitability."
            ),
            query=overbid_dist,
            x=0,
            y=value_panels_y_4,
            w=12,
            h=15,
            unit="percent",
        ),
        create_bar_chart(
            title="Proposer share of total value",
            description=(
                "Useful for monitoring whether builders retain too much value "
                "or if proposer rewards are healthy."
            ),
            query=proposer_share_total,
            x=12,
            y=value_panels_y_4,
            w=12,
            h=15,
            unit="percent",
            axis_max=100,
        ),
        create_bar_chart(
            title="Negative Total Value Blocks (MEV-boost)",
            description=(
                "Counts negative-value blocks per builder. "
                "Useful for identifying risky or inefficient builders and "
                "relay-builder mismatches."
            ),
            query=negative_blocks_mev,
            x=0,
            y=value_panels_y_5,
            w=12,
            h=15,
            unit="percent",
            axis_max=100,
            overrides=get_builder_color_overrides(),
        ),
        create_bar_chart(
            title="Negative Total Value Blocks (vanilla)",
            description=(
                "Counts negative-value blocks per builder (vanilla blocks "
                "only). Useful for identifying risky or inefficient builders "
                "and relay-builder mismatches."
            ),
            query=negative_blocks_vanilla,
            x=12,
            y=value_panels_y_5,
            w=12,
            h=15,
            unit="percent",
            axis_max=100,
            overrides=get_builder_color_overrides(),
        ),
        create_table(
            title="Negative Total Value blocks",
            description=(
                "Lists all blocks where total value was negative. "
                "Helps diagnose which builders are losing money, and whether "
                "specific relays or strategies malfunctioned."
            ),
            query=negative_blocks_table,
            x=0,
            y=value_panels_y_6,
            w=24,
            h=18,
        ),
    ]

    # Create the dashboard
    return Dashboard(
        title="MEV-Boost Relay (Thomas' Worktest)",
        uid="e46c6ca2-cd80-4811-955b-f4fcafc860af",
        description=(
            "MEV-Boost Relay analytics dashboard showing market share, "
            "builder performance, and value distribution"
        ),
        tags=["mev-boost", "relay", "builder", "ethereum"],
        timezone="browser",
        panels=panels,
        time=Time(start="now-20d", end="now"),
        refresh="",
        editable=True,
        version=168,
        schemaVersion=42,
    ).auto_panel_ids()
