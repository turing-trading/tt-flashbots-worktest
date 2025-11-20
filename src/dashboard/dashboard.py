"""MEV-Boost Relay Dashboard - Grafanalib implementation."""

from grafanalib.core import (
    Dashboard,
    DashboardLink,
    Time,
    Templating,
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

    # Row 4: Value and Profitability (y=63)
    value_row_y = 63
    value_panels_y_1 = 64
    value_panels_y_2 = 71
    value_panels_y_3 = 79
    value_panels_y_4 = 94
    value_panels_y_5 = 109
    value_panels_y_6 = 124

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
            description="Shows the percentage of Ethereum blocks built through MEV-Boost vs. vanilla blocks. "
            "High MEV-Boost share indicates strong builder-relay ecosystem usage and higher proposer rewards.",
            query=mev_boost_market_share_pie,
            x=0,
            y=general_panels_y,
            w=12,
            h=15,
            reduce_fields="/^market_share_pct$/",
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "vanilla"},
                    "properties": [
                        {"id": "color", "value": {"fixedColor": "red", "mode": "fixed"}}
                    ],
                }
            ],
        ),
        create_time_series(
            title="MEV-Boost Market Share",
            description="Tracks how the share of MEV-Boost blocks changes over time. "
            "Useful for spotting shifts in relay adoption, outages, or network-wide MEV dynamics.",
            query=mev_boost_market_share_ts,
            x=12,
            y=general_panels_y,
            w=12,
            h=15,
            stacking_mode="normal",
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
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "vanilla"},
                    "properties": [
                        {"id": "color", "value": {"fixedColor": "red", "mode": "fixed"}}
                    ],
                }
            ],
        ),
        # Row 2: Relay
        create_row("Relay", relay_row_y),
        create_pie_chart(
            title="Relay Market Share",
            description="Displays each relay's share of delivered blocks. "
            "Helps identify dominant relays and diversification across the relay ecosystem.",
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
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "Ultrasound"},
                    "properties": [
                        {"id": "color", "value": {"fixedColor": "#37872D", "mode": "fixed"}}
                    ],
                }
            ],
        ),
        create_time_series(
            title="Relay Market Share",
            description="Shows the relative performance and activity of relays over time. "
            "Reveals reliability issues, relay churn, or surges in usage by builders or validators.",
            query=relay_market_share_ts,
            x=12,
            y=relay_panels_y,
            w=12,
            h=15,
            stacking_mode="normal",
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
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "Titan"},
                    "properties": [
                        {"id": "color", "value": {"fixedColor": "green", "mode": "fixed"}}
                    ],
                }
            ],
        ),
        create_time_series(
            title="Builder Market Share (Number of blocks)",
            description="Shows block-production trends over time for each builder. "
            "Useful for identifying new entrants, growth/decline in builder influence, and network events affecting builder activity.",
            query=builder_ms_blocks_ts,
            x=12,
            y=builder_panels_y_1,
            w=12,
            h=15,
            stacking_mode="normal",
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
        ),
        create_pie_chart(
            title="Builder Market Share (ETH profit)",
            description="Distribution of total MEV/ETH profit captured by each builder. "
            "Shows not just who produces blocks—but who extracts the most value.",
            query=builder_ms_profit_pie,
            x=0,
            y=builder_panels_y_2,
            w=12,
            h=15,
            reduce_fields="/^total_profit_eth$/",
        ),
        create_time_series(
            title="Builder Market Share (ETH profit)",
            description="Tracks builder profitability over time. "
            "Profit spikes often correlate with MEV opportunities (liquidations, arbitrage, mempool spikes).",
            query=builder_ms_profit_ts,
            x=12,
            y=builder_panels_y_2,
            w=12,
            h=15,
            unit="none",
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
        ),
        # Row 4: Value and Profitability
        create_row("Value and Profitability", value_row_y),
        create_bar_chart(
            title="Total Value Distribution (percent)",
            description="Distribution of block values (proposer profit) for MEV-Boost vs vanilla blocks. "
            "Reveals how MEV-Boost increases value, and how often negative or low-value blocks occur.",
            query=total_value_dist,
            query2=total_value_dist_2,
            x=0,
            y=value_panels_y_1,
            w=12,
            h=15,
            transformations=[{"id": "merge", "options": {}}],
        ),
        create_stat(
            title="Average Total Value",
            description="Displays average ETH earned per block for MEV-Boost and vanilla. "
            "Useful high-level metric to quantify the value of MEV-Boost.",
            query=avg_total_value,
            x=12,
            y=value_panels_y_1,
            w=12,
            h=7,
            unit="currencyUSD",
            transformations=[{"id": "transpose", "options": {}}],
        ),
        create_stat(
            title="Negative Total Value",
            description="Shows the percentage of blocks with negative value for MEV-Boost and vanilla.",
            query=negative_total_value,
            x=12,
            y=value_panels_y_2,
            w=12,
            h=8,
            unit="percent",
            transformations=[{"id": "transpose", "options": {}}],
        ),
        create_pie_chart(
            title="Proposer vs. Builder Profit",
            description="Breaks down MEV revenue split between proposers and builders. "
            "Useful for monitoring whether builders retain too much value or if proposer rewards are healthy.",
            query=proposer_vs_builder,
            x=0,
            y=value_panels_y_3,
            w=12,
            h=15,
            unit="percentunit",
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "Proposer Profit"},
                    "properties": [
                        {"id": "color", "value": {"fixedColor": "orange", "mode": "fixed"}}
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "Builder Profit"},
                    "properties": [
                        {"id": "color", "value": {"fixedColor": "green", "mode": "fixed"}}
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "Relay Fee"},
                    "properties": [
                        {"id": "color", "value": {"fixedColor": "red", "mode": "fixed"}}
                    ],
                },
            ],
        ),
        create_time_series(
            title="Proposer share (per builder)",
            description="Proposer share of total value in block per builder over time.",
            query=proposer_share_per_builder,
            x=12,
            y=value_panels_y_3,
            w=12,
            h=15,
            interval="1h",
            max_data_points=100,
            spanNulls=True,
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
        ),
        create_bar_chart(
            title="Overbid Distribution",
            description="Shows how much builders are overbidding relative to block value. "
            "Large overbids indicate competitive pressure among builders or strategies that prioritize winning over profitability.",
            query=overbid_dist,
            x=0,
            y=value_panels_y_4,
            w=12,
            h=15,
        ),
        create_bar_chart(
            title="Proposer share of total value",
            description="Useful for monitoring whether builders retain too much value or if proposer rewards are healthy.",
            query=proposer_share_total,
            x=12,
            y=value_panels_y_4,
            w=12,
            h=15,
        ),
        create_bar_chart(
            title="Negative Total Value Blocks (MEV-boost)",
            description="Counts negative-value blocks per builder. "
            "Useful for identifying risky or inefficient builders and relay-builder mismatches.",
            query=negative_blocks_mev,
            x=0,
            y=value_panels_y_5,
            w=12,
            h=15,
        ),
        create_bar_chart(
            title="Negative Total Value Blocks (vanilla)",
            description="Counts negative-value blocks per builder (vanilla blocks only). "
            "Useful for identifying risky or inefficient builders and relay-builder mismatches.",
            query=negative_blocks_vanilla,
            x=12,
            y=value_panels_y_5,
            w=12,
            h=15,
        ),
        create_table(
            title="Negative Total Value blocks",
            description="Lists all blocks where total value was negative. "
            "Helps diagnose which builders are losing money, and whether specific relays or strategies malfunctioned.",
            query=negative_blocks_table,
            x=0,
            y=value_panels_y_6,
            w=24,
            h=18,
        ),
    ]

    # Create the dashboard
    dashboard = Dashboard(
        title="MEV-Boost Relay (Thomas' Worktest)",
        uid="e46c6ca2-cd80-4811-955b-f4fcafc860af",
        description="MEV-Boost Relay analytics dashboard showing market share, builder performance, and value distribution",
        tags=["mev-boost", "relay", "builder", "ethereum"],
        timezone="browser",
        panels=panels,
        time=Time(start="now-20d", end="now"),
        refresh="",
        editable=True,
        version=168,
        schemaVersion=42,
    ).auto_panel_ids()

    return dashboard
