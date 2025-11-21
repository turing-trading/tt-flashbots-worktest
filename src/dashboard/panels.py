"""Helper functions for creating Grafana panels."""

from typing import Any

from grafanalib.core import (
    BarChart,
    GridPos,
    PieChartv2,
    RowPanel,
    Stat,
    Table,
    TimeSeries,
)


def create_row(title: str, y: int, collapsed: bool = False) -> RowPanel:
    """Create a row panel.

    Args:
        title: Row title
        y: Y position in the grid
        collapsed: Whether the row is collapsed by default

    Returns:
        RowPanel object
    """
    return RowPanel(
        title=title, gridPos=GridPos(h=1, w=24, x=0, y=y), collapsed=collapsed
    )


def create_sql_target(
    query: str,
    ref_id: str = "A",
    hide: bool = False,
    interval: str | None = None,
    max_data_points: int | None = None,
) -> dict[str, Any]:
    """Create a SQL target for PostgreSQL datasource.

    Args:
        query: SQL query string
        ref_id: Reference ID for the query
        hide: Whether to hide this query
        interval: Optional interval for time grouping
        max_data_points: Optional max data points

    Returns:
        Target dictionary
    """
    target_params: dict[str, Any] = {
        "rawSql": query,
        "refId": ref_id,
        "hide": hide,
        "datasource": "ts-flashbots",
        "format": "table",
        "rawQuery": True,
    }

    if interval:
        target_params["interval"] = interval
    if max_data_points:
        target_params["maxDataPoints"] = max_data_points

    return target_params


def create_pie_chart(
    title: str,
    description: str,
    query: str,
    x: int,
    y: int,
    w: int = 12,
    h: int = 15,
    unit: str = "percent",
    pie_type: str = "donut",
    reduce_fields: str = "",
    transformations: list[dict[str, Any]] | None = None,
    overrides: list[dict[str, Any]] | None = None,
    **kwargs: Any,
) -> PieChartv2:
    """Create a pie chart panel.

    Args:
        title: Panel title
        description: Panel description
        query: SQL query
        x: X position in grid
        y: Y position in grid
        w: Width in grid units (default 12)
        h: Height in grid units (default 15)
        unit: Unit for values (default "percent")
        pie_type: Type of pie chart ("pie" or "donut")
        reduce_fields: Field regex for reduce options (e.g., "/^market_share_pct$/")
        transformations: List of transformation dictionaries
        overrides: List of field override dictionaries
        **kwargs: Additional arguments to pass to PieChartv2

    Returns:
        PieChartv2 object
    """
    return PieChartv2(
        title=title,
        description=description,
        targets=[create_sql_target(query)],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        unit=unit,
        pieType=pie_type,
        legendPlacement="right",
        legendDisplayMode="list",
        reduceOptionsCalcs=["lastNotNull"],
        reduceOptionsFields=reduce_fields,
        reduceOptionsValues=bool(reduce_fields),
        transformations=transformations or [],
        overrides=overrides or [],
        **kwargs,
    )


def create_time_series(
    title: str,
    description: str,
    query: str,
    x: int,
    y: int,
    w: int = 12,
    h: int = 15,
    unit: str = "percent",
    interval: str = "10m",
    max_data_points: int = 300,
    stacking_mode: str | None = None,
    transformations: list[dict[str, Any]] | None = None,
    overrides: list[dict[str, Any]] | None = None,
    axis_scale_type: str | None = None,
    tooltip_mode: str = "multi",
    tooltip_sort: str = "none",
    **kwargs: Any,
) -> TimeSeries:
    """Create a time series panel.

    Args:
        title: Panel title
        description: Panel description
        query: SQL query
        x: X position in grid
        y: Y position in grid
        w: Width in grid units (default 12)
        h: Height in grid units (default 15)
        unit: Unit for values (default "percent")
        interval: Time interval for grouping (default "10m")
        max_data_points: Maximum data points (default 300)
        stacking_mode: Stacking mode ("normal", "percent", or None for no stacking)
        transformations: List of transformation dictionaries
        overrides: List of field override dictionaries
        axis_scale_type: Y-axis scale type ("linear", "log", or None for default)
        tooltip_mode: Tooltip mode ("single", "multi", "none") (default "multi")
        tooltip_sort: Tooltip sort order ("none", "asc", "desc") (default "none")
        **kwargs: Additional arguments to pass to TimeSeries

    Returns:
        TimeSeries object
    """
    # Build stacking configuration
    stacking_config = {"group": "A", "mode": stacking_mode} if stacking_mode else {}

    # Build extraJson for axis scale and tooltip configuration
    extra_json = kwargs.pop("extraJson", {})

    # Configure axis scale if specified
    if axis_scale_type:
        extra_json["fieldConfig"] = extra_json.get("fieldConfig", {})
        extra_json["fieldConfig"]["defaults"] = extra_json["fieldConfig"].get(
            "defaults", {}
        )
        extra_json["fieldConfig"]["defaults"]["custom"] = extra_json["fieldConfig"][
            "defaults"
        ].get("custom", {})
        extra_json["fieldConfig"]["defaults"]["custom"]["scaleDistribution"] = {
            "type": axis_scale_type
        }

    # Configure tooltip
    extra_json["options"] = extra_json.get("options", {})
    extra_json["options"]["tooltip"] = {
        "mode": tooltip_mode,
        "sort": tooltip_sort,
    }

    return TimeSeries(
        title=title,
        description=description,
        targets=[
            create_sql_target(query, interval=interval, max_data_points=max_data_points)
        ],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        unit=unit,
        stacking=stacking_config,
        transformations=transformations or [],
        overrides=overrides or [],
        extraJson=extra_json or None,
        **kwargs,
    )


def create_bar_chart(
    title: str,
    description: str,
    query: str,
    x: int,
    y: int,
    w: int = 12,
    h: int = 15,
    unit: str = "percent",
    x_field: str | None = None,
    query2: str | None = None,
    transformations: list[dict[str, Any]] | None = None,
    axis_max: float | None = None,
    axis_min: float | None = None,
    **kwargs: Any,
) -> BarChart:
    """Create a bar chart panel.

    Args:
        title: Panel title
        description: Panel description
        query: SQL query (first query)
        x: X position in grid
        y: Y position in grid
        w: Width in grid units (default 12)
        h: Height in grid units (default 15)
        unit: Unit for values (default "percent") - applied via overrides
        x_field: Field name to use for X-axis (required for bar charts)
        query2: Optional second SQL query (for merge transformations)
        transformations: List of transformation dictionaries
        axis_max: Maximum value for Y-axis (default None for auto)
        axis_min: Minimum value for Y-axis (default None for auto)
        **kwargs: Additional arguments to pass to BarChart

    Returns:
        BarChart object
    """
    # BarChart doesn't have a unit parameter, add it via extraJson fieldConfig
    overrides = kwargs.pop("overrides", [])

    # Build targets list
    targets = [create_sql_target(query, ref_id="A")]
    if query2:
        targets.append(create_sql_target(query2, ref_id="B"))

    # Add extraJson for options and fieldConfig that aren't in grafanalib's BarChart
    extra_json: dict[str, Any] = {}

    # Add unit configuration
    extra_json["fieldConfig"] = extra_json.get("fieldConfig", {})
    extra_json["fieldConfig"]["defaults"] = extra_json["fieldConfig"].get(
        "defaults", {}
    )
    extra_json["fieldConfig"]["defaults"]["unit"] = unit

    # Add axis min/max configuration if specified
    if axis_max is not None:
        extra_json["fieldConfig"]["defaults"]["max"] = axis_max
    if axis_min is not None:
        extra_json["fieldConfig"]["defaults"]["min"] = axis_min

    # Add x-axis field configuration if specified
    if x_field:
        extra_json["options"] = extra_json.get("options", {})
        extra_json["options"]["xField"] = x_field
        extra_json["options"]["xTickLabelSpacing"] = 100

    return BarChart(
        title=title,
        description=description,
        targets=targets,
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        overrides=overrides,
        transformations=transformations or [],
        fillOpacity=80,
        extraJson=extra_json or None,
        **kwargs,
    )


def create_stat(
    title: str,
    description: str,
    query: str,
    x: int,
    y: int,
    w: int = 12,
    h: int = 7,
    unit: str = "none",
    color: str | None = None,
    transformations: list[dict[str, Any]] | None = None,
    **kwargs: Any,
) -> Stat:
    """Create a stat panel.

    Args:
        title: Panel title
        description: Panel description
        query: SQL query
        x: X position in grid
        y: Y position in grid
        w: Width in grid units (default 12)
        h: Height in grid units (default 7)
        unit: Unit for values (default "none") - called "format" in Stat
        color: Color for the stat panel (e.g., "green", "red", "blue")
        transformations: List of transformation dictionaries
        **kwargs: Additional arguments to pass to Stat

    Returns:
        Stat object
    """
    # Use extraJson to set thresholds with color
    extra_json = {}
    if color:
        extra_json["fieldConfig"] = {
            "defaults": {
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": color, "value": None}],
                }
            }
        }

    return Stat(
        title=title,
        description=description,
        targets=[create_sql_target(query)],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        format=unit,  # Stat uses "format" not "unit"
        transformations=transformations or [],
        extraJson=extra_json or None,
        **kwargs,
    )


def create_table(
    title: str,
    description: str,
    query: str,
    x: int,
    y: int,
    w: int = 24,
    h: int = 18,
    **kwargs: Any,
) -> Table:
    """Create a table panel.

    Args:
        title: Panel title
        description: Panel description
        query: SQL query
        x: X position in grid
        y: Y position in grid
        w: Width in grid units (default 24)
        h: Height in grid units (default 18)
        **kwargs: Additional arguments to pass to Table

    Returns:
        Table object
    """
    return Table(
        title=title,
        description=description,
        targets=[create_sql_target(query)],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        **kwargs,
    )
