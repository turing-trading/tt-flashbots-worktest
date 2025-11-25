"""Helper functions for creating Grafana panels."""

from typing import Any

import attr
from grafanalib.core import (
    BarChart,
    GridPos,
    Panel,  # type: ignore[attr-defined]
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
    axis_max: float | None = None,
    axis_min: float | None = None,
    tooltip_mode: str = "multi",
    tooltip_sort: str = "desc",
    show_points: str = "auto",
    connect_null_values: str = "never",
    fill_opacity: int = 0,
    line_interpolation: str = "linear",
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
        axis_max: Maximum value for Y-axis (default None for auto)
        axis_min: Minimum value for Y-axis (default None for auto)
        tooltip_mode: Tooltip mode ("single", "multi", "none") (default "multi")
        tooltip_sort: Tooltip sort order ("none", "asc", "desc") (default "desc")
        show_points: Show points mode ("auto", "always", "never") (default "auto")
        connect_null_values: Connect null values ("never", "threshold", "always")
        (default "never")
        fill_opacity: Fill opacity 0-100 (default 0)
        line_interpolation: Line interpolation mode ("linear", "smooth", "stepBefore",
        "stepAfter") (default "linear")
        **kwargs: Additional arguments to pass to TimeSeries

    Returns:
        TimeSeries object
    """
    # Build stacking configuration
    stacking_config = {"group": "A", "mode": stacking_mode} if stacking_mode else {}

    # Build extraJson for axis scale and tooltip configuration
    extra_json = kwargs.pop("extraJson", {})

    # Initialize fieldConfig if needed for any configuration
    if (
        axis_scale_type  # noqa: PLR0916
        or axis_max is not None
        or axis_min is not None
        or show_points != "auto"
        or connect_null_values != "never"
        or fill_opacity != 0
        or line_interpolation != "linear"
    ):
        extra_json["fieldConfig"] = extra_json.get("fieldConfig", {})
        extra_json["fieldConfig"]["defaults"] = extra_json["fieldConfig"].get(
            "defaults", {}
        )

    # Configure axis scale if specified
    if axis_scale_type:
        extra_json["fieldConfig"]["defaults"]["custom"] = extra_json["fieldConfig"][
            "defaults"
        ].get("custom", {})
        extra_json["fieldConfig"]["defaults"]["custom"]["scaleDistribution"] = {
            "type": axis_scale_type
        }

    # Configure display options (show points, connect null values, fill opacity,
    # line interpolation)
    if (
        show_points != "auto"
        or connect_null_values != "never"
        or fill_opacity != 0
        or line_interpolation != "linear"
    ):
        extra_json["fieldConfig"]["defaults"]["custom"] = extra_json["fieldConfig"][
            "defaults"
        ].get("custom", {})
        if show_points != "auto":
            extra_json["fieldConfig"]["defaults"]["custom"]["showPoints"] = show_points
        if connect_null_values != "never":
            extra_json["fieldConfig"]["defaults"]["custom"]["spanNulls"] = (
                connect_null_values
            )
        if fill_opacity != 0:
            extra_json["fieldConfig"]["defaults"]["custom"]["fillOpacity"] = (
                fill_opacity
            )
        if line_interpolation != "linear":
            extra_json["fieldConfig"]["defaults"]["custom"]["lineInterpolation"] = (
                line_interpolation
            )

    # Configure axis min/max if specified
    if axis_max is not None:
        extra_json["fieldConfig"]["defaults"]["max"] = axis_max
    if axis_min is not None:
        extra_json["fieldConfig"]["defaults"]["min"] = axis_min

    # Configure tooltip
    extra_json["options"] = extra_json.get("options", {})
    extra_json["options"]["tooltip"] = {
        "hideZeros": False,
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
    orientation: str = "auto",
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
        orientation: Bar orientation ("auto", "horizontal", "vertical")
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

    # Add orientation configuration
    if orientation != "auto":
        extra_json["options"] = extra_json.get("options", {})
        extra_json["options"]["orientation"] = orientation

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


@attr.s
class XYChart(Panel):
    """XY Chart panel for scatter plots."""

    xField: str = attr.ib(default="x")  # noqa: N815
    yField: str = attr.ib(default="y")  # noqa: N815
    colorField: str | None = attr.ib(default=None)  # noqa: N815
    seriesMapping: str = attr.ib(default="auto")  # noqa: N815
    unit: str = attr.ib(default="short")
    transformations: list[dict[str, Any]] = attr.ib(factory=list)
    overrides: list[dict[str, Any]] = attr.ib(factory=list)

    def to_json_data(self) -> dict[str, Any]:
        """Convert to JSON data for Grafana."""
        panel_json = super().panel_json(overrides={})
        panel_json["type"] = "xychart"
        panel_json["fieldConfig"] = {
            "defaults": {
                "unit": self.unit,
                "color": {"mode": "palette-classic-by-name"},
                "custom": {
                    "hideFrom": {"tooltip": False, "viz": False, "legend": False}
                },
            },
            "overrides": self.overrides,
        }

        # Add transformations if specified
        if self.transformations:
            panel_json["transformations"] = self.transformations

        # Build options with color field if specified
        options: dict[str, Any] = {
            "dims": {},
            "series": [],
            "seriesMapping": self.seriesMapping,
            "tooltip": {"mode": "single", "sort": "none"},
            "legend": {
                "showLegend": True,
                "displayMode": "list",
                "placement": "bottom",
            },
        }

        # Set dims based on whether we have a color field
        if self.colorField:
            options["dims"] = {
                "x": self.xField,
                "y": self.yField,
                "color": self.colorField,
            }
        else:
            options["dims"] = {
                "x": self.xField,
                "y": self.yField,
            }

        panel_json["options"] = options
        return panel_json


def create_scatter_plot(
    title: str,
    description: str,
    query: str,
    x: int,
    y: int,
    w: int = 12,
    h: int = 15,
    x_field: str = "total_value",
    y_field: str = "proposer_subsidy",
    color_field: str | None = None,
    unit: str = "ETH",
    transformations: list[dict[str, Any]] | None = None,
    overrides: list[dict[str, Any]] | None = None,
    **kwargs: Any,
) -> XYChart:
    """Create a scatter plot panel using XY Chart.

    Args:
        title: Panel title
        description: Panel description
        query: SQL query
        x: X position in grid
        y: Y position in grid
        w: Width in grid units (default 12)
        h: Height in grid units (default 15)
        x_field: Field name for x-axis (default "total_value")
        y_field: Field name for y-axis (default "proposer_subsidy")
        color_field: Field name for coloring points (default None)
        unit: Unit for values (default "ETH")
        transformations: List of transformation dictionaries
        overrides: List of field override dictionaries
        **kwargs: Additional arguments

    Returns:
        XYChart panel
    """
    return XYChart(
        title=title,
        description=description,
        targets=[create_sql_target(query)],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        xField=x_field,
        yField=y_field,
        colorField=color_field,
        unit=unit,
        transformations=transformations or [],
        overrides=overrides or [],
        **kwargs,
    )
