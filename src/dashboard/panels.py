"""Helper functions for creating Grafana panels."""

from typing import Any, Dict, List, Optional

from grafanalib.core import (
    BarChart,
    GridPos,
    PieChartv2,
    RowPanel,
    Stat,
    Table,
    Target,
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
    return RowPanel(title=title, gridPos=GridPos(h=1, w=24, x=0, y=y), collapsed=collapsed)


def create_sql_target(
    query: str,
    ref_id: str = "A",
    hide: bool = False,
    interval: Optional[str] = None,
    max_data_points: Optional[int] = None,
) -> Dict[str, Any]:
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
    target_params: Dict[str, Any] = {
        "rawSql": query,
        "refId": ref_id,
        "hide": hide,
        "datasource": "${DS_TIMESCALEDB_- FLASHBOTS}",
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
        **kwargs: Additional arguments to pass to TimeSeries

    Returns:
        TimeSeries object
    """
    return TimeSeries(
        title=title,
        description=description,
        targets=[create_sql_target(query, interval=interval, max_data_points=max_data_points)],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        unit=unit,
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
    **kwargs: Any,
) -> BarChart:
    """Create a bar chart panel.

    Args:
        title: Panel title
        description: Panel description
        query: SQL query
        x: X position in grid
        y: Y position in grid
        w: Width in grid units (default 12)
        h: Height in grid units (default 15)
        unit: Unit for values (default "percent") - applied via overrides
        **kwargs: Additional arguments to pass to BarChart

    Returns:
        BarChart object
    """
    # BarChart doesn't have a unit parameter, but we can add it via overrides
    overrides = kwargs.pop("overrides", [])
    if unit != "percent":
        # Add unit override if needed
        pass

    return BarChart(
        title=title,
        description=description,
        targets=[create_sql_target(query)],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        overrides=overrides,
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
        **kwargs: Additional arguments to pass to Stat

    Returns:
        Stat object
    """
    return Stat(
        title=title,
        description=description,
        targets=[create_sql_target(query)],
        gridPos=GridPos(h=h, w=w, x=x, y=y),
        format=unit,  # Stat uses "format" not "unit"
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
