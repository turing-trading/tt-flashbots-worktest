"""Shared progress bar utilities for Rich console displays."""

from typing import TYPE_CHECKING

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


if TYPE_CHECKING:
    from rich.console import Console


def create_standard_progress(
    console: Console | None = None, *, expand: bool = False
) -> Progress:
    """Create a standard progress bar with time remaining estimation.

    Use this for processes with a known total where time estimation is valuable.

    Args:
        console: Rich console instance (optional)
        expand: Whether to expand the progress bar to full width

    Returns:
        Configured Progress instance with:
        - Spinner
        - Task description
        - Progress bar
        - M of N counter
        - Time elapsed
        - Time remaining

    Example:
        ```python
        from rich.console import Console
        from src.helpers.progress import create_standard_progress

        console = Console()
        progress = create_standard_progress(console)

        with progress:
            task_id = progress.add_task("Processing blocks", total=1000)
            # ... process items ...
            progress.update(task_id, advance=1)
        ```
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
        console=console,
        expand=expand,
    )


def create_simple_progress(
    console: Console | None = None, *, expand: bool = False
) -> Progress:
    """Create a simple progress bar without time remaining estimation.

    Use this for processes where the total is uncertain or time estimation
    is not meaningful.

    Args:
        console: Rich console instance (optional)
        expand: Whether to expand the progress bar to full width

    Returns:
        Configured Progress instance with:
        - Spinner
        - Task description
        - Progress bar
        - M of N counter
        - Time elapsed (no time remaining)

    Example:
        ```python
        from rich.console import Console
        from src.helpers.progress import create_simple_progress

        console = Console()
        progress = create_simple_progress(console, expand=True)

        with progress:
            task_id = progress.add_task("Fetching data", total=None)
            # ... process items ...
            progress.update(task_id, advance=1)
        ```
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        expand=expand,
    )


__all__ = [
    "TaskID",
    "create_simple_progress",
    "create_standard_progress",
]
