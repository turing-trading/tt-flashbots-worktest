"""Shared progress bar utilities for Rich console displays."""

from contextlib import contextmanager

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
    from collections.abc import Iterator

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


@contextmanager
def track_progress(
    description: str,
    total: int,
    console: Console | None = None,
    *,
    show_time_remaining: bool = True,
) -> Iterator[tuple[Progress, TaskID]]:
    """Context manager for tracking progress with automatic cleanup.

    This is a convenience wrapper that creates a progress bar, adds a task,
    and automatically manages the progress context.

    Args:
        description: Task description to display
        total: Total number of items to process
        console: Rich console instance (optional)
        show_time_remaining: Whether to show time remaining estimate

    Yields:
        Tuple of (Progress instance, TaskID) for updating progress

    Example:
        ```python
        from src.helpers.progress import track_progress

        items = range(1000)
        with track_progress("Processing items", total=len(items)) as (progress, task):
            for item in items:
                # Process item
                progress.update(task, advance=1)
        ```
    """
    if show_time_remaining:
        progress = create_standard_progress(console)
    else:
        progress = create_simple_progress(console)

    with progress:
        task_id = progress.add_task(description, total=total)
        yield progress, task_id


def track_batches(
    progress: Progress,
    task_id: TaskID,
    batch_num: int,
    total_batches: int,
    items_processed: int,
    base_description: str,
) -> None:
    """Update progress display for batch processing.

    This helper standardizes progress updates for batch processing operations.

    Args:
        progress: Progress instance
        task_id: Task ID to update
        batch_num: Current batch number (1-indexed)
        total_batches: Total number of batches
        items_processed: Number of items processed in this batch
        base_description: Base description for the task

    Example:
        ```python
        from src.helpers.progress import create_standard_progress, track_batches

        progress = create_standard_progress()
        with progress:
            task_id = progress.add_task("Processing", total=total_items)

            for batch_num, batch in enumerate(batches, start=1):
                # Process batch
                track_batches(
                    progress, task_id, batch_num, len(batches),
                    len(batch), "Processing items"
                )
        ```
    """
    description = f"{base_description} [batch {batch_num}/{total_batches}]"
    progress.update(task_id, advance=items_processed, description=description)


__all__ = [
    "TaskID",
    "create_simple_progress",
    "create_standard_progress",
    "track_batches",
    "track_progress",
]
