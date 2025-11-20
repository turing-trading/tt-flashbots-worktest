"""Tests for progress bar utilities."""

from io import StringIO

import pytest
from rich.console import Console
from rich.progress import Progress

from src.helpers.progress import (
    create_simple_progress,
    create_standard_progress,
    track_batches,
    track_progress,
)


class TestCreateStandardProgress:
    """Tests for create_standard_progress function."""

    def test_creates_progress_instance(self) -> None:
        """Test that function creates Progress instance."""
        progress = create_standard_progress()
        assert isinstance(progress, Progress)

    def test_uses_provided_console(self) -> None:
        """Test that function uses provided console."""
        console = Console()
        progress = create_standard_progress(console=console)
        assert progress.console == console

    def test_expand_parameter(self) -> None:
        """Test expand parameter is applied."""
        progress = create_standard_progress(expand=True)
        assert progress.expand is True

    def test_has_time_remaining_column(self) -> None:
        """Test that progress has time remaining column."""
        progress = create_standard_progress()
        # Check that columns include time remaining
        column_types = [type(col).__name__ for col in progress.columns]
        assert "TimeRemainingColumn" in column_types

    def test_has_time_elapsed_column(self) -> None:
        """Test that progress has time elapsed column."""
        progress = create_standard_progress()
        column_types = [type(col).__name__ for col in progress.columns]
        assert "TimeElapsedColumn" in column_types


class TestCreateSimpleProgress:
    """Tests for create_simple_progress function."""

    def test_creates_progress_instance(self) -> None:
        """Test that function creates Progress instance."""
        progress = create_simple_progress()
        assert isinstance(progress, Progress)

    def test_uses_provided_console(self) -> None:
        """Test that function uses provided console."""
        console = Console()
        progress = create_simple_progress(console=console)
        assert progress.console == console

    def test_expand_parameter(self) -> None:
        """Test expand parameter is applied."""
        progress = create_simple_progress(expand=True)
        assert progress.expand is True

    def test_no_time_remaining_column(self) -> None:
        """Test that progress does not have time remaining column."""
        progress = create_simple_progress()
        column_types = [type(col).__name__ for col in progress.columns]
        assert "TimeRemainingColumn" not in column_types

    def test_has_time_elapsed_column(self) -> None:
        """Test that progress has time elapsed column."""
        progress = create_simple_progress()
        column_types = [type(col).__name__ for col in progress.columns]
        assert "TimeElapsedColumn" in column_types


class TestTrackProgress:
    """Tests for track_progress context manager."""

    def test_yields_progress_and_task_id(self) -> None:
        """Test context manager yields progress and task ID."""
        with track_progress("Test task", total=100) as (progress, task_id):
            assert isinstance(progress, Progress)
            assert isinstance(task_id, int)

    def test_creates_task_with_description(self) -> None:
        """Test task is created with correct description."""
        with track_progress("Processing items", total=50) as (progress, task_id):
            task = progress.tasks[task_id]
            assert task.description == "Processing items"
            assert task.total == 50

    def test_show_time_remaining_true(self) -> None:
        """Test creates standard progress when show_time_remaining=True."""
        with track_progress("Test", total=100, show_time_remaining=True) as (
            progress,
            _,
        ):
            column_types = [type(col).__name__ for col in progress.columns]
            assert "TimeRemainingColumn" in column_types

    def test_show_time_remaining_false(self) -> None:
        """Test creates simple progress when show_time_remaining=False."""
        with track_progress("Test", total=100, show_time_remaining=False) as (
            progress,
            _,
        ):
            column_types = [type(col).__name__ for col in progress.columns]
            assert "TimeRemainingColumn" not in column_types

    def test_uses_provided_console(self) -> None:
        """Test uses provided console instance."""
        console = Console()
        with track_progress("Test", total=100, console=console) as (progress, _):
            assert progress.console == console

    def test_allows_progress_updates(self) -> None:
        """Test progress can be updated within context."""
        with track_progress("Test", total=100) as (progress, task_id):
            progress.update(task_id, advance=10)
            task = progress.tasks[task_id]
            assert task.completed == 10

            progress.update(task_id, advance=20)
            task = progress.tasks[task_id]
            assert task.completed == 30

    def test_cleans_up_after_context(self) -> None:
        """Test progress is properly cleaned up after context."""
        console = Console(file=StringIO())
        with track_progress("Test", total=100, console=console) as (progress, task_id):
            progress.update(task_id, advance=50)

        # Progress should be finished after context exits
        assert not progress.live.is_started


class TestTrackBatches:
    """Tests for track_batches helper function."""

    def test_updates_progress_with_batch_info(self) -> None:
        """Test function updates progress with batch information."""
        progress = create_standard_progress()
        with progress:
            task_id = progress.add_task("Processing", total=1000)

            track_batches(
                progress=progress,
                task_id=task_id,
                batch_num=1,
                total_batches=10,
                items_processed=100,
                base_description="Processing items",
            )

            task = progress.tasks[task_id]
            assert task.completed == 100
            assert "[batch 1/10]" in task.description
            assert "Processing items" in task.description

    def test_tracks_multiple_batches(self) -> None:
        """Test function tracks multiple batch updates."""
        progress = create_standard_progress()
        with progress:
            task_id = progress.add_task("Processing", total=1000)

            # First batch
            track_batches(progress, task_id, 1, 5, 200, "Loading data")
            task = progress.tasks[task_id]
            assert task.completed == 200
            assert "[batch 1/5]" in task.description

            # Second batch
            track_batches(progress, task_id, 2, 5, 200, "Loading data")
            task = progress.tasks[task_id]
            assert task.completed == 400
            assert "[batch 2/5]" in task.description

            # Third batch
            track_batches(progress, task_id, 3, 5, 200, "Loading data")
            task = progress.tasks[task_id]
            assert task.completed == 600
            assert "[batch 3/5]" in task.description

    def test_formats_batch_description_correctly(self) -> None:
        """Test batch description formatting."""
        progress = create_standard_progress()
        with progress:
            task_id = progress.add_task("Test", total=100)

            track_batches(
                progress,
                task_id,
                batch_num=7,
                total_batches=20,
                items_processed=5,
                base_description="Downloading files",
            )

            task = progress.tasks[task_id]
            expected_desc = "Downloading files [batch 7/20]"
            assert task.description == expected_desc

    def test_handles_single_batch(self) -> None:
        """Test function handles single batch (1/1) correctly."""
        progress = create_standard_progress()
        with progress:
            task_id = progress.add_task("Test", total=100)

            track_batches(progress, task_id, 1, 1, 100, "Single batch operation")

            task = progress.tasks[task_id]
            assert task.completed == 100
            assert "[batch 1/1]" in task.description


class TestProgressIntegration:
    """Integration tests for progress utilities."""

    def test_complete_workflow(self) -> None:
        """Test complete progress tracking workflow."""
        console = Console(file=StringIO())
        items = list(range(100))
        batch_size = 20
        batches = [items[i : i + batch_size] for i in range(0, len(items), batch_size)]

        with track_progress(
            "Processing items", total=len(items), console=console
        ) as (progress, task_id):
            for batch_num, batch in enumerate(batches, start=1):
                # Process batch (simulated)
                processed = len(batch)

                # Update progress
                track_batches(
                    progress,
                    task_id,
                    batch_num,
                    len(batches),
                    processed,
                    "Processing items",
                )

            # Verify final state
            task = progress.tasks[task_id]
            assert task.completed == 100
            assert "[batch 5/5]" in task.description

    def test_standard_vs_simple_progress(self) -> None:
        """Test difference between standard and simple progress."""
        # Standard progress
        standard = create_standard_progress()
        standard_columns = [type(col).__name__ for col in standard.columns]

        # Simple progress
        simple = create_simple_progress()
        simple_columns = [type(col).__name__ for col in simple.columns]

        # Standard has more columns (time remaining)
        assert "TimeRemainingColumn" in standard_columns
        assert "TimeRemainingColumn" not in simple_columns

        # Both have time elapsed
        assert "TimeElapsedColumn" in standard_columns
        assert "TimeElapsedColumn" in simple_columns
