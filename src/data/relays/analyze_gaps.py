"""Analyze relay data to detect gaps using daily statistics and outlier detection."""

import json
from asyncio import run
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.data.relays.gap_detection import (
    consolidate_gaps,
    date_to_slot_range,
    format_gap_summary,
)
from src.helpers.db import AsyncSessionLocal
from src.helpers.logging import get_logger


class GapAnalyzer:
    """Analyze relay data to detect gaps using statistical methods."""

    def __init__(
        self,
        output_file: str = "relay_gaps.json",
        consolidate: bool = True,
    ):
        """Initialize gap analyzer.

        Args:
            output_file: Path to save detected gaps JSON
            consolidate: Whether to consolidate adjacent gaps
        """
        self.output_file = output_file
        self.consolidate = consolidate
        self.logger = get_logger("gap_analyzer", log_level="INFO")
        self.console = Console()

    async def _run_coverage_query(self, session: AsyncSession) -> list[dict]:
        """Run the daily coverage statistics query.

        Returns:
            List of dictionaries with coverage statistics per relay per day
        """
        self.logger.info("Running daily coverage statistics query...")

        # Read the SQL query file
        query_path = (
            Path(__file__).parent.parent.parent
            / "analysis"
            / "queries"
            / "relay"
            / "daily_coverage_stats.sql"
        )

        with open(query_path) as f:
            sql = f.read()

        # Execute query
        result = await session.execute(text(sql))
        rows = result.fetchall()

        # Convert to list of dicts
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def _convert_outliers_to_gaps(self, outliers: list[dict]) -> list[dict]:
        """Convert outlier days to gap ranges (slot ranges).

        Args:
            outliers: List of outlier day records

        Returns:
            List of gap dictionaries with relay, from_slot, to_slot, dates
        """
        gaps = []

        for outlier in outliers:
            relay = outlier["relay"]
            date = outlier["date"]

            # Convert date to slot range
            date_obj = (
                datetime.strptime(str(date), "%Y-%m-%d")
                if isinstance(date, str)
                else date
            )
            from_slot, to_slot = date_to_slot_range(date_obj)

            gaps.append(
                {
                    "relay": relay,
                    "from_slot": from_slot,
                    "to_slot": to_slot,
                    "date": str(date),
                    "block_count": outlier["block_count"],
                    "expected_count": outlier["relay_avg"],
                    "missing_estimate": outlier["missing_estimate"],
                }
            )

        return gaps

    def _display_outliers_table(self, outliers: list[dict]) -> None:
        """Display outliers in a formatted table.

        Args:
            outliers: List of outlier records
        """
        if not outliers:
            self.console.print(
                "[green]No outliers detected - all relays have consistent coverage[/green]"
            )
            return

        table = Table(title="Detected Outlier Days (Low Coverage)")
        table.add_column("Relay", style="cyan")
        table.add_column("Date", style="magenta")
        table.add_column("Blocks", justify="right", style="yellow")
        table.add_column("Expected", justify="right", style="green")
        table.add_column("% of Avg", justify="right", style="red")
        table.add_column("Missing", justify="right", style="red")

        for outlier in outliers:
            table.add_row(
                outlier["relay"],
                str(outlier["date"]),
                str(outlier["block_count"]),
                f"{outlier['relay_avg']:.0f}",
                f"{outlier['pct_of_avg']:.1f}%",
                str(outlier["missing_estimate"]),
            )

        self.console.print(table)

    def _save_gaps_to_file(self, gaps: list[dict]) -> None:
        """Save gaps to JSON file.

        Args:
            gaps: List of gap dictionaries
        """
        from decimal import Decimal

        output_path = Path(self.output_file)

        # Convert any datetime/Decimal objects to JSON-serializable types
        gaps_serializable = []
        for gap in gaps:
            gap_copy = {}
            for key, value in gap.items():
                if isinstance(value, datetime):
                    gap_copy[key] = value.strftime("%Y-%m-%d")
                elif isinstance(value, Decimal):
                    gap_copy[key] = float(value)
                else:
                    gap_copy[key] = value
            gaps_serializable.append(gap_copy)

        with open(output_path, "w") as f:
            json.dump(gaps_serializable, f, indent=2)

        self.console.print(f"\n[green]✓ Gaps saved to {output_path}[/green]")

    async def analyze(self) -> list[dict]:
        """Analyze relay data and detect gaps.

        Returns:
            List of gap dictionaries
        """
        self.console.print("[bold blue]Relay Gap Analyzer[/bold blue]\n")

        async with AsyncSessionLocal() as session:
            # Run coverage query
            coverage_stats = await self._run_coverage_query(session)

            if not coverage_stats:
                self.console.print("[yellow]No coverage data found[/yellow]")
                return []

            self.logger.info(f"Analyzed {len(coverage_stats)} relay-day combinations")

            # Filter outliers
            outliers = [stat for stat in coverage_stats if stat["is_outlier"]]

            self.logger.info(f"Found {len(outliers)} outlier days")

            # Display outliers
            self._display_outliers_table(outliers)

            if not outliers:
                return []

            # Convert outliers to gaps
            gaps = self._convert_outliers_to_gaps(outliers)

            # Consolidate adjacent gaps if requested
            if self.consolidate:
                self.console.print("\n[cyan]Consolidating adjacent gaps...[/cyan]")
                original_count = len(gaps)
                gaps = consolidate_gaps(gaps, max_gap_slots=7200)  # 1 day
                self.logger.info(
                    f"Consolidated {original_count} gaps into {len(gaps)} ranges"
                )

            # Display gap summary
            self.console.print("\n[bold]Gap Summary:[/bold]")
            self.console.print(format_gap_summary(gaps))

            # Save to file
            self._save_gaps_to_file(gaps)

            return gaps


async def main():
    """Run gap analysis."""
    analyzer = GapAnalyzer(output_file="relay_gaps.json", consolidate=True)
    gaps = await analyzer.analyze()

    if gaps:
        console = Console()
        console.print(
            f"\n[bold green]✓ Analysis complete[/bold green] - "
            f"Found {len(gaps)} gap range(s)"
        )
        console.print("\n[cyan]Next steps:[/cyan]")
        console.print("1. Review relay_gaps.json")
        console.print("2. Run: poetry run python src/data/relays/retry_gaps.py")
    else:
        console = Console()
        console.print(
            "\n[green]✓ No gaps detected - all relays have consistent coverage[/green]"
        )


if __name__ == "__main__":
    run(main())
