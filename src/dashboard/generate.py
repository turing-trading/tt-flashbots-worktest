"""CLI script to generate Grafana dashboard JSON from grafanalib definition."""

import json
from pathlib import Path

from grafanalib._gen import DashboardEncoder

from src.dashboard.dashboard import generate_dashboard


def main():
    """Generate the dashboard JSON and save it to a file."""
    # Generate the dashboard
    dashboard = generate_dashboard()

    # Convert to JSON
    dashboard_json = json.dumps(
        dashboard.to_json_data(),
        indent=2,
        cls=DashboardEncoder,
        sort_keys=True,
    )

    # Determine output path
    output_path = Path(__file__).parent / "generated_dashboard.json"

    # Write to file
    output_path.write_text(dashboard_json)

    print(f"Dashboard JSON generated successfully: {output_path}")
    print(f"Size: {len(dashboard_json)} bytes")


if __name__ == "__main__":
    main()
