"""CLI script to update Grafana dashboard via API.

This script generates the dashboard using grafanalib and pushes it to Grafana.
Supports both preview and production environments with confirmation prompts.
"""

import argparse
import json
import socket
import sys

from typing import Literal

from grafanalib._gen import DashboardEncoder  # noqa: PLC2701
import httpx
from httpx import AsyncHTTPTransport, Request, Response
from pydantic import BaseModel, Field

from src.dashboard.dashboard import generate_dashboard
from src.helpers.config import get_optional_env, get_required_env


class NameSolver:
    """Resolve hostnames to IP addresses."""

    def get(self, name: str) -> str:
        """Get the IP address for a hostname."""
        return socket.gethostbyname(name)

    def resolve(self, request: Request) -> Request:
        """Resolve the hostname to an IP address."""
        host = request.url.host
        ip = self.get(host)

        if ip:
            request.extensions["sni_hostname"] = host
            request.url = request.url.copy_with(host=ip)

        return request


class AsyncCustomHost(AsyncHTTPTransport):
    """Async HTTP transport that resolves hostnames to IP addresses."""

    def __init__(self, solver: NameSolver, *args: object, **kwargs: object) -> None:
        """Initialize the AsyncCustomHost transport."""
        self.solver = solver
        super().__init__(*args, **kwargs)  # type: ignore[call-arg]

    async def handle_async_request(self, request: Request) -> Response:
        """Handle an asynchronous request."""
        request = self.solver.resolve(request)
        return await super().handle_async_request(request)


class DashboardConfig(BaseModel):
    """Configuration for dashboard deployment."""

    title: str = Field(..., description="Dashboard title")
    uid: str = Field(..., description="Dashboard unique identifier")
    url: str = Field(..., description="Grafana instance URL")
    folder_id: int = Field(default=0, description="Grafana folder ID (0 for General)")


class GrafanaDashboardResponse(BaseModel):
    """Response from Grafana dashboard API."""

    id: int = Field(..., description="Dashboard numeric ID")
    uid: str = Field(..., description="Dashboard unique identifier")
    url: str = Field(..., description="Dashboard URL path")
    status: str = Field(..., description="Response status")
    version: int = Field(..., description="Dashboard version")
    slug: str = Field(..., description="Dashboard slug")


class GrafanaErrorResponse(BaseModel):
    """Error response from Grafana API."""

    message: str = Field(..., description="Error message")
    status: str = Field(default="error", description="Error status")


# Dashboard configurations
def get_preview_config() -> DashboardConfig:
    """Get preview dashboard configuration.

    Uses Private Dashboard folder for preview/development work.
    """
    # Private Dashboard folder for preview
    folder_id_str = (
        get_optional_env("GRAFANA_PREVIEW_FOLDER_ID")
        or get_optional_env("GRAFANA_FOLDER_ID", "0")
    )
    return DashboardConfig(
        title="Preview - MEV-Boost Relay (Thomas' Worktest)",
        uid="e46c6ca2-cd80-4811-955b-test",
        url=get_optional_env("GRAFANA_PREVIEW_URL") or get_required_env("GRAFANA_URL"),
        folder_id=int(folder_id_str) if folder_id_str else 0,
    )


def get_production_config() -> DashboardConfig:
    """Get production dashboard configuration.

    Uses Public Dashboard folder for production deployment.
    """
    # Public Dashboard folder for production
    folder_id_str = (
        get_optional_env("GRAFANA_PRODUCTION_FOLDER_ID")
        or get_optional_env("GRAFANA_FOLDER_ID", "0")
    )
    return DashboardConfig(
        title="MEV-Boost Relay (Thomas' Worktest)",
        uid="e46c6ca2-cd80-4811-955b-f4fcafc860af",
        url=get_required_env("GRAFANA_URL"),
        folder_id=int(folder_id_str) if folder_id_str else 0,
    )


def get_dashboard_payload(config: DashboardConfig) -> dict[str, str | dict | int]:
    """Generate dashboard JSON payload for Grafana API.

    Args:
        config: Dashboard configuration

    Returns:
        Dashboard payload ready for Grafana API
    """
    # Generate the dashboard using grafanalib
    dashboard = generate_dashboard()

    # Convert to JSON data structure
    dashboard_data = dashboard.to_json_data()

    # Override title and uid from config
    dashboard_data["title"] = config.title
    dashboard_data["uid"] = config.uid

    # Create the API payload
    return {
        "dashboard": dashboard_data,
        "folderId": config.folder_id,
        "overwrite": True,
        "message": "Updated via update_dashboard.py script",
    }


async def update_dashboard(
    config: DashboardConfig,
    api_key: str,
    *,
    dry_run: bool = False,
) -> GrafanaDashboardResponse | None:
    """Update dashboard in Grafana.

    Args:
        config: Dashboard configuration
        api_key: Grafana API key
        dry_run: If True, only print what would be done without making changes

    Returns:
        API response on success, None on failure

    Raises:
        httpx.HTTPStatusError: If API request fails
    """
    # Generate payload
    payload = get_dashboard_payload(config)

    if dry_run:
        print("\n=== DRY RUN MODE ===")
        print(f"Would update dashboard at: {config.url}")
        print(f"Dashboard title: {config.title}")
        print(f"Dashboard UID: {config.uid}")
        print(f"Folder ID: {config.folder_id}")
        print(f"\nPayload size: {len(json.dumps(payload, cls=DashboardEncoder))} bytes")
        return None

    # Serialize payload using DashboardEncoder
    payload_json = json.dumps(payload, cls=DashboardEncoder)

    # Make API request
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    api_url = f"{config.url}/api/dashboards/db"

    async with httpx.AsyncClient(
        timeout=30.0, transport=AsyncCustomHost(NameSolver())
    ) as client:
        try:
            response = await client.post(
                api_url,
                headers=headers,
                content=payload_json,
            )
            response.raise_for_status()

            # Parse response
            response_data = response.json()
            return GrafanaDashboardResponse(**response_data)

        except httpx.HTTPStatusError as e:
            print(f"\n❌ Error updating dashboard: {e}", file=sys.stderr)
            try:
                error_data = e.response.json()
                error = GrafanaErrorResponse(**error_data)
                print(f"Error message: {error.message}", file=sys.stderr)
            except Exception:
                print(f"Response body: {e.response.text}", file=sys.stderr)
            raise


def confirm_production_update(config: DashboardConfig) -> bool:
    """Prompt user to confirm production dashboard update.

    Args:
        config: Production dashboard configuration

    Returns:
        True if user confirms, False otherwise
    """
    print("\n" + "=" * 70)
    print("⚠️  PRODUCTION UPDATE CONFIRMATION")
    print("=" * 70)
    print("You are about to update the PRODUCTION dashboard:")
    print(f"  Title: {config.title}")
    print(f"  UID: {config.uid}")
    print(f"  URL: {config.url}")
    print("=" * 70)

    response = input("\nType 'yes' to confirm: ").strip().lower()
    return response == "yes"


async def main(
    environment: Literal["preview", "production"],
    *,
    dry_run: bool = False,
    skip_confirmation: bool = False,
) -> int:
    """Main entry point for dashboard update script.

    Args:
        environment: Target environment (preview or production)
        dry_run: If True, only print what would be done
        skip_confirmation: If True, skip production confirmation prompt

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Select configuration
    config = (
        get_preview_config() if environment == "preview" else get_production_config()
    )

    print(f"\n🚀 Updating {environment.upper()} dashboard...")
    print(f"Dashboard: {config.title}")
    print(f"UID: {config.uid}")

    # Confirm production updates
    if (
        environment == "production"
        and not skip_confirmation
        and not dry_run
        and not confirm_production_update(config)
    ):
        print("\n❌ Production update cancelled by user")
        return 1

    try:
        # Get Grafana API key
        api_key = get_required_env("GRAFANA_API_KEY")

        # Update dashboard
        result = await update_dashboard(config, api_key, dry_run=dry_run)

        if dry_run:
            print("\n✅ Dry run completed successfully")
            return 0

        if result:
            print("\n✅ Dashboard updated successfully!")
            print(f"Dashboard ID: {result.id}")
            print(f"Version: {result.version}")
            print(f"URL: {config.url}{result.url}")
            return 0

        return 1

    except ValueError as e:
        print(f"\n❌ Configuration error: {e}", file=sys.stderr)
        return 1
    except httpx.HTTPStatusError:
        # Error already printed by update_dashboard
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}", file=sys.stderr)
        return 1


def cli() -> None:
    """Command-line interface entry point."""
    parser = argparse.ArgumentParser(
        description="Update Grafana dashboard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update preview dashboard
  python -m src.dashboard.update_dashboard preview

  # Update production dashboard (with confirmation)
  python -m src.dashboard.update_dashboard production

  # Dry run mode
  python -m src.dashboard.update_dashboard preview --dry-run

  # Skip confirmation (use with caution!)
  python -m src.dashboard.update_dashboard production --skip-confirmation
        """,
    )

    parser.add_argument(
        "environment",
        choices=["preview", "production"],
        help="Target environment",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--skip-confirmation",
        action="store_true",
        help="Skip production confirmation prompt (use with caution!)",
    )

    args = parser.parse_args()

    # Run async main
    import asyncio

    exit_code = asyncio.run(
        main(
            args.environment,
            dry_run=args.dry_run,
            skip_confirmation=args.skip_confirmation,
        )
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    cli()
