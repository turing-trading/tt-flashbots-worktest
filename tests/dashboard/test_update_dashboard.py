"""Tests for update_dashboard script."""

from collections.abc import Generator
import os
from unittest.mock import MagicMock, patch

import pytest

from typing import TYPE_CHECKING, Never

import httpx
from httpx import Request, Response

from src.dashboard.update_dashboard import (
    AsyncCustomHost,
    DashboardConfig,
    GrafanaDashboardResponse,
    GrafanaErrorResponse,
    NameSolver,
    cli,
    confirm_production_update,
    get_dashboard_payload,
    get_preview_config,
    get_production_config,
    main,
    update_dashboard,
)


if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture


class TestNameSolver:
    """Tests for NameSolver class."""

    def test_get_returns_ip_address(self) -> None:
        """Test that get returns IP address for hostname."""
        solver = NameSolver()
        # Use localhost which should always resolve
        ip = solver.get("localhost")
        assert ip in {"127.0.0.1", "::1"}

    def test_resolve_updates_request_with_ip(self) -> None:
        """Test that resolve updates request with IP address."""
        solver = NameSolver()

        # Create a mock request with localhost
        request = Request("GET", "http://localhost:8080/test")

        # Resolve the request
        resolved = solver.resolve(request)

        # Check that SNI hostname was set
        assert "sni_hostname" in resolved.extensions  # type: ignore[attr-defined]
        assert resolved.extensions["sni_hostname"] == "localhost"

        # Check that host was changed to IP
        assert resolved.url.host in {"127.0.0.1", "::1"}  # type: ignore[attr-defined]


class TestAsyncCustomHost:
    """Tests for AsyncCustomHost class."""

    @pytest.mark.asyncio
    async def test_handle_async_request_calls_solver(self) -> None:
        """Test that handle_async_request uses solver."""
        solver = MagicMock(spec=NameSolver)
        solver.resolve = MagicMock(side_effect=lambda req: req)

        transport = AsyncCustomHost(solver)

        # Create a mock request
        request = Request("GET", "http://example.com/test")

        # Mock the parent's handle_async_request
        with patch.object(
            httpx.AsyncHTTPTransport,
            "handle_async_request",
            return_value=Response(200, json={"status": "ok"}),
        ):
            response = await transport.handle_async_request(request)

            # Verify solver was called
            solver.resolve.assert_called_once()
            assert response.status_code == 200


@pytest.fixture
def clean_grafana_env() -> Generator[None]:
    """Clean Grafana environment variables before and after test."""
    saved = {
        "GRAFANA_URL": os.environ.get("GRAFANA_URL"),
        "GRAFANA_PREVIEW_URL": os.environ.get("GRAFANA_PREVIEW_URL"),
        "GRAFANA_FOLDER_ID": os.environ.get("GRAFANA_FOLDER_ID"),
        "GRAFANA_API_KEY": os.environ.get("GRAFANA_API_KEY"),
    }

    # Clear all
    for key in saved:
        if key in os.environ:
            del os.environ[key]

    yield

    # Restore
    for key, value in saved.items():
        if value is not None:
            os.environ[key] = value
        elif key in os.environ:
            del os.environ[key]


@pytest.mark.usefixtures("clean_grafana_env")
class TestDashboardConfigs:
    """Tests for dashboard configuration functions."""

    def test_get_preview_config_uses_grafana_url_as_fallback(self) -> None:
        """Test preview config falls back to GRAFANA_URL."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"

        config = get_preview_config()

        assert config.title == "Preview - MEV-Boost Relay (Thomas' Worktest)"
        assert config.uid == "e46c6ca2-cd80-4811-955b-test"
        assert config.url == "https://grafana.example.com"
        assert config.folder_id == 0

    def test_get_preview_config_uses_preview_url_if_set(self) -> None:
        """Test preview config uses GRAFANA_PREVIEW_URL if set."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_PREVIEW_URL"] = "https://preview.grafana.example.com"

        config = get_preview_config()
        assert config.url == "https://preview.grafana.example.com"

    def test_get_preview_config_with_custom_folder_id(self) -> None:
        """Test preview config with custom folder ID."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_FOLDER_ID"] = "42"

        config = get_preview_config()
        assert config.folder_id == 42

    def test_get_production_config(self) -> None:
        """Test production config."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"

        config = get_production_config()

        assert config.title == "MEV-Boost Relay (Thomas' Worktest)"
        assert config.uid == "e46c6ca2-cd80-4811-955b-f4fcafc860af"
        assert config.url == "https://grafana.example.com"
        assert config.folder_id == 0

    def test_get_production_config_with_custom_folder_id(self) -> None:
        """Test production config with custom folder ID."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_FOLDER_ID"] = "100"

        config = get_production_config()
        assert config.folder_id == 100


class TestGetDashboardPayload:
    """Tests for get_dashboard_payload function."""

    def test_get_dashboard_payload_structure(self) -> None:
        """Test that get_dashboard_payload returns correct structure."""
        config = DashboardConfig(
            title="Test Dashboard",
            uid="test-uid-123",
            url="https://grafana.example.com",
            folder_id=5,
        )

        payload = get_dashboard_payload(config)

        # Check payload structure
        assert "dashboard" in payload
        assert "folderId" in payload
        assert "overwrite" in payload
        assert "message" in payload

        # Check values
        assert payload["folderId"] == 5
        assert payload["overwrite"] is True
        assert isinstance(payload["message"], str)
        assert "update_dashboard.py" in payload["message"]

        # Check dashboard data
        dashboard = payload["dashboard"]
        assert isinstance(dashboard, dict)
        assert dashboard["title"] == "Test Dashboard"
        assert dashboard["uid"] == "test-uid-123"


@pytest.mark.asyncio
class TestUpdateDashboard:
    """Tests for update_dashboard function."""

    async def test_update_dashboard_dry_run_mode(
        self, capsys: CaptureFixture[str]
    ) -> None:
        """Test update_dashboard in dry run mode."""
        config = DashboardConfig(
            title="Test",
            uid="test-123",
            url="https://grafana.example.com",
            folder_id=0,
        )

        result = await update_dashboard(config, "fake-api-key", dry_run=True)

        assert result is None

        # Check output
        captured = capsys.readouterr()
        assert "DRY RUN MODE" in captured.out
        assert "Test" in captured.out
        assert "test-123" in captured.out

    async def test_update_dashboard_success(self) -> None:
        """Test successful dashboard update."""
        config = DashboardConfig(
            title="Test",
            uid="test-123",
            url="https://grafana.example.com",
            folder_id=0,
        )

        # Mock successful response
        mock_response = {
            "id": 1,
            "uid": "test-123",
            "url": "/d/test-123/test",
            "status": "success",
            "version": 2,
            "slug": "test",
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value.__aenter__.return_value = mock_instance

            # Create sync mock for post response
            mock_post = MagicMock()
            mock_post.status_code = 200
            mock_post.json.return_value = mock_response
            mock_post.raise_for_status = MagicMock()

            # Make post method return awaitable
            async def async_post(*args: object, **kwargs: object) -> MagicMock:
                return mock_post

            mock_instance.post = async_post

            result = await update_dashboard(config, "test-api-key")

            assert result is not None
            assert result.id == 1
            assert result.uid == "test-123"
            assert result.version == 2

    async def test_update_dashboard_http_error(self) -> None:
        """Test dashboard update with HTTP error."""
        config = DashboardConfig(
            title="Test",
            uid="test-123",
            url="https://grafana.example.com",
            folder_id=0,
        )

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value.__aenter__.return_value = mock_instance

            # Create error response
            error_response = Response(
                400,
                json={"message": "Invalid dashboard", "status": "error"},
            )

            # Create sync mock for post response
            mock_post = MagicMock()

            def raise_error() -> Never:
                msg = "Bad request"
                raise httpx.HTTPStatusError(
                    msg, request=MagicMock(), response=error_response
                )

            mock_post.raise_for_status = raise_error

            # Make post method return awaitable
            async def async_post(*args: object, **kwargs: object) -> MagicMock:
                return mock_post

            mock_instance.post = async_post

            with pytest.raises(httpx.HTTPStatusError):
                await update_dashboard(config, "test-api-key")

    async def test_update_dashboard_http_error_unparseable_response(
        self, capsys: CaptureFixture[str]
    ) -> None:
        """Test dashboard update with HTTP error and unparseable response."""
        config = DashboardConfig(
            title="Test",
            uid="test-123",
            url="https://grafana.example.com",
            folder_id=0,
        )

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value.__aenter__.return_value = mock_instance

            # Create error response with plain text (not JSON)
            error_response = Response(500, text="Internal server error")

            # Create sync mock for post response
            mock_post = MagicMock()
            mock_post.text = "Internal server error"

            def raise_error() -> Never:
                msg = "Server error"
                raise httpx.HTTPStatusError(
                    msg, request=MagicMock(), response=error_response
                )

            mock_post.raise_for_status = raise_error

            # Make post method return awaitable
            async def async_post(*args: object, **kwargs: object) -> MagicMock:
                return mock_post

            mock_instance.post = async_post

            with pytest.raises(httpx.HTTPStatusError):
                await update_dashboard(config, "test-api-key")

            # Check stderr output
            captured = capsys.readouterr()
            assert "Error updating dashboard" in captured.err


@pytest.mark.usefixtures("clean_grafana_env")
class TestConfirmProductionUpdate:
    """Tests for confirm_production_update function."""

    def test_confirm_production_update_yes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test confirmation with 'yes' input."""
        config = DashboardConfig(
            title="Production Dashboard",
            uid="prod-123",
            url="https://grafana.example.com",
            folder_id=0,
        )

        # Mock user input
        monkeypatch.setattr("builtins.input", lambda _: "yes")

        result = confirm_production_update(config)
        assert result is True

    def test_confirm_production_update_no(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test confirmation with 'no' input."""
        config = DashboardConfig(
            title="Production Dashboard",
            uid="prod-123",
            url="https://grafana.example.com",
            folder_id=0,
        )

        # Mock user input
        monkeypatch.setattr("builtins.input", lambda _: "no")

        result = confirm_production_update(config)
        assert result is False

    def test_confirm_production_update_other_input(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test confirmation with other input (uppercase YES)."""
        config = DashboardConfig(
            title="Production Dashboard",
            uid="prod-123",
            url="https://grafana.example.com",
            folder_id=0,
        )

        # Mock user input - note that strip().lower() is applied, so "YES" becomes "yes"
        monkeypatch.setattr("builtins.input", lambda _: "YES")

        result = confirm_production_update(config)
        # Should be True because the function does .strip().lower()
        assert result is True


@pytest.mark.asyncio
@pytest.mark.usefixtures("clean_grafana_env")
class TestMain:
    """Tests for main function."""

    async def test_main_preview_dry_run(self, capsys: CaptureFixture[str]) -> None:
        """Test main with preview environment in dry run."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        exit_code = await main("preview", dry_run=True)

        assert exit_code == 0

        captured = capsys.readouterr()
        assert "PREVIEW" in captured.out
        assert "Dry run completed" in captured.out

    async def test_main_production_cancelled(
        self, monkeypatch: pytest.MonkeyPatch, capsys: CaptureFixture[str]
    ) -> None:
        """Test main with production when user cancels."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        # Mock user declining confirmation
        monkeypatch.setattr("builtins.input", lambda _: "no")

        exit_code = await main("production")

        assert exit_code == 1

        captured = capsys.readouterr()
        assert "cancelled" in captured.out

    async def test_main_production_skip_confirmation(self) -> None:
        """Test main with production and skip_confirmation."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        # Mock successful update
        with patch("src.dashboard.update_dashboard.update_dashboard") as mock_update:
            mock_update.return_value = GrafanaDashboardResponse(
                id=1,
                uid="test",
                url="/d/test",
                status="success",
                version=1,
                slug="test",
            )

            exit_code = await main("production", skip_confirmation=True)

            assert exit_code == 0

    async def test_main_missing_api_key(self, capsys: CaptureFixture[str]) -> None:
        """Test main with missing API key."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        # GRAFANA_API_KEY is not set

        exit_code = await main("preview", skip_confirmation=True)

        assert exit_code == 1

        captured = capsys.readouterr()
        assert "Configuration error" in captured.err

    async def test_main_http_error(self) -> None:
        """Test main with HTTP error."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        with patch("src.dashboard.update_dashboard.update_dashboard") as mock_update:
            error_response = Response(400, json={"error": "Bad request"})
            mock_update.side_effect = httpx.HTTPStatusError(
                "Error", request=MagicMock(), response=error_response
            )

            exit_code = await main("preview", skip_confirmation=True)

            assert exit_code == 1

    async def test_main_unexpected_error(self, capsys: CaptureFixture[str]) -> None:
        """Test main with unexpected error."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        with patch("src.dashboard.update_dashboard.update_dashboard") as mock_update:
            mock_update.side_effect = RuntimeError("Unexpected error")

            exit_code = await main("preview", skip_confirmation=True)

            assert exit_code == 1

            captured = capsys.readouterr()
            assert "Unexpected error" in captured.err

    async def test_main_update_returns_none(self) -> None:
        """Test main when update_dashboard returns None (should return 1)."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        with patch("src.dashboard.update_dashboard.update_dashboard") as mock_update:
            # Return None (e.g., in dry run mode without dry_run flag)
            mock_update.return_value = None

            exit_code = await main("preview", skip_confirmation=True)

            assert exit_code == 1


class TestModels:
    """Tests for Pydantic models."""

    def test_dashboard_config_model(self) -> None:
        """Test DashboardConfig model."""
        config = DashboardConfig(
            title="Test",
            uid="test-123",
            url="https://grafana.example.com",
            folder_id=5,
        )

        assert config.title == "Test"
        assert config.uid == "test-123"
        assert config.url == "https://grafana.example.com"
        assert config.folder_id == 5

    def test_grafana_dashboard_response_model(self) -> None:
        """Test GrafanaDashboardResponse model."""
        response = GrafanaDashboardResponse(
            id=1,
            uid="test",
            url="/d/test",
            status="success",
            version=2,
            slug="test-dashboard",
        )

        assert response.id == 1
        assert response.uid == "test"
        assert response.version == 2

    def test_grafana_error_response_model(self) -> None:
        """Test GrafanaErrorResponse model."""
        error = GrafanaErrorResponse(
            message="Invalid dashboard",
            status="error",
        )

        assert error.message == "Invalid dashboard"
        assert error.status == "error"

    def test_grafana_error_response_default_status(self) -> None:
        """Test GrafanaErrorResponse default status."""
        error = GrafanaErrorResponse(message="Error occurred")

        assert error.message == "Error occurred"
        assert error.status == "error"


@pytest.mark.usefixtures("clean_grafana_env")
class TestCLI:
    """Tests for CLI entry point."""

    def test_cli_preview_with_dry_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test CLI with preview and dry-run flags."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        # Mock sys.argv
        test_args = ["update_dashboard.py", "preview", "--dry-run"]
        monkeypatch.setattr("sys.argv", test_args)

        # Mock sys.exit to capture exit code
        exit_code = None

        def mock_exit(code: int) -> None:
            nonlocal exit_code
            exit_code = code

        monkeypatch.setattr("sys.exit", mock_exit)

        # Run CLI
        cli()

        # Should exit with 0
        assert exit_code == 0

    def test_cli_production_with_skip_confirmation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test CLI with production and skip-confirmation."""
        os.environ["GRAFANA_URL"] = "https://grafana.example.com"
        os.environ["GRAFANA_API_KEY"] = "test-key"

        # Mock sys.argv
        test_args = [
            "update_dashboard.py",
            "production",
            "--skip-confirmation",
            "--dry-run",
        ]
        monkeypatch.setattr("sys.argv", test_args)

        # Mock sys.exit
        exit_code: int | None = None

        def mock_exit(code: int) -> None:
            nonlocal exit_code
            exit_code = code

        monkeypatch.setattr("sys.exit", mock_exit)

        # Run CLI
        cli()

        # Should exit with 0
        assert exit_code == 0

    def test_cli_help_flag(
        self, monkeypatch: pytest.MonkeyPatch, capsys: CaptureFixture
    ) -> None:
        """Test CLI with --help flag."""
        test_args = ["update_dashboard.py", "--help"]
        monkeypatch.setattr("sys.argv", test_args)

        # --help should raise SystemExit(0)
        with pytest.raises(SystemExit) as exc_info:
            cli()

        assert exc_info.value.code == 0

        # Check help text in output
        captured = capsys.readouterr()
        assert "Update Grafana dashboard" in captured.out
        assert "preview" in captured.out
        assert "production" in captured.out
