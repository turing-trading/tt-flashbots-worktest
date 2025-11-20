"""Tests for database helper functions."""

import pytest

from src.helpers.db import get_database_url


class TestGetDatabaseUrl:
    """Tests for get_database_url function."""

    def test_get_database_url_default_behavior(self) -> None:
        """Test get_database_url default behavior."""
        import os

        # Save current env
        saved_url = os.environ.get("DATABASE_URL")

        try:
            # Set test URL
            os.environ["DATABASE_URL"] = "postgresql://localhost/test"

            url = get_database_url()

            assert url is not None
            assert "postgresql" in url
        finally:
            # Restore env
            if saved_url:
                os.environ["DATABASE_URL"] = saved_url
            elif "DATABASE_URL" in os.environ:
                del os.environ["DATABASE_URL"]
