"""Tests for dashboard query loading utilities."""

import pytest

from src.dashboard.queries import load_all_queries, load_query


class TestLoadQuery:
    """Tests for load_query function."""

    def test_load_query_general_category(self) -> None:
        """Test loading a query from A_general category."""
        query = load_query("A_general", "1_mev_boost_market_share.sql")

        assert isinstance(query, str)
        assert len(query) > 0
        # Check it's a SQL query
        assert "SELECT" in query.upper() or "WITH" in query.upper()

    def test_load_query_relay_category(self) -> None:
        """Test loading a query from B_relay category."""
        query = load_query("B_relay", "1_relay_market_share.sql")

        assert isinstance(query, str)
        assert len(query) > 0

    def test_load_query_builder_category(self) -> None:
        """Test loading a query from C_builder category."""
        query = load_query("C_builder", "1_builder_market_share_number_of_blocks.sql")

        assert isinstance(query, str)
        assert len(query) > 0

    def test_load_query_value_category(self) -> None:
        """Test loading a query from D_value_and_profitability category."""
        query = load_query(
            "D_value_and_profitability", "1_total_value_distribution_percent.sql"
        )

        assert isinstance(query, str)
        assert len(query) > 0

    def test_load_query_nonexistent_file_raises(self) -> None:
        """Test that loading a nonexistent query file raises an error."""
        with pytest.raises(FileNotFoundError):
            load_query("A_general", "nonexistent_query.sql")

    def test_load_query_nonexistent_category_raises(self) -> None:
        """Test that loading from a nonexistent category raises an error."""
        with pytest.raises(FileNotFoundError):
            load_query("Z_nonexistent", "some_query.sql")


class TestLoadAllQueries:
    """Tests for load_all_queries function."""

    def test_load_all_queries_returns_dict(self) -> None:
        """Test that load_all_queries returns a dictionary."""
        queries = load_all_queries()

        assert isinstance(queries, dict)

    def test_load_all_queries_has_expected_keys(self) -> None:
        """Test that load_all_queries returns expected query keys."""
        queries = load_all_queries()

        # Check for expected categories in keys
        categories = ["A_general", "B_relay", "C_builder", "D_value_and_profitability"]
        for category in categories:
            # At least one key should contain this category
            assert any(category in key for key in queries), (
                f"No queries found for category {category}"
            )

    def test_load_all_queries_has_general_queries(self) -> None:
        """Test that general queries are loaded."""
        queries = load_all_queries()

        # Check for specific general query
        assert any(
            "A_general" in key and "mev_boost_market_share" in key for key in queries
        )

    def test_load_all_queries_values_are_strings(self) -> None:
        """Test that all query values are non-empty strings."""
        queries = load_all_queries()

        for key, value in queries.items():
            assert isinstance(value, str), f"Query {key} should be a string"
            assert len(value) > 0, f"Query {key} should not be empty"

    def test_load_all_queries_values_are_sql(self) -> None:
        """Test that all query values appear to be SQL queries."""
        queries = load_all_queries()

        for key, value in queries.items():
            # Basic check that it looks like SQL
            assert any(
                keyword in value.upper() for keyword in ["SELECT", "WITH", "INSERT"]
            ), f"Query {key} doesn't appear to be SQL"

    def test_load_all_queries_keys_have_category_prefix(self) -> None:
        """Test that query keys have category prefix."""
        queries = load_all_queries()

        valid_prefixes = [
            "A_general",
            "B_relay",
            "C_builder",
            "D_value_and_profitability",
        ]

        for key in queries:
            assert any(key.startswith(prefix) for prefix in valid_prefixes), (
                f"Query key {key} doesn't start with valid category prefix"
            )

    def test_load_all_queries_has_multiple_queries(self) -> None:
        """Test that multiple queries are loaded."""
        queries = load_all_queries()

        # Should have at least 10 queries based on the dashboard structure
        assert len(queries) >= 10

    def test_load_all_queries_ignores_non_sql_files(self) -> None:
        """Test that non-SQL files are ignored."""
        queries = load_all_queries()

        # All keys should correspond to .sql files
        # dashboard.json should not be included
        assert not any("dashboard.json" in key for key in queries)

    def test_load_all_queries_includes_all_categories(self) -> None:
        """Test that queries from all categories are loaded."""
        queries = load_all_queries()

        # Count queries per category
        category_counts = {
            "A_general": 0,
            "B_relay": 0,
            "C_builder": 0,
            "D_value_and_profitability": 0,
        }

        for key in queries:
            for category in category_counts:
                if key.startswith(category):
                    category_counts[category] += 1

        # Each category should have at least one query
        for category, count in category_counts.items():
            assert count > 0, f"Category {category} has no queries loaded"

    def test_load_all_queries_key_format(self) -> None:
        """Test that query keys follow the expected format."""
        queries = load_all_queries()

        for key in queries:
            # Key should be category_filename (without .sql)
            parts = key.split("_", 1)
            assert len(parts) >= 2, f"Query key {key} doesn't follow expected format"

            # First part should be a category letter
            assert parts[0] in {
                "A",
                "B",
                "C",
                "D",
            }, f"Query key {key} doesn't start with valid category letter"
