"""Utilities for loading SQL queries from files."""

from pathlib import Path
from typing import Dict


def load_query(category: str, query_file: str) -> str:
    """Load a SQL query from a file.

    Args:
        category: The category folder (A_general, B_relay, C_builder, D_value_and_profitability)
        query_file: The query filename (e.g., "1_mev_boost_market_share.sql")

    Returns:
        The SQL query string
    """
    query_path = Path(__file__).parent / "queries" / category / query_file
    with open(query_path, "r") as f:
        return f.read()


def load_all_queries() -> Dict[str, str]:
    """Load all queries and return them in a dictionary keyed by name."""
    queries_dir = Path(__file__).parent / "queries"
    queries = {}

    for category_dir in queries_dir.iterdir():
        if not category_dir.is_dir():
            continue

        for query_file in category_dir.glob("*.sql"):
            # Use the file stem (without .sql) as the key
            key = f"{category_dir.name}_{query_file.stem}"
            queries[key] = query_file.read_text()

    return queries
