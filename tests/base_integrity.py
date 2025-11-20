"""Base class for data integrity tests with common assertion methods."""

from typing import TYPE_CHECKING

from sqlalchemy import text


if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class IntegrityTestBase:
    """Base class providing common assertion methods for integrity tests.

    This class provides reusable methods for common integrity test patterns:
    - Checking for duplicate records
    - Validating data consistency
    - Checking foreign key integrity
    - Validating field formats

    Example:
        ```python
        from tests.base_integrity import IntegrityTestBase

        class TestMyTableIntegrity(IntegrityTestBase):
            async def test_no_duplicates(self, async_session):
                await self.assert_no_duplicates(
                    async_session,
                    table="my_table",
                    columns=["id", "key"],
                )
        ```
    """

    async def assert_no_duplicates(
        self,
        session: "AsyncSession",
        table: str,
        columns: list[str],
        max_violations: int = 10,
    ) -> None:
        """Assert that there are no duplicate records for given column combination.

        Args:
            session: Database session
            table: Table name to check
            columns: List of column names that should be unique together
            max_violations: Maximum number of violations to return (default: 10)

        Raises:
            AssertionError: If duplicate records are found
        """
        column_list = ", ".join(columns)
        query = f"""
            SELECT {column_list}, COUNT(*) as count
            FROM {table}
            GROUP BY {column_list}
            HAVING COUNT(*) > 1
            LIMIT :max_violations
        """
        result = await session.execute(text(query), {"max_violations": max_violations})
        duplicates = result.fetchall()

        assert len(duplicates) == 0, (
            f"Found {len(duplicates)} duplicate records in {table} "
            f"for columns {columns}: {duplicates}"
        )

    async def assert_foreign_key_integrity(
        self,
        session: "AsyncSession",
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
        max_violations: int = 10,
    ) -> None:
        """Assert that foreign key references are valid.

        Args:
            session: Database session
            child_table: Table with foreign key
            child_column: Foreign key column
            parent_table: Referenced table
            parent_column: Referenced column
            max_violations: Maximum number of violations to return (default: 10)

        Raises:
            AssertionError: If orphaned foreign key references are found
        """
        query = f"""
            SELECT c.{child_column}
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{child_column} = p.{parent_column}
            WHERE p.{parent_column} IS NULL
            LIMIT :max_violations
        """
        result = await session.execute(text(query), {"max_violations": max_violations})
        orphaned = result.fetchall()

        assert len(orphaned) == 0, (
            f"Found {len(orphaned)} orphaned foreign key references "
            f"in {child_table}.{child_column}: {orphaned}"
        )

    async def assert_field_format(
        self,
        session: "AsyncSession",
        table: str,
        column: str,
        pattern: str,
        max_violations: int = 10,
    ) -> None:
        """Assert that field values match expected format pattern.

        Args:
            session: Database session
            table: Table name
            column: Column name to validate
            pattern: SQL LIKE pattern or regex pattern
            max_violations: Maximum number of violations to return (default: 10)

        Raises:
            AssertionError: If invalid field formats are found
        """
        query = f"""
            SELECT {column}
            FROM {table}
            WHERE {column} NOT LIKE :pattern
            LIMIT :max_violations
        """
        result = await session.execute(
            text(query), {"pattern": pattern, "max_violations": max_violations}
        )
        invalid = result.fetchall()

        assert len(invalid) == 0, (
            f"Found {len(invalid)} invalid formats in {table}.{column}: {invalid}"
        )

    async def assert_non_negative(
        self,
        session: "AsyncSession",
        table: str,
        column: str,
        max_violations: int = 10,
    ) -> None:
        """Assert that numeric field values are non-negative.

        Args:
            session: Database session
            table: Table name
            column: Column name to validate
            max_violations: Maximum number of violations to return (default: 10)

        Raises:
            AssertionError: If negative values are found
        """
        query = f"""
            SELECT {column}
            FROM {table}
            WHERE {column} < 0
            LIMIT :max_violations
        """
        result = await session.execute(text(query), {"max_violations": max_violations})
        negative = result.fetchall()

        assert len(negative) == 0, (
            f"Found {len(negative)} negative values in {table}.{column}: {negative}"
        )

    async def assert_field_consistency(
        self,
        session: "AsyncSession",
        table: str,
        field1: str,
        field2: str,
        comparison: str,
        max_violations: int = 10,
        error_message: str | None = None,
    ) -> None:
        """Assert that two fields maintain expected relationship.

        Args:
            session: Database session
            table: Table name
            field1: First field name
            field2: Second field name
            comparison: SQL comparison operator (e.g., '<=', '>=', '=')
            max_violations: Maximum number of violations to return (default: 10)
            error_message: Optional custom error message

        Raises:
            AssertionError: If field relationship is violated

        Example:
            ```python
            # Assert gas_used <= gas_limit
            await self.assert_field_consistency(
                session, "blocks", "gas_used", "gas_limit", "<=",
                error_message="gas_used cannot exceed gas_limit"
            )
            ```
        """
        query = f"""
            SELECT {field1}, {field2}
            FROM {table}
            WHERE NOT ({field1} {comparison} {field2})
            LIMIT :max_violations
        """
        result = await session.execute(text(query), {"max_violations": max_violations})
        violations = result.fetchall()

        msg = (
            error_message
            or f"Found {len(violations)} violations of {field1} {comparison} {field2} "
            f"in {table}: {violations}"
        )
        assert len(violations) == 0, msg

    async def get_count(
        self, session: "AsyncSession", table: str, where: str | None = None
    ) -> int:
        """Get row count from table with optional WHERE clause.

        Args:
            session: Database session
            table: Table name
            where: Optional WHERE clause (without 'WHERE' keyword)

        Returns:
            Row count
        """
        query = f"SELECT COUNT(*) FROM {table}"
        if where:
            query += f" WHERE {where}"

        result = await session.execute(text(query))
        return result.scalar() or 0

    async def get_distinct_count(
        self, session: "AsyncSession", table: str, column: str
    ) -> int:
        """Get count of distinct values in column.

        Args:
            session: Database session
            table: Table name
            column: Column name

        Returns:
            Distinct value count
        """
        query = f"SELECT COUNT(DISTINCT {column}) FROM {table}"
        result = await session.execute(text(query))
        return result.scalar() or 0


__all__ = ["IntegrityTestBase"]
