"""Type definitions for HTTP responses."""

# Recursive JSON value type without using Any
# This represents any valid JSON value
# Using PEP 695 type alias syntax to avoid recursion issues
type JsonValue = (
    str | int | float | bool | dict[str, JsonValue] | list[JsonValue] | None
)

# Type for JSON responses (can be object, array, or None for errors)
type JsonResponse = dict[str, JsonValue] | list[JsonValue] | None

__all__ = ["JsonResponse", "JsonValue"]
