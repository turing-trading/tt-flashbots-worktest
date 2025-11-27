"""Type definitions for HTTP responses."""

from typing import Any


# JSON value type - using Any for the recursive case
# since pyright has trouble with recursive type aliases
type JsonValue = str | int | float | bool | dict[str, Any] | list[Any] | None

# Type for JSON responses (can be object, array, or None for errors)
type JsonResponse = dict[str, Any] | list[Any] | None

__all__ = ["JsonResponse", "JsonValue"]
