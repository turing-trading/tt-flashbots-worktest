"""Tests for HTTP models type definitions."""

import math

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from src.helpers.http_models import JsonResponse, JsonValue


def test_json_value_string() -> None:
    """Test JsonValue with string."""
    value: JsonValue = "test"
    assert value == "test"


def test_json_value_int() -> None:
    """Test JsonValue with int."""
    value: JsonValue = 42
    assert value == 42


def test_json_value_float() -> None:
    """Test JsonValue with float."""
    value: JsonValue = math.pi
    assert value == math.pi


def test_json_value_bool() -> None:
    """Test JsonValue with bool."""
    value: JsonValue = True
    assert value is True


def test_json_value_none() -> None:
    """Test JsonValue with None."""
    value: JsonValue = None
    assert value is None


def test_json_value_list() -> None:
    """Test JsonValue with list."""
    value: JsonValue = [1, 2, "three", True, None]
    assert value == [1, 2, "three", True, None]


def test_json_value_dict() -> None:
    """Test JsonValue with dict."""
    value: JsonValue = {"key": "value", "number": 42, "nested": {"foo": "bar"}}
    assert value == {"key": "value", "number": 42, "nested": {"foo": "bar"}}


def test_json_response_dict() -> None:
    """Test JsonResponse with dict."""
    response: JsonResponse = {"status": "success", "data": {"id": 1}}
    assert response == {"status": "success", "data": {"id": 1}}


def test_json_response_list() -> None:
    """Test JsonResponse with list."""
    response: JsonResponse = [{"id": 1}, {"id": 2}]
    assert response == [{"id": 1}, {"id": 2}]


def test_json_response_none() -> None:
    """Test JsonResponse with None."""
    response: JsonResponse = None
    assert response is None
