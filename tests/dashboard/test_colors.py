"""Tests for color utility functions."""

from src.dashboard.colors import (
    get_builder_color_overrides,
    get_combined_overrides,
    get_relay_color_overrides,
    get_special_color_overrides,
)


def test_get_combined_overrides_empty() -> None:
    """Test get_combined_overrides with no lists."""
    result = get_combined_overrides()
    assert result == []


def test_get_combined_overrides_single_list() -> None:
    """Test get_combined_overrides with a single list."""
    overrides = [{"matcher": {"id": "byName"}, "properties": []}]
    result = get_combined_overrides(overrides)
    assert result == overrides


def test_get_combined_overrides_multiple_lists() -> None:
    """Test get_combined_overrides with multiple lists."""
    list1 = [{"matcher": {"id": "byName", "options": "test1"}, "properties": []}]
    list2 = [{"matcher": {"id": "byName", "options": "test2"}, "properties": []}]
    list3 = [{"matcher": {"id": "byName", "options": "test3"}, "properties": []}]

    result = get_combined_overrides(list1, list2, list3)

    assert len(result) == 3
    assert result[0] == list1[0]
    assert result[1] == list2[0]
    assert result[2] == list3[0]


def test_get_combined_overrides_with_actual_overrides() -> None:
    """Test get_combined_overrides with real override functions."""
    relay_overrides = get_relay_color_overrides()
    builder_overrides = get_builder_color_overrides()
    special_overrides = get_special_color_overrides()

    combined = get_combined_overrides(
        relay_overrides, builder_overrides, special_overrides
    )

    # Should equal sum of all override lists
    expected_length = (
        len(relay_overrides) + len(builder_overrides) + len(special_overrides)
    )
    assert len(combined) == expected_length

    # First items should be from relay_overrides
    assert combined[0] == relay_overrides[0]

    # Last items should be from special_overrides
    assert combined[-1] == special_overrides[-1]
