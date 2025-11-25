"""Tests for dashboard panel creation functions."""

from src.dashboard.panels import create_bar_chart


def test_create_bar_chart_with_axis_min() -> None:
    """Test create_bar_chart with axis_min parameter to cover line 325."""
    panel = create_bar_chart(
        title="Test Chart",
        description="Test Description",
        query="SELECT * FROM test",
        x=0,
        y=0,
        w=12,
        h=15,
        axis_min=0.0,
        axis_max=100.0,
        x_field="test_field",
    )

    # Verify the panel was created
    assert panel.title == "Test Chart"  # type: ignore[attr-defined]
    assert panel.description == "Test Description"  # type: ignore[attr-defined]

    # Verify axis_min was set in extraJson
    assert "fieldConfig" in panel.extraJson  # type: ignore[attr-defined]
    assert "defaults" in panel.extraJson["fieldConfig"]  # type: ignore[attr-defined]
    assert panel.extraJson["fieldConfig"]["defaults"]["min"] == 0.0  # type: ignore[attr-defined]
    assert panel.extraJson["fieldConfig"]["defaults"]["max"] == 100.0  # type: ignore[attr-defined]
