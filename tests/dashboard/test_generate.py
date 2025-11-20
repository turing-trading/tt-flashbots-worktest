"""End-to-end test for dashboard generation."""

import pytest


def test_dashboard_generate_main():
    """Test that generate.py main() runs successfully."""
    from src.dashboard.generate import main

    # Run the main function - this is the e2e test
    # If this succeeds, the dashboard JSON is generated
    main()
