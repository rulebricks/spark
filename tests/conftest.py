"""Shared pytest fixtures and utilities."""

import pytest


@pytest.fixture
def sample_rb_schema():
    """A representative Rulebricks response schema payload."""
    return [
        {"key": "priority", "name": "Priority", "type": "string"},
        {"key": "reportable", "name": "Reportable", "type": "boolean"},
        {"key": "risk_score", "name": "Risk Score", "type": "number"},
        {"key": "next_review", "name": "Next Review", "type": "date"},
        {"key": "tags", "name": "Tags", "type": "list"},
        {"key": "metadata", "name": "Metadata", "type": "object"},
    ]
