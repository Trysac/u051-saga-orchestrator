"""Behavior tests for the public API of saga_orchestrator."""

from __future__ import annotations

import pytest

import saga_orchestrator


def test_normalize_trims_and_lowercases() -> None:
    assert saga_orchestrator.normalize("  Hello World ") == "hello world"


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_normalize_rejects_blank_input(value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        saga_orchestrator.normalize(value)


def test_version_is_semver() -> None:
    parts = saga_orchestrator.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
