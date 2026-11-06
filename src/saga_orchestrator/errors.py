"""Typed errors raised by the saga orchestrator."""

from __future__ import annotations

from typing import Literal

ErrorCode = Literal[
    "invalid_definition",
    "already_exists",
    "not_found",
    "invalid_context",
    "context_too_large",
    "invalid_id",
    "store_full",
    "corrupt_record",
]

_MAX_MESSAGE = 200


class SagaError(Exception):
    """Base error of the library.

    Attributes:
        code: Stable machine-readable error code.
        message: Human-readable description; it never contains context values.
    """

    def __init__(self, code: ErrorCode, message: str) -> None:
        text = message[:_MAX_MESSAGE]
        super().__init__(text)
        self.code: ErrorCode = code
        self.message: str = text
