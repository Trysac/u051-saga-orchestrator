"""Immutable data model: steps, saga records and error information."""

from __future__ import annotations

import copy
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast, get_args

from .errors import SagaError

SagaStatus = Literal["running", "compensating", "completed", "compensated", "failed"]
StepState = Literal["pending", "running", "done", "compensating", "compensated", "skipped"]

SAGA_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,64}")
STEP_NAME_PATTERN = re.compile(r"[a-z0-9_]{1,64}")
TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "compensated", "failed"})
MAX_STEPS = 100
MAX_TEXT = 200

_STATUSES: frozenset[str] = frozenset(get_args(SagaStatus))
_STATES: frozenset[str] = frozenset(get_args(StepState))
_RECORD_FIELDS = (
    "saga_id",
    "status",
    "context",
    "steps",
    "error",
    "created_at",
    "updated_at",
)
_ERROR_FIELDS = ("step", "type", "message")


def _corrupt() -> SagaError:
    return SagaError("corrupt_record", "stored saga record is malformed")


def validate_saga_id(saga_id: object) -> str:
    """Return ``saga_id`` if it matches ``[A-Za-z0-9_-]{1,64}``.

    Raises:
        SagaError: with code ``invalid_id`` otherwise.
    """
    if not isinstance(saga_id, str) or SAGA_ID_PATTERN.fullmatch(saga_id) is None:
        raise SagaError("invalid_id", "saga id must match [A-Za-z0-9_-]{1,64}")
    return saga_id


def require_positive_int(value: object, name: str) -> int:
    """Return ``value`` if it is an ``int`` (not ``bool``) >= 1.

    Raises:
        SagaError: with code ``invalid_definition`` otherwise.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise SagaError("invalid_definition", f"{name} must be a positive integer")
    return value


@dataclass(frozen=True)
class Step:
    """One saga step.

    ``action`` receives a copy of the saga context and returns a dict that is merged
    into the context (or ``None``). ``compensate`` undoes a completed action and
    receives the context at failure time. Both must be idempotent. ``retries`` (0-10)
    extra attempts are made, waiting ``retry_delay`` seconds between attempts.
    """

    name: str
    action: Callable[[dict[str, Any]], dict[str, Any] | None]
    compensate: Callable[[dict[str, Any]], None] | None = None
    retries: int = 0
    retry_delay: float = 0.5


@dataclass(frozen=True)
class ErrorInfo:
    """Failure details: step name, exception type name and a truncated message."""

    step: str
    type: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Return a JSON-compatible dictionary."""
        return {"step": self.step, "type": self.type, "message": self.message}

    @classmethod
    def from_dict(cls, data: object) -> ErrorInfo:
        """Rebuild from :meth:`to_dict` output.

        Raises:
            SagaError: with code ``corrupt_record`` when ``data`` is malformed.
        """
        if not isinstance(data, Mapping) or set(data) != set(_ERROR_FIELDS):
            raise _corrupt()
        step, kind, message = data["step"], data["type"], data["message"]
        if not all(isinstance(value, str) for value in (step, kind, message)):
            raise _corrupt()
        return cls(step, kind, message[:MAX_TEXT])


def _parse_step(item: object) -> tuple[str, StepState]:
    if not isinstance(item, list | tuple) or len(item) != 2:
        raise _corrupt()
    name, state = item
    if not isinstance(name, str) or STEP_NAME_PATTERN.fullmatch(name) is None:
        raise _corrupt()
    if not isinstance(state, str) or state not in _STATES:
        raise _corrupt()
    return name, cast(StepState, state)


@dataclass(frozen=True)
class SagaRecord:
    """Persisted state of one saga.

    ``context`` holds only JSON types (``str``, ``int``, ``float``, ``bool``, ``None``,
    ``list`` and ``str``-keyed ``dict``). Timestamps are ISO 8601 UTC strings.
    """

    saga_id: str
    status: SagaStatus
    context: dict[str, Any]
    steps: tuple[tuple[str, StepState], ...]
    error: ErrorInfo | None
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible deep copy of the record."""
        return {
            "saga_id": self.saga_id,
            "status": self.status,
            "context": copy.deepcopy(self.context),
            "steps": [[name, state] for name, state in self.steps],
            "error": None if self.error is None else self.error.to_dict(),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SagaRecord:
        """Rebuild a record from :meth:`to_dict` output.

        Raises:
            SagaError: with code ``corrupt_record`` when ``data`` is malformed.
        """
        if not isinstance(data, Mapping) or set(data) != set(_RECORD_FIELDS):
            raise _corrupt()
        saga_id = data["saga_id"]
        status = data["status"]
        context = data["context"]
        raw_steps = data["steps"]
        created_at = data["created_at"]
        updated_at = data["updated_at"]
        if not isinstance(saga_id, str) or SAGA_ID_PATTERN.fullmatch(saga_id) is None:
            raise _corrupt()
        if not isinstance(status, str) or status not in _STATUSES:
            raise _corrupt()
        if not isinstance(context, dict):
            raise _corrupt()
        if not isinstance(created_at, str) or not isinstance(updated_at, str):
            raise _corrupt()
        if not isinstance(raw_steps, list | tuple) or not 1 <= len(raw_steps) <= MAX_STEPS:
            raise _corrupt()
        steps = tuple(_parse_step(item) for item in raw_steps)
        raw_error = data["error"]
        error = None if raw_error is None else ErrorInfo.from_dict(raw_error)
        return cls(
            saga_id=saga_id,
            status=cast(SagaStatus, status),
            context=copy.deepcopy(context),
            steps=steps,
            error=error,
            created_at=created_at,
            updated_at=updated_at,
        )
