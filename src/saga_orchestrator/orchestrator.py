"""Orchestration saga engine: ordered steps, compensations, retries and resume.

Delivery is at-least-once: a step found ``running`` on resume is executed again, so
actions and compensations must be idempotent. Use one orchestrator per saga id at a
time; there is no locking.
"""

from __future__ import annotations

import copy
import functools
import json
import logging
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .errors import SagaError
from .models import (
    MAX_STEPS,
    MAX_TEXT,
    STEP_NAME_PATTERN,
    TERMINAL_STATUSES,
    ErrorInfo,
    SagaRecord,
    SagaStatus,
    Step,
    StepState,
    require_positive_int,
    validate_saga_id,
)
from .stores import SagaStore

_log = logging.getLogger(__name__)
_log.addHandler(logging.NullHandler())

EventCallback = Callable[[Mapping[str, str | int]], None]
DEFAULT_MAX_CONTEXT_BYTES = 1_048_576

_MAX_DEPTH = 64
_MAX_RETRIES = 10


def _definition_error(message: str) -> SagaError:
    return SagaError("invalid_definition", message)


def _check_structure(node: object, depth: int, path: set[int], budget: list[int]) -> None:
    budget[0] -= 1
    if budget[0] < 0:
        raise SagaError("context_too_large", "context exceeds the size limit")
    children: Sequence[object]
    if isinstance(node, dict):
        if not all(isinstance(key, str) for key in node):
            raise SagaError("invalid_context", "context keys must be strings")
        children = list(node.values())
    elif isinstance(node, list | tuple):
        children = node
    else:
        return
    if depth >= _MAX_DEPTH or id(node) in path:
        raise SagaError("invalid_context", "context is nested too deeply or circular")
    path.add(id(node))
    try:
        for child in children:
            _check_structure(child, depth + 1, path, budget)
    finally:
        path.discard(id(node))


def _encode_context(context: object, max_bytes: int) -> dict[str, Any]:
    """Validate ``context`` and return its JSON round-trip (tuples become lists)."""
    if not isinstance(context, dict):
        raise SagaError("invalid_context", "context must be a dict with string keys")
    _check_structure(context, 0, set(), [max_bytes])
    try:
        text = json.dumps(context, allow_nan=False, separators=(",", ":"), ensure_ascii=False)
        size = len(text.encode("utf-8"))
    except (TypeError, ValueError, RecursionError):
        raise SagaError("invalid_context", "context must be JSON serializable") from None
    if size > max_bytes:
        raise SagaError("context_too_large", "context exceeds the size limit")
    loaded: dict[str, Any] = json.loads(text)
    return loaded


def _validate_step(step: object, seen: set[str]) -> None:
    if not isinstance(step, Step):
        raise _definition_error("every step must be a Step")
    if not isinstance(step.name, str) or STEP_NAME_PATTERN.fullmatch(step.name) is None:
        raise _definition_error("step names must match [a-z0-9_]{1,64}")
    if step.name in seen:
        raise _definition_error("step names must be unique")
    seen.add(step.name)
    if not callable(step.action):
        raise _definition_error("step action must be callable")
    if step.compensate is not None and not callable(step.compensate):
        raise _definition_error("step compensate must be callable or None")
    retries = step.retries
    if isinstance(retries, bool) or not isinstance(retries, int):
        raise _definition_error("step retries must be an integer between 0 and 10")
    if not 0 <= retries <= _MAX_RETRIES:
        raise _definition_error("step retries must be an integer between 0 and 10")
    delay = step.retry_delay
    if isinstance(delay, bool) or not isinstance(delay, int | float):
        raise _definition_error("step retry_delay must be a non-negative number")
    if not math.isfinite(delay) or delay < 0:
        raise _definition_error("step retry_delay must be a non-negative number")


def _validate_steps(steps: object) -> tuple[Step, ...]:
    if not isinstance(steps, Sequence) or isinstance(steps, str | bytes):
        raise _definition_error("steps must be a sequence of Step")
    if not 1 <= len(steps) <= MAX_STEPS:
        raise _definition_error("a saga needs between 1 and 100 steps")
    seen: set[str] = set()
    for step in steps:
        _validate_step(step, seen)
    return tuple(steps)


def _error_info(step_name: str, exc: Exception) -> ErrorInfo:
    try:
        message = str(exc)
    except Exception:
        message = ""
    safe = message.encode("utf-8", "replace").decode("utf-8")
    return ErrorInfo(step_name, type(exc).__name__[:MAX_TEXT], safe[:MAX_TEXT])


@dataclass
class _Run:
    """Mutable working state of one saga execution."""

    saga_id: str
    created_at: str
    status: SagaStatus
    states: list[StepState]
    context: dict[str, Any]
    error: ErrorInfo | None


class SagaOrchestrator:
    """Runs ordered steps and compensates completed ones in reverse order on failure.

    Progress is saved before each step (``running``) and after it (``done``), so
    :meth:`resume` can continue after a crash. Because a step found ``running`` is
    executed again, actions and compensations must be idempotent (at-least-once).
    Only ``Exception`` is retried; ``BaseException`` propagates and leaves the stored
    record untouched. Use one orchestrator per saga id at a time (no locking).

    ``on_event`` receives mappings with exactly the keys ``saga_id``, ``kind``,
    ``step``, ``attempt`` and ``status`` (the saga status); it never receives context
    values or exception messages.
    """

    def __init__(
        self,
        steps: Sequence[Step],
        store: SagaStore,
        *,
        clock: Callable[[], datetime],
        sleep: Callable[[float], None],
        on_event: EventCallback | None = None,
        max_context_bytes: int = DEFAULT_MAX_CONTEXT_BYTES,
    ) -> None:
        self._steps = _validate_steps(steps)
        if not callable(clock) or not callable(sleep):
            raise _definition_error("clock and sleep must be callable")
        if on_event is not None and not callable(on_event):
            raise _definition_error("on_event must be callable or None")
        self._names = tuple(step.name for step in self._steps)
        self._store = store
        self._clock = clock
        self._sleep = sleep
        self._on_event = on_event
        self._max_context_bytes = require_positive_int(max_context_bytes, "max_context_bytes")

    def start(self, saga_id: str, context: dict[str, Any]) -> SagaRecord:
        """Validate the input, persist a new saga and run it to a stable outcome.

        Raises:
            SagaError: ``invalid_id``, ``invalid_context``, ``context_too_large``,
                ``already_exists``, ``store_full`` or ``invalid_definition`` (naive clock).
        """
        validate_saga_id(saga_id)
        normalized = _encode_context(context, self._max_context_bytes)
        if self._store.load(saga_id) is not None:
            raise SagaError("already_exists", "a saga with this id already exists")
        states: list[StepState] = ["pending" for _ in self._steps]
        run = _Run(
            saga_id=saga_id,
            created_at=self._now(),
            status="running",
            states=states,
            context=normalized,
            error=None,
        )
        self._persist(run)
        return self._forward(run)

    def resume(self, saga_id: str) -> SagaRecord:
        """Continue a stored saga; terminal records are returned untouched.

        Raises:
            SagaError: ``invalid_id``, ``not_found`` or ``invalid_definition`` when the
                stored step names differ from this orchestrator's steps.
        """
        validate_saga_id(saga_id)
        record = self._store.load(saga_id)
        if record is None:
            raise SagaError("not_found", "saga not found")
        if record.status in TERMINAL_STATUSES:
            return record
        if tuple(name for name, _ in record.steps) != self._names:
            raise _definition_error("stored saga steps do not match the definition")
        run = _Run(
            saga_id=record.saga_id,
            created_at=record.created_at,
            status=record.status,
            states=[state for _, state in record.steps],
            context=record.context,
            error=record.error,
        )
        return self._forward(run) if run.status == "running" else self._compensate(run)

    def status(self, saga_id: str) -> SagaRecord:
        """Return the stored record of ``saga_id``.

        Raises:
            SagaError: ``invalid_id`` or ``not_found``.
        """
        validate_saga_id(saga_id)
        record = self._store.load(saga_id)
        if record is None:
            raise SagaError("not_found", "saga not found")
        return record

    def _now(self) -> str:
        value = self._clock()
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise _definition_error("clock must return timezone-aware datetimes")
        stamp = value.astimezone(timezone.utc).isoformat(timespec="microseconds")
        return stamp.replace("+00:00", "Z")

    def _persist(self, run: _Run) -> SagaRecord:
        record = SagaRecord(
            saga_id=run.saga_id,
            status=run.status,
            context=run.context,
            steps=tuple(zip(self._names, run.states, strict=True)),
            error=run.error,
            created_at=run.created_at,
            updated_at=self._now(),
        )
        self._store.save(record)
        return record

    def _emit(self, run: _Run, kind: str, step: str, attempt: int) -> None:
        if self._on_event is None:
            return
        payload: dict[str, str | int] = {
            "saga_id": run.saga_id,
            "kind": kind,
            "step": step,
            "attempt": attempt,
            "status": run.status,
        }
        try:
            self._on_event(payload)
        except Exception:
            _log.warning("on_event callback failed: kind=%s", kind)

    def _finish(self, run: _Run, status: SagaStatus) -> SagaRecord:
        run.status = status
        record = self._persist(run)
        self._emit(run, "saga_finished", "", 0)
        return record

    def _apply(self, step: Step, run: _Run) -> None:
        result = step.action(copy.deepcopy(run.context))
        if result is None:
            return
        if not isinstance(result, dict):
            raise TypeError("step action must return a dict or None")
        run.context = _encode_context({**run.context, **result}, self._max_context_bytes)

    def _invoke(self, compensate: Callable[[dict[str, Any]], None], run: _Run) -> None:
        compensate(copy.deepcopy(run.context))

    def _attempts(
        self,
        run: _Run,
        step: Step,
        phase: str,
        call: Callable[[], None],
    ) -> Exception | None:
        last: Exception | None = None
        for attempt in range(1, step.retries + 2):
            self._emit(run, f"{phase}_started", step.name, attempt)
            try:
                call()
            except Exception as exc:
                last = exc
                self._emit(run, f"{phase}_failed", step.name, attempt)
                _log.warning(
                    "%s failed: step=%s attempt=%d error=%s",
                    phase,
                    step.name,
                    attempt,
                    type(exc).__name__,
                )
                if attempt <= step.retries:
                    self._sleep(step.retry_delay)
            else:
                self._emit(run, f"{phase}_succeeded", step.name, attempt)
                return None
        return last

    def _forward(self, run: _Run) -> SagaRecord:
        for index, step in enumerate(self._steps):
            if run.states[index] == "done":
                continue
            run.states[index] = "running"
            self._persist(run)
            call = functools.partial(self._apply, step, run)
            error = self._attempts(run, step, "step", call)
            if error is not None:
                run.states[index] = "skipped"
                run.status = "compensating"
                run.error = _error_info(step.name, error)
                self._persist(run)
                return self._compensate(run)
            run.states[index] = "done"
            self._persist(run)
        return self._finish(run, "completed")

    def _compensate(self, run: _Run) -> SagaRecord:
        for index in range(len(self._steps) - 1, -1, -1):
            if run.states[index] not in ("done", "compensating"):
                continue
            step = self._steps[index]
            compensate = step.compensate
            if compensate is None:
                run.states[index] = "skipped"
                self._persist(run)
                continue
            run.states[index] = "compensating"
            self._persist(run)
            call = functools.partial(self._invoke, compensate, run)
            error = self._attempts(run, step, "compensation", call)
            if error is not None:
                run.error = _error_info(step.name, error)
                return self._finish(run, "failed")
            run.states[index] = "compensated"
            self._persist(run)
        return self._finish(run, "compensated")
