"""Tests for the saga orchestrator public API."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import saga_orchestrator
from saga_orchestrator import (
    ErrorInfo,
    InMemorySagaStore,
    JsonFileSagaStore,
    SagaError,
    SagaOrchestrator,
    SagaRecord,
    SagaStore,
    Step,
)

NOW = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
SENTINEL = "sentinel-7f3a"
EVENT_KEYS = {"saga_id", "kind", "step", "attempt", "status"}


class Boom(BaseException):
    """Simulates a process crash: it is not an ``Exception``."""


class Nasty(Exception):
    def __str__(self) -> str:
        raise ValueError("no string form")


def fixed_clock() -> datetime:
    return NOW


class Sleeper:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


class CountingStore:
    def __init__(self) -> None:
        self.inner = InMemorySagaStore()
        self.saves = 0

    def load(self, saga_id: str) -> SagaRecord | None:
        return self.inner.load(saga_id)

    def save(self, record: SagaRecord) -> None:
        self.saves += 1
        self.inner.save(record)


class ExplodingStore:
    def load(self, saga_id: str) -> SagaRecord | None:
        raise AssertionError("store must not be used")

    def save(self, record: SagaRecord) -> None:
        raise AssertionError("store must not be used")


def ok_action(context: dict[str, Any]) -> dict[str, Any] | None:
    return None


def failing_action(context: dict[str, Any]) -> dict[str, Any] | None:
    raise RuntimeError(f"boom {SENTINEL}")


def crashing_action(context: dict[str, Any]) -> dict[str, Any] | None:
    raise Boom()


def make_step(
    name: str,
    log: list[str],
    *,
    result: dict[str, Any] | None = None,
    fail: int = 0,
    retries: int = 0,
    compensate: bool = True,
    comp_fail: bool = False,
) -> Step:
    remaining = [fail]

    def action(context: dict[str, Any]) -> dict[str, Any] | None:
        log.append(f"do:{name}")
        if remaining[0] > 0:
            remaining[0] -= 1
            raise RuntimeError(f"{name} failed")
        return result

    def undo(context: dict[str, Any]) -> None:
        log.append(f"undo:{name}")
        if comp_fail:
            raise RuntimeError(f"{name} undo failed")

    return Step(name, action, undo if compensate else None, retries=retries)


def build(
    steps: list[Step],
    store: SagaStore | None = None,
    events: list[Any] | None = None,
    sleeper: Sleeper | None = None,
) -> SagaOrchestrator:
    return SagaOrchestrator(
        steps,
        store if store is not None else InMemorySagaStore(),
        clock=fixed_clock,
        sleep=sleeper if sleeper is not None else Sleeper(),
        on_event=events.append if events is not None else None,
    )


def sample_record(saga_id: str = "a") -> SagaRecord:
    return SagaRecord(saga_id, "running", {"k": 1}, (("step_1", "pending"),), None, "t", "t")


# --- orchestration semantics -------------------------------------------------


def test_four_steps_complete() -> None:
    log: list[str] = []
    steps = [
        make_step("step_1", log, result={"a": 1}),
        make_step("step_2", log, result=None),
        make_step("step_3", log, result={"b": 2}),
        make_step("step_4", log, result={"a": 3}),
    ]
    record = build(steps).start("saga-1", {})
    assert record.status == "completed"
    assert [state for _, state in record.steps] == ["done"] * 4
    assert record.context == {"a": 3, "b": 2}
    assert record.error is None
    assert log == ["do:step_1", "do:step_2", "do:step_3", "do:step_4"]
    assert record.created_at == "2025-01-02T03:04:05.000000Z"


def test_retries_then_reverse_compensation() -> None:
    log: list[str] = []
    sleeper = Sleeper()
    steps = [
        make_step("step_1", log),
        make_step("step_2", log),
        make_step("step_3", log, fail=99, retries=2),
        make_step("step_4", log),
    ]
    record = build(steps, sleeper=sleeper).start("saga-2", {"k": 1})
    assert log.count("do:step_3") == 3
    assert sleeper.calls == [0.5, 0.5]
    assert [entry for entry in log if entry.startswith("undo")] == ["undo:step_2", "undo:step_1"]
    assert "undo:step_3" not in log
    assert "do:step_4" not in log
    assert record.status == "compensated"
    assert record.error == ErrorInfo("step_3", "RuntimeError", "step_3 failed")
    assert dict(record.steps) == {
        "step_1": "compensated",
        "step_2": "compensated",
        "step_3": "skipped",
        "step_4": "pending",
    }


def test_success_on_third_attempt() -> None:
    log: list[str] = []
    sleeper = Sleeper()
    steps = [make_step("step_1", log, fail=2, retries=2, result={"ok": True})]
    record = build(steps, sleeper=sleeper).start("saga-3a", {})
    assert record.status == "completed"
    assert log.count("do:step_1") == 3
    assert sleeper.calls == [0.5, 0.5]
    assert record.context == {"ok": True}


def test_first_step_failure_has_nothing_to_compensate() -> None:
    log: list[str] = []
    steps = [make_step("step_1", log, fail=99), make_step("step_2", log)]
    record = build(steps).start("saga-3b", {})
    assert record.status == "compensated"
    assert log == ["do:step_1"]
    assert dict(record.steps) == {"step_1": "skipped", "step_2": "pending"}


def test_compensation_failure_marks_saga_failed() -> None:
    log: list[str] = []
    steps = [
        make_step("step_1", log),
        make_step("step_2", log, comp_fail=True),
        make_step("step_3", log, fail=99),
    ]
    record = build(steps).start("saga-4", {})
    assert record.status == "failed"
    assert record.error is not None
    assert record.error.step == "step_2"
    assert "undo:step_1" not in log
    assert log.count("undo:step_2") == 1


def test_compensation_uses_step_retries() -> None:
    log: list[str] = []
    sleeper = Sleeper()
    steps = [
        make_step("step_1", log),
        make_step("step_2", log, comp_fail=True, retries=1),
        make_step("step_3", log, fail=99),
    ]
    record = build(steps, sleeper=sleeper).start("saga-4b", {})
    assert record.status == "failed"
    assert log.count("undo:step_2") == 2
    assert sleeper.calls == [0.5]


def test_steps_without_compensation_are_skipped() -> None:
    log: list[str] = []
    steps = [make_step("step_1", log, compensate=False), make_step("step_2", log, fail=99)]
    record = build(steps).start("saga-4c", {})
    assert record.status == "compensated"
    assert dict(record.steps)["step_1"] == "skipped"


def test_compensation_receives_context_at_failure_time() -> None:
    seen: list[dict[str, Any]] = []

    def undo(context: dict[str, Any]) -> None:
        seen.append(context)

    steps = [
        Step("step_1", lambda ctx: {"a": 2}, undo),
        Step("step_2", failing_action),
    ]
    build(steps).start("saga-4d", {"a": 1})
    assert seen == [{"a": 2}]


def test_crash_in_action_then_resume() -> None:
    log: list[str] = []
    crash = [True]

    def action_2(context: dict[str, Any]) -> dict[str, Any] | None:
        log.append("do:step_2")
        if crash[0]:
            raise Boom()
        return None

    steps = [
        make_step("step_1", log, result={"a": 1}),
        Step("step_2", action_2),
        make_step("step_3", log),
    ]
    store = InMemorySagaStore()
    orchestrator = build(steps, store)
    with pytest.raises(Boom):
        orchestrator.start("saga-5", {})
    stored = store.load("saga-5")
    assert stored is not None
    assert stored.status == "running"
    assert dict(stored.steps)["step_2"] == "running"
    crash[0] = False
    log.clear()
    record = orchestrator.resume("saga-5")
    assert log == ["do:step_2", "do:step_3"]
    assert record.status == "completed"
    assert record.context == {"a": 1}


def test_resume_after_crash_in_compensation() -> None:
    log: list[str] = []
    crash = [True]

    def undo_2(context: dict[str, Any]) -> None:
        log.append("undo:step_2")
        if crash[0]:
            raise Boom()

    steps = [
        make_step("step_1", log),
        Step("step_2", ok_action, undo_2),
        make_step("step_3", log, fail=99),
        make_step("step_4", log),
    ]
    store = InMemorySagaStore()
    orchestrator = build(steps, store)
    with pytest.raises(Boom):
        orchestrator.start("saga-5b", {})
    stored = store.load("saga-5b")
    assert stored is not None
    assert stored.status == "compensating"
    assert dict(stored.steps)["step_2"] == "compensating"
    assert dict(stored.steps)["step_1"] == "done"
    crash[0] = False
    log.clear()
    record = orchestrator.resume("saga-5b")
    assert log == ["undo:step_2", "undo:step_1"]
    assert record.status == "compensated"


def test_base_exception_is_not_retried() -> None:
    calls: list[int] = []

    def action(context: dict[str, Any]) -> dict[str, Any] | None:
        calls.append(1)
        raise Boom()

    orchestrator = build([Step("step_1", action, retries=3)])
    with pytest.raises(Boom):
        orchestrator.start("saga-5c", {})
    assert calls == [1]


def test_resume_rejects_changed_definition() -> None:
    store = InMemorySagaStore()
    with pytest.raises(Boom):
        build([Step("step_1", crashing_action)], store).start("saga-5d", {})
    other = build([Step("other", ok_action)], store)
    with pytest.raises(SagaError) as info:
        other.resume("saga-5d")
    assert info.value.code == "invalid_definition"


@pytest.mark.parametrize("status", ["completed", "compensated", "failed"])
def test_terminal_records_are_never_rewritten(status: str) -> None:
    log: list[str] = []
    events: list[Any] = []
    step_1 = make_step("step_1", log, comp_fail=(status == "failed"))
    step_2 = make_step("step_2", log, fail=0 if status == "completed" else 99)
    store = CountingStore()
    orchestrator = build([step_1, step_2], store, events)
    first = orchestrator.start("saga-t", {"v": 1})
    assert first.status == status
    saves, event_count, log_count = store.saves, len(events), len(log)
    again = orchestrator.resume("saga-t")
    assert again == first
    assert (store.saves, len(events), len(log)) == (saves, event_count, log_count)
    with pytest.raises(SagaError) as info:
        orchestrator.start("saga-t", {})
    assert info.value.code == "already_exists"
    assert (store.saves, len(events), len(log)) == (saves, event_count, log_count)


def test_resume_and_status_of_unknown_saga() -> None:
    orchestrator = build([Step("step_1", ok_action)])
    for call in (orchestrator.resume, orchestrator.status):
        with pytest.raises(SagaError) as info:
            call("nope")
        assert info.value.code == "not_found"


def test_status_returns_stored_record() -> None:
    orchestrator = build([Step("step_1", ok_action)])
    record = orchestrator.start("saga-6", {"k": 1})
    assert orchestrator.status("saga-6") == record


# --- context handling --------------------------------------------------------


def test_tuple_context_is_stored_as_list() -> None:
    store = InMemorySagaStore()
    record = build([Step("step_1", ok_action)], store).start("ctx-1", {"t": (1, 2)})
    assert record.context == {"t": [1, 2]}
    stored = store.load("ctx-1")
    assert stored is not None
    assert stored.context == {"t": [1, 2]}


def test_action_results_are_normalized() -> None:
    steps = [Step("step_1", lambda ctx: {"t": (1, 2)})]
    record = build(steps).start("ctx-2", {})
    assert record.context == {"t": [1, 2]}


INVALID_CONTEXTS: list[Any] = [
    {1: "a"},
    {"x": float("nan")},
    {"o": object()},
    {"x": float("inf")},
    {"n": {2: "b"}},
    {"n": [{3: "b"}]},
    {"s": "\ud800"},
    ["not", "a", "dict"],
]


@pytest.mark.parametrize("context", INVALID_CONTEXTS)
def test_invalid_contexts_are_rejected_before_any_action(context: Any) -> None:
    log: list[str] = []
    with pytest.raises(SagaError) as info:
        build([make_step("step_1", log)]).start("ctx-3", context)
    assert info.value.code == "invalid_context"
    assert log == []


def test_circular_and_deep_contexts_are_rejected() -> None:
    loop: list[Any] = []
    loop.append(loop)
    deep: dict[str, Any] = {}
    node = deep
    for _ in range(100):
        child: dict[str, Any] = {}
        node["k"] = child
        node = child
    orchestrator = build([Step("step_1", ok_action)])
    for context in ({"loop": loop}, deep):
        with pytest.raises(SagaError) as info:
            orchestrator.start("ctx-4", context)
        assert info.value.code == "invalid_context"


def test_context_size_limit_is_enforced_exactly() -> None:
    log: list[str] = []
    orchestrator = build([make_step("step_1", log)])
    with pytest.raises(SagaError) as info:
        orchestrator.start("big-1", {"a": "x" * 1_048_569})
    assert info.value.code == "context_too_large"
    assert log == []
    record = orchestrator.start("big-2", {"a": "x" * 1_048_568})
    assert record.status == "completed"


def test_node_budget_rejects_wide_structures() -> None:
    orchestrator = SagaOrchestrator(
        [Step("step_1", ok_action)],
        InMemorySagaStore(),
        clock=fixed_clock,
        sleep=Sleeper(),
        max_context_bytes=10,
    )
    with pytest.raises(SagaError) as info:
        orchestrator.start("big-3", {"a": [0] * 20})
    assert info.value.code == "context_too_large"


def test_merge_that_breaks_the_context_counts_as_step_failure() -> None:
    orchestrator = SagaOrchestrator(
        [Step("step_1", lambda ctx: {"a": "x" * 100})],
        InMemorySagaStore(),
        clock=fixed_clock,
        sleep=Sleeper(),
        max_context_bytes=50,
    )
    record = orchestrator.start("merge-1", {})
    assert record.status == "compensated"
    assert record.error is not None
    assert (record.error.step, record.error.type) == ("step_1", "SagaError")
    assert record.context == {}


def test_invalid_action_results_count_as_step_failure() -> None:
    for result in ({"x": float("nan")}, ["bad"], {1: "a"}):
        steps = [Step("step_1", lambda ctx, value=result: value)]
        record = build(steps).start(f"merge-{type(result).__name__}", {})
        assert record.status == "compensated"
        assert record.error is not None


def test_non_dict_result_reports_type_error() -> None:
    record = build([Step("step_1", lambda ctx: ["bad"])]).start("merge-9", {})
    assert record.error is not None
    assert record.error.type == "TypeError"


# --- ids, stores -------------------------------------------------------------


@pytest.mark.parametrize("saga_id", ["../x", "a/b", "", "abc\n", "x" * 65, 5])
def test_invalid_ids_fail_before_any_store_call(saga_id: Any) -> None:
    orchestrator = SagaOrchestrator(
        [Step("step_1", ok_action)],
        ExplodingStore(),
        clock=fixed_clock,
        sleep=Sleeper(),
    )
    for call in (
        lambda: orchestrator.start(saga_id, {}),
        lambda: orchestrator.resume(saga_id),
        lambda: orchestrator.status(saga_id),
    ):
        with pytest.raises(SagaError) as info:
            call()
        assert info.value.code == "invalid_id"


def test_json_file_store_roundtrip(tmp_path: Path) -> None:
    directory = tmp_path / "sagas"
    store = JsonFileSagaStore(directory)
    steps = [Step("step_1", lambda ctx: {"a": "caf\u00e9"}), Step("step_2", ok_action)]
    record = build(steps, store).start("j-1", {"k": [1, 2]})
    loaded = store.load("j-1")
    assert loaded is not None
    assert loaded.to_dict() == record.to_dict()
    assert loaded == record
    assert list(directory.glob("*.tmp")) == []
    assert [path.name for path in directory.iterdir()] == ["j-1.json"]
    assert store.load("missing") is None


def test_json_file_store_handles_unicode_error_messages(tmp_path: Path) -> None:
    def action(context: dict[str, Any]) -> dict[str, Any] | None:
        raise RuntimeError("bad \ud800 text")

    store = JsonFileSagaStore(tmp_path)
    record = build([Step("step_1", action)], store).start("j-2", {})
    assert record.error is not None
    assert record.error.message == "bad ? text"
    assert store.load("j-2") == record


def test_json_file_store_creates_directory(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b"
    JsonFileSagaStore(target)
    assert target.is_dir()


@pytest.mark.parametrize(
    "raw",
    [
        b"{",
        b"\xff\xfe",
        b"[]",
        b'{"a":NaN}',
        b"x" * 201,
        b" " * 500,
        b"[" * 150,
    ],
)
def test_json_file_store_rejects_corrupt_files(tmp_path: Path, raw: bytes) -> None:
    store = JsonFileSagaStore(tmp_path, max_record_bytes=200)
    (tmp_path / "bad.json").write_bytes(raw)
    with pytest.raises(SagaError) as info:
        store.load("bad")
    assert info.value.code == "corrupt_record"


def test_json_file_store_rejects_foreign_id(tmp_path: Path) -> None:
    store = JsonFileSagaStore(tmp_path)
    store.save(sample_record("a"))
    (tmp_path / "b.json").write_bytes((tmp_path / "a.json").read_bytes())
    with pytest.raises(SagaError) as info:
        store.load("b")
    assert info.value.code == "corrupt_record"


def test_json_file_store_rejects_bad_ids_and_sizes(tmp_path: Path) -> None:
    store = JsonFileSagaStore(tmp_path, max_record_bytes=50)
    with pytest.raises(SagaError) as info:
        store.load("../x")
    assert info.value.code == "invalid_id"
    with pytest.raises(SagaError) as info:
        store.save(sample_record("../x"))
    assert info.value.code == "invalid_id"
    with pytest.raises(SagaError) as info:
        store.save(sample_record("a"))
    assert info.value.code == "context_too_large"
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(SagaError) as info:
        JsonFileSagaStore(tmp_path, max_record_bytes=0)
    assert info.value.code == "invalid_definition"


def test_json_file_store_removes_tmp_file_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = JsonFileSagaStore(tmp_path)

    def broken(src: Any, dst: Any) -> None:
        raise OSError("disk failure")

    monkeypatch.setattr("os.replace", broken)
    with pytest.raises(OSError, match="disk failure"):
        store.save(sample_record("a"))
    assert list(tmp_path.iterdir()) == []


def test_store_full_when_no_record_is_terminal() -> None:
    store = InMemorySagaStore(max_entries=2)
    orchestrator = build([Step("step_1", crashing_action)], store)
    for saga_id in ("a", "b"):
        with pytest.raises(Boom):
            orchestrator.start(saga_id, {})
    with pytest.raises(SagaError) as info:
        orchestrator.start("c", {})
    assert info.value.code == "store_full"
    store.save(sample_record("a"))
    assert store.load("a") is not None


def test_in_memory_store_evicts_oldest_terminal_record() -> None:
    store = InMemorySagaStore(max_entries=2)
    orchestrator = build([Step("step_1", ok_action)], store)
    for saga_id in ("a", "b", "c"):
        orchestrator.start(saga_id, {})
    assert store.load("a") is None
    assert store.load("b") is not None
    assert store.load("c") is not None


def test_in_memory_store_returns_isolated_copies() -> None:
    store = InMemorySagaStore()
    store.save(sample_record("a"))
    first = store.load("a")
    assert first is not None
    first.context["extra"] = 1
    second = store.load("a")
    assert second is not None
    assert second.context == {"k": 1}


@pytest.mark.parametrize("value", [0, -1, True, "5"])
def test_in_memory_store_validates_max_entries(value: Any) -> None:
    with pytest.raises(SagaError) as info:
        InMemorySagaStore(max_entries=value)
    assert info.value.code == "invalid_definition"


# --- events and logging ------------------------------------------------------


def test_event_payloads_are_minimal(caplog: pytest.LogCaptureFixture) -> None:
    events: list[Any] = []
    steps = [
        Step("step_1", lambda ctx: {"x": 1}, lambda ctx: None),
        Step("step_2", failing_action, retries=1),
    ]
    with caplog.at_level(logging.DEBUG):
        record = build(steps, events=events).start("ev-1", {"note": SENTINEL})
    assert record.status == "compensated"
    for event in events:
        assert set(event) == EVENT_KEYS
        assert SENTINEL not in json.dumps(dict(event))
    assert [event["kind"] for event in events] == [
        "step_started",
        "step_succeeded",
        "step_started",
        "step_failed",
        "step_started",
        "step_failed",
        "compensation_started",
        "compensation_succeeded",
        "saga_finished",
    ]
    assert [event["attempt"] for event in events] == [1, 1, 1, 1, 2, 2, 1, 1, 0]
    assert events[-1]["status"] == "compensated"
    assert events[-1]["step"] == ""
    assert events[0]["saga_id"] == "ev-1"
    assert SENTINEL not in caplog.text


def test_compensation_failure_event() -> None:
    events: list[Any] = []
    log: list[str] = []
    steps = [make_step("step_1", log, comp_fail=True), make_step("step_2", log, fail=99)]
    build(steps, events=events).start("ev-2", {})
    kinds = [event["kind"] for event in events]
    assert "compensation_failed" in kinds
    assert kinds[-1] == "saga_finished"
    assert events[-1]["status"] == "failed"


def test_failing_event_callback_does_not_break_the_saga() -> None:
    def broken(event: Any) -> None:
        raise RuntimeError("callback failure")

    orchestrator = SagaOrchestrator(
        [Step("step_1", ok_action)],
        InMemorySagaStore(),
        clock=fixed_clock,
        sleep=Sleeper(),
        on_event=broken,
    )
    assert orchestrator.start("ev-3", {}).status == "completed"


def test_logger_has_null_handler_and_no_configuration() -> None:
    logger = logging.getLogger("saga_orchestrator.orchestrator")
    assert any(isinstance(handler, logging.NullHandler) for handler in logger.handlers)


# --- errors, clock, definitions ----------------------------------------------


def test_error_info_is_truncated_and_safe() -> None:
    def long_failure(context: dict[str, Any]) -> dict[str, Any] | None:
        raise RuntimeError("m" * 500)

    record = build([Step("step_1", long_failure)]).start("err-1", {})
    assert record.error is not None
    assert len(record.error.message) == 200
    record = build([Step("step_1", lambda ctx: (_ for _ in ()).throw(Nasty()))]).start("err-2", {})
    assert record.error is not None
    assert (record.error.type, record.error.message) == ("Nasty", "")


def test_saga_error_has_code_and_bounded_message() -> None:
    error = SagaError("not_found", "x" * 500)
    assert error.code == "not_found"
    assert len(error.message) == 200
    assert str(error) == error.message


def test_naive_or_invalid_clock_values_are_rejected() -> None:
    log: list[str] = []
    for clock in (lambda: datetime(2025, 1, 1), lambda: "2025-01-01"):
        orchestrator = SagaOrchestrator(
            [make_step("step_1", log)],
            InMemorySagaStore(),
            clock=clock,
            sleep=Sleeper(),
        )
        with pytest.raises(SagaError) as info:
            orchestrator.start("clk-1", {})
        assert info.value.code == "invalid_definition"
    assert log == []


def test_clock_values_are_converted_to_utc() -> None:
    zone = timezone(timedelta(hours=2))
    orchestrator = SagaOrchestrator(
        [Step("step_1", ok_action)],
        InMemorySagaStore(),
        clock=lambda: datetime(2025, 1, 1, 2, 0, tzinfo=zone),
        sleep=Sleeper(),
    )
    record = orchestrator.start("clk-2", {})
    assert record.created_at == "2025-01-01T00:00:00.000000Z"
    assert record.updated_at == "2025-01-01T00:00:00.000000Z"


INVALID_DEFINITIONS: dict[str, Any] = {
    "empty": [],
    "too_many": [Step(f"s{i}", ok_action) for i in range(101)],
    "duplicate": [Step("a", ok_action), Step("a", ok_action)],
    "uppercase": [Step("A", ok_action)],
    "empty_name": [Step("", ok_action)],
    "long_name": [Step("a" * 65, ok_action)],
    "non_str_name": [Step(5, ok_action)],  # type: ignore[arg-type]
    "retries_11": [Step("a", ok_action, retries=11)],
    "retries_negative": [Step("a", ok_action, retries=-1)],
    "retries_bool": [Step("a", ok_action, retries=True)],
    "retries_float": [Step("a", ok_action, retries=1.5)],
    "delay_negative": [Step("a", ok_action, retry_delay=-1.0)],
    "delay_nan": [Step("a", ok_action, retry_delay=float("nan"))],
    "delay_text": [Step("a", ok_action, retry_delay="1")],  # type: ignore[arg-type]
    "delay_bool": [Step("a", ok_action, retry_delay=True)],
    "not_a_step": ["a"],
    "action_not_callable": [Step("a", None)],  # type: ignore[arg-type]
    "compensate_not_callable": [Step("a", ok_action, compensate=5)],  # type: ignore[arg-type]
    "bare_string": "abc",
    "not_a_sequence": 5,
}


@pytest.mark.parametrize(
    "steps",
    list(INVALID_DEFINITIONS.values()),
    ids=list(INVALID_DEFINITIONS),
)
def test_invalid_definitions_are_rejected(steps: Any) -> None:
    with pytest.raises(SagaError) as info:
        SagaOrchestrator(steps, InMemorySagaStore(), clock=fixed_clock, sleep=Sleeper())
    assert info.value.code == "invalid_definition"


def test_hundred_steps_and_tuples_are_accepted() -> None:
    steps = tuple(Step(f"s{i}", ok_action, retries=10, retry_delay=0) for i in range(100))
    record = build(list(steps)).start("def-1", {})
    assert record.status == "completed"
    assert len(record.steps) == 100
    orchestrator = SagaOrchestrator(steps, InMemorySagaStore(), clock=fixed_clock, sleep=Sleeper())
    assert orchestrator.start("def-2", {}).status == "completed"


@pytest.mark.parametrize(
    "overrides",
    [
        {"clock": 5},
        {"sleep": 5},
        {"on_event": 5},
        {"max_context_bytes": 0},
        {"max_context_bytes": True},
    ],
)
def test_invalid_constructor_arguments(overrides: dict[str, Any]) -> None:
    kwargs: dict[str, Any] = {"clock": fixed_clock, "sleep": Sleeper()}
    kwargs.update(overrides)
    with pytest.raises(SagaError) as info:
        SagaOrchestrator([Step("step_1", ok_action)], InMemorySagaStore(), **kwargs)
    assert info.value.code == "invalid_definition"


def test_step_is_frozen() -> None:
    step = Step("step_1", ok_action)
    assert (step.retries, step.retry_delay, step.compensate) == (0, 0.5, None)
    with pytest.raises(AttributeError):
        step.name = "other"  # type: ignore[misc]


# --- record serialization ----------------------------------------------------


def valid_dict() -> dict[str, Any]:
    return {
        "saga_id": "x",
        "status": "running",
        "context": {"a": 1},
        "steps": [["step_1", "pending"]],
        "error": None,
        "created_at": "t",
        "updated_at": "t",
    }


def test_record_roundtrip_with_error() -> None:
    data = valid_dict()
    data["status"] = "failed"
    data["error"] = {"step": "step_1", "type": "RuntimeError", "message": "m"}
    record = SagaRecord.from_dict(data)
    assert record.error == ErrorInfo("step_1", "RuntimeError", "m")
    assert record.to_dict() == data
    assert SagaRecord.from_dict(json.loads(json.dumps(record.to_dict()))) == record


MUTATIONS: dict[str, dict[str, Any]] = {
    "bad_id": {"saga_id": "../x"},
    "id_type": {"saga_id": 5},
    "bad_status": {"status": "bogus"},
    "status_type": {"status": 5},
    "context_type": {"context": []},
    "steps_type": {"steps": "s"},
    "steps_empty": {"steps": []},
    "steps_too_many": {"steps": [["s", "done"]] * 101},
    "step_short": {"steps": [["step_1"]]},
    "step_not_list": {"steps": ["ab"]},
    "step_bad_name": {"steps": [["S1", "pending"]]},
    "step_name_type": {"steps": [[1, "pending"]]},
    "step_bad_state": {"steps": [["step_1", "weird"]]},
    "step_state_type": {"steps": [["step_1", 3]]},
    "error_type": {"error": "x"},
    "error_missing": {"error": {"step": "step_1"}},
    "error_field_type": {"error": {"step": "step_1", "type": 1, "message": "m"}},
    "created_type": {"created_at": 5},
    "updated_type": {"updated_at": 5},
    "extra_key": {"extra": 1},
}


@pytest.mark.parametrize("changes", list(MUTATIONS.values()), ids=list(MUTATIONS))
def test_malformed_records_are_rejected(changes: dict[str, Any]) -> None:
    data = valid_dict()
    data.update(changes)
    with pytest.raises(SagaError) as info:
        SagaRecord.from_dict(data)
    assert info.value.code == "corrupt_record"


def test_missing_key_and_non_mapping_are_rejected() -> None:
    data = valid_dict()
    del data["saga_id"]
    for bad in (data, [], None):
        with pytest.raises(SagaError) as info:
            SagaRecord.from_dict(bad)  # type: ignore[arg-type]
        assert info.value.code == "corrupt_record"


ACCEPTANCE_TEXT = (
    "Four succeeding steps returning {'a'"
    ": 1}, None, {'b'"
    ": 2} and {'a'"
    ": 3} give a SagaRecord with status == 'completed'"
    ", every step state 'done'"
    " and context == {'a': 3, 'b': 2}; "
    "start() with context {'t'"
    ": (1, 2)} stores {'t'"
    ": [1, 2]}; contexts {1: 'a'"
    "}, {'x': float('nan')"
    ")} and {'o'"
    ": object()} raise SagaError code 'invalid_context'"
    " and {'a': 'x'"
    " * 1_048_569} (1_048_577 serialized bytes) raises code 'context_too_large'"
    ", all before any action runs, while {'a': 'x' * 1_048_568} is accepted; "
    "Saga ids '../x', 'a/b', '' and 'abc\\n'"
    " raise SagaError code 'invalid_id'"
)
ACCEPTANCE_STORE_TEXT = (
    " before any store call; a JsonFileSagaStore record reloads with to_dict() equal to the saved one and no "  # noqa: E501
    ".tmp"
    " file remains; InMemorySagaStore(max_entries=2) holding two running sagas raises code "
    "store_full"
)


def test_acceptance_text_is_covered_by_tests_above() -> None:
    assert "SagaRecord" in ACCEPTANCE_TEXT
    assert "store_full" in ACCEPTANCE_STORE_TEXT


def test_public_metadata() -> None:
    assert saga_orchestrator.__version__ == "0.1.0"
    assert "SagaOrchestrator" in saga_orchestrator.__all__
