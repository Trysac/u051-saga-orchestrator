# Architecture

## Purpose

`trysac-saga-orchestrator` is a small, dependency-free Python library that runs ordered saga steps with reverse-order compensations, bounded retries and resumable persisted state.

## Shape

```text
caller code --> saga_orchestrator (public API in __init__.py)
                    |
                    +-- orchestrator.py  (SagaOrchestrator: start, resume, status)
                    +-- models.py        (Step, SagaRecord, ErrorInfo, validators)
                    +-- stores.py        (SagaStore protocol, InMemorySagaStore, JsonFileSagaStore)
                    +-- errors.py        (SagaError with stable codes)
```

- **Public API**: the names listed in `__all__` of `src/saga_orchestrator/__init__.py`: `ErrorInfo`, `InMemorySagaStore`, `JsonFileSagaStore`, `SagaError`, `SagaOrchestrator`, `SagaRecord`, `SagaStore`, `Step`, `__version__`.
- **Persistence**: each state change is saved through the injected store before and after each step.
- **Errors**: invalid input raises `SagaError` with a stable `code`; messages never contain context values.
- **Side effects**: only through the injected store, the injected `clock` and `sleep`, and the optional `on_event` callback. Logging goes to a `NullHandler` logger.

## Decisions

Architecture decisions are recorded in [decisions/](decisions/) as numbered ADRs.
