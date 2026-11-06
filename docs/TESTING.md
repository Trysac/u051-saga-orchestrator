# Testing

## Running

```sh
uv run pytest --cov=saga_orchestrator   # tests with coverage report
uv run python scripts/check.py          # every gate, as CI runs it
```

## Gates

- Branch coverage of `src/saga_orchestrator` must stay at or above 80 %; the gate fails below it.
- Tests exercise the public API (`import saga_orchestrator`) and assert observable effects: return values, raised `SagaError` codes and messages, stored records and emitted events.

## Test files

The test suite lives in `tests/`. Each module of the package has a matching test file: `tests/test_models.py`, `tests/test_orchestrator.py` and `tests/test_stores.py`, with shared fixtures in `tests/conftest.py`. Tests cover the acceptance behaviour of the library: successful runs and merged contexts, retries and reverse-order compensation, compensation failure leading to `failed`, resume after a crash with `running` and `compensating` states, terminal records returned unchanged, context validation and size limits, saga id validation, the atomic JSON-file store and the bounded in-memory store, and event payload keys.

Tests are deterministic: they use injected clocks and sleep functions, no network and no dependence on test order.
