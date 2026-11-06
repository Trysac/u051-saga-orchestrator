# saga-orchestrator

[![PyPI version](https://img.shields.io/pypi/v/trysac-saga-orchestrator)](https://pypi.org/project/trysac-saga-orchestrator/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-saga-orchestrator)](LICENSE)
[![CI](https://github.com/Trysac/u051-saga-orchestrator/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u051-saga-orchestrator/actions/workflows/ci.yml)

In-process saga orchestrator: ordered steps, reverse-order compensations, bounded retries and resumable persisted state.

## Problem

Multi-step business operations that touch several services (reserve stock, charge card, ship) leave inconsistent state when a middle step fails and nobody undoes the earlier ones. Hand-written try/except chains forget compensations and cannot resume after a crash. This library runs ordered steps, compensates completed steps in reverse order on failure, retries each step a bounded number of times and persists progress so an interrupted saga can be resumed with `resume()`.

## Install

```sh
pip install trysac-saga-orchestrator
# or
uv add trysac-saga-orchestrator
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import time
from datetime import datetime, timezone

from saga_orchestrator import InMemorySagaStore, SagaOrchestrator, Step

steps = [
    Step("reserve", lambda ctx: {"reserved": True}, lambda ctx: None),
    Step("charge", lambda ctx: None),
]
orchestrator = SagaOrchestrator(
    steps, InMemorySagaStore(), clock=lambda: datetime.now(timezone.utc), sleep=time.sleep
)
print(orchestrator.start("order-1", {"sku": "A1"}).status)  # -> "completed"
```

## Public API

- `Step(name, action, compensate=None, retries=0, retry_delay=0.5)`: frozen dataclass; `retries` must be 0-10. `action` receives a copy of the context and returns a dict to merge or `None`. `compensate` receives the context at failure time.
- `SagaOrchestrator(steps, store, *, clock, sleep, on_event=None, max_context_bytes=1_048_576)`: 1-100 steps with unique names matching `[a-z0-9_]{1,64}`. Methods `start(saga_id, context)`, `resume(saga_id)` and `status(saga_id)` each return a `SagaRecord`.
- `SagaRecord`: `saga_id`, `status` (`running`, `compensating`, `completed`, `compensated`, `failed`), `context`, `steps`, `error`, `created_at`, `updated_at`; provides `to_dict()` and `from_dict()`.
- `ErrorInfo`: `step`, `type`, `message` (truncated to 200 characters).
- `SagaStore` protocol with `load` and `save`.
- `InMemorySagaStore(max_entries=10_000)`: evicts the oldest terminal record when full; raises `store_full` if none is terminal. Not thread-safe.
- `JsonFileSagaStore(directory, *, max_record_bytes=1_114_112)`: one JSON file per saga, written atomically through a `.tmp` file, fsync and `os.replace`.

### Semantics and limits

- Delivery is at-least-once: a step found `running` on resume is executed again, so actions and compensations must be idempotent.
- Only steps that completed are compensated, in reverse order. A step that fails after its retries is stored as `skipped` and is not compensated.
- Retries catch `Exception` only; `BaseException` propagates and leaves the stored record untouched.
- Contexts must be JSON-serializable (tuples become lists), with string keys, and at most `max_context_bytes` in UTF-8.
- Terminal records are never rewritten; `resume()` returns them unchanged.
- Use one orchestrator per saga id at a time; there is no locking.
- Events (`on_event`) carry only `saga_id`, `kind`, `step`, `attempt` and `status`; they never include context values or exception messages.

### Errors

`SagaError.code` is one of `invalid_definition`, `already_exists`, `not_found`, `invalid_context`, `context_too_large`, `invalid_id`, `store_full` or `corrupt_record`.

## Security

See [docs/SECURITY.md](docs/SECURITY.md) for the threat model and limits.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
