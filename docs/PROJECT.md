# Project record

## Objective

Provide a small, dependency-free in-process saga orchestrator for Python: ordered steps, reverse-order compensations of completed steps, bounded retries per step and persisted state that can be resumed after a crash.

## Scope

- In scope: `Step`, `SagaOrchestrator` (`start`, `resume`, `status`), `SagaRecord` and `ErrorInfo`, the `SagaStore` protocol with `InMemorySagaStore` and `JsonFileSagaStore`, and `SagaError` with stable codes, all documented in [README.md](../README.md) and tested.
- Out of scope: async or parallel steps, per-step timeouts, locking between orchestrators, message brokers and external workflow servers (such as Temporal or Celery), and any runtime dependency.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 681164 / 92893 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.8007 |

## Factory run u051 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | claude-sonnet-5-5 | yes | 2568 | 76800 | 0 | 4246 | 0.3870 |
| fix-1 | claude-sonnet-5-5 | no | 22 | 5204 | 26671 | 302114 | 0.1490 |
| audit | claude-sonnet-5-5 | no | 16 | 4379 | 36191 | 216088 | 0.1559 |
| fix-2 | claude-sonnet-5-5 | no | 6 | 1507 | 7795 | 40656 | 0.0386 |
| docs | claude-haiku-5-5 | no | 115448 | 5003 | 0 | 0 | 0.0702 |

Total: $0.8007. Prices: Anthropic list prices.
