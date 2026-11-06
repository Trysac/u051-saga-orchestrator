# Saga orchestrator semantics

`saga_orchestrator` runs an ordered list of `Step` objects and, when a step still fails after
its retries, runs the compensations of the steps that completed, in reverse order.

## Delivery guarantees

- Progress is saved before each step (state `running`) and after it (state `done`).
- A step found `running` on `resume()` is executed again, so actions and compensations must be
  **idempotent** (at-least-once delivery).
- A crash during compensation is resumed from the step stored in state `compensating`.
- Terminal records (`completed`, `compensated`, `failed`) are never rewritten: `resume()` returns
  them without running code, saving or emitting events.
- Use one orchestrator per saga id at a time; the library does not lock.

## Retries

A step is attempted `retries + 1` times (retries 0-10) and the injected `sleep` receives
`retry_delay` between attempts. Only `Exception` is retried; a `BaseException` propagates and
leaves the stored record as it was (the current step stays `running` or `compensating`).

## Context

The context is a JSON document: `str` keys, values `str`, `int`, `float`, `bool`, `None`, `list`
and `dict`. Tuples are stored as lists. The serialized UTF-8 size is capped by
`max_context_bytes` (default 1 MiB) and the check is repeated after every merge; a merge that
breaks the rules counts as a step failure.

## Events

`on_event` receives exactly `saga_id`, `kind`, `step`, `attempt` and `status` (the saga status at
that moment). Context values and exception messages are never part of an event or a log record.

## Out of scope

Async or parallel steps, timeouts, locking and message brokers.
