# In-process orchestration saga with persisted progress and at-least-once resume

Status: accepted (generated, pending human review)

## Context

Multi-step operations across services need explicit compensations and the ability to continue after a crash. Temporal or Celery require servers, which is too heavy for a small in-process library that must stay dependency-free.

## Decision

Implement the orchestration saga pattern (Garcia-Molina and Salem 1987) as a synchronous orchestrator over a SagaStore protocol. State is saved before and after each step, only completed steps are compensated in reverse order, and resume re-executes a step found running (at-least-once, so actions must be idempotent). The context is normalised through a JSON round trip with strict size, depth and key checks that rerun after every merge. Clock, sleep and event sink are injected, events carry no context values or messages, and stores are bounded (max entries with terminal eviction, capped file reads, atomic tmp+fsync+replace writes).

## Consequences

Failure handling is deterministic and testable without a network or real time. Actions and compensations must be idempotent and one orchestrator may work on a saga id at a time because no locking is provided. Contexts are limited to JSON types and tuples come back as lists. Async or parallel steps, timeouts and brokers are out of scope.
