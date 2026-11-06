# Security notes

## Threat model

`trysac-saga-orchestrator` is a library with no network access. Its only I/O is the `JsonFileSagaStore` writing to a directory chosen by the caller.

Defended:

- **Invalid saga ids**: ids must fully match `[A-Za-z0-9_-]{1,64}` before any store call, which blocks path traversal such as `../x` or `a/b` in `JsonFileSagaStore`.
- **Hostile or oversized context**: context keys must be strings, JSON encoding rejects NaN and non-serializable values, nesting is capped at 64 levels, cycles are rejected and the UTF-8 size is capped by `max_context_bytes` (default 1 MiB).
- **Corrupt or oversized stored records**: `JsonFileSagaStore.load` reads at most `max_record_bytes + 1` bytes, rejects non-JSON content and `NaN`/`Infinity` constants, and checks that the stored id matches the requested id; failures raise `corrupt_record`.
- **Torn writes**: records are written to a `.tmp` file, flushed, fsynced and moved with `os.replace`.
- **Information leakage**: `SagaError` messages and `on_event` payloads never contain context values or exception messages; step error messages are truncated to 200 characters.
- **Deserialization**: no `eval`, `exec` or pickle is used; only `json` is used.

Not defended:

- **Concurrent writers**: there is no locking. Two orchestrators running the same saga id can corrupt its progress; callers must serialize access.
- **Non-idempotent steps**: a step found `running` after a crash is executed again (at-least-once delivery).
- **Stored data confidentiality and integrity**: records are plain JSON on disk with no encryption or signature; anyone with write access to the directory can alter them.
- **Malicious step callables**: the library runs the callables it is given with full process privileges and does not sandbox them.
- **Unbounded in-memory store**: `InMemorySagaStore` holds up to `max_entries` records (default 10,000) and does not limit memory per record beyond the context cap.

## Supply chain

No runtime dependencies. Development tools are pinned in the project tooling and updated through Dependabot. Releases are intended for PyPI trusted publishing (OIDC) when available, with no long-lived token stored in the repository.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).
