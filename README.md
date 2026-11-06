# saga-orchestrator

[![PyPI version](https://img.shields.io/pypi/v/trysac-saga-orchestrator)](https://pypi.org/project/trysac-saga-orchestrator/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-saga-orchestrator)](LICENSE)
[![CI](https://github.com/Trysac/u051-saga-orchestrator/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u051-saga-orchestrator/actions/workflows/ci.yml)

In-process saga orchestrator: ordered steps, reverse-order compensations, bounded retries and resumable persisted state

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-saga-orchestrator
# or
uv add trysac-saga-orchestrator
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import saga_orchestrator

# normalize() trims surrounding whitespace and lower-cases the text.
print(saga_orchestrator.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    saga_orchestrator.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
