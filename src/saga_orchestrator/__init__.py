"""In-process saga orchestrator with compensations, bounded retries and resumable state."""

from __future__ import annotations

from .errors import SagaError
from .models import ErrorInfo, SagaRecord, Step
from .orchestrator import SagaOrchestrator
from .stores import InMemorySagaStore, JsonFileSagaStore, SagaStore

__all__ = [
    "ErrorInfo",
    "InMemorySagaStore",
    "JsonFileSagaStore",
    "SagaError",
    "SagaOrchestrator",
    "SagaRecord",
    "SagaStore",
    "Step",
    "__version__",
]

__version__ = "0.1.0"
