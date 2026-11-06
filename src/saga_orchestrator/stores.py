"""Saga stores: a bounded in-memory store and an atomic JSON-file store."""

from __future__ import annotations

import json
import os
from collections import OrderedDict
from pathlib import Path
from typing import Any, Protocol

from .errors import SagaError
from .models import TERMINAL_STATUSES, SagaRecord, require_positive_int, validate_saga_id

__all__ = ["InMemorySagaStore", "JsonFileSagaStore", "SagaStore"]


class SagaStore(Protocol):
    """Persistence contract for saga records."""

    def load(self, saga_id: str) -> SagaRecord | None:
        """Return the stored record, or ``None`` when the id is unknown."""

    def save(self, record: SagaRecord) -> None:
        """Persist ``record``, replacing any record with the same id."""


class InMemorySagaStore:
    """Bounded in-memory store (not thread-safe).

    When full, the oldest terminal record is evicted to make room for a new id; if no
    record is terminal, ``SagaError`` with code ``store_full`` is raised.
    """

    def __init__(self, max_entries: int = 10_000) -> None:
        self._max_entries = require_positive_int(max_entries, "max_entries")
        self._data: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def load(self, saga_id: str) -> SagaRecord | None:
        """Return a copy of the stored record, or ``None``."""
        data = self._data.get(saga_id)
        return None if data is None else SagaRecord.from_dict(data)

    def save(self, record: SagaRecord) -> None:
        """Store a copy of ``record``, evicting the oldest terminal record if needed."""
        data = record.to_dict()
        if record.saga_id not in self._data and len(self._data) >= self._max_entries:
            self._evict()
        self._data[record.saga_id] = data

    def _evict(self) -> None:
        for saga_id, data in self._data.items():
            if data["status"] in TERMINAL_STATUSES:
                del self._data[saga_id]
                return
        raise SagaError("store_full", "store is full and holds no terminal saga")


def _reject_constant(name: str) -> Any:
    raise ValueError("non-finite number in stored record")


class JsonFileSagaStore:
    """One JSON file per saga id inside ``directory``.

    Writes go to a ``.tmp`` file in the same directory, are flushed and fsynced and are
    then moved over the target with ``os.replace``. Reads are capped at
    ``max_record_bytes``; larger or unparsable files raise ``corrupt_record``.
    I/O failures surface as ``OSError``.
    """

    def __init__(self, directory: Path, *, max_record_bytes: int = 1_114_112) -> None:
        self._max_record_bytes = require_positive_int(max_record_bytes, "max_record_bytes")
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)

    def _path(self, saga_id: str) -> Path:
        validate_saga_id(saga_id)
        return self._directory / f"{saga_id}.json"

    def load(self, saga_id: str) -> SagaRecord | None:
        """Return the stored record, or ``None`` when no file exists."""
        path = self._path(saga_id)
        try:
            with path.open("rb") as handle:
                raw = handle.read(self._max_record_bytes + 1)
        except FileNotFoundError:
            return None
        if len(raw) > self._max_record_bytes:
            raise SagaError("corrupt_record", "stored saga record exceeds the size limit")
        try:
            data = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
        except (ValueError, RecursionError):
            raise SagaError("corrupt_record", "stored saga record is not valid JSON") from None
        record = SagaRecord.from_dict(data)
        if record.saga_id != saga_id:
            raise SagaError("corrupt_record", "stored saga record has a different id")
        return record

    def save(self, record: SagaRecord) -> None:
        """Atomically write ``record``."""
        path = self._path(record.saga_id)
        text = json.dumps(
            record.to_dict(),
            allow_nan=False,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        payload = text.encode("utf-8")
        if len(payload) > self._max_record_bytes:
            raise SagaError("context_too_large", "saga record exceeds the size limit")
        tmp = path.with_name(path.name + ".tmp")
        try:
            with tmp.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
