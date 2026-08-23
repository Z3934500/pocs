from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from .config import settings

# Module-level lock: ensures atomic read-modify-write across threads.
# In production (Redis backend), this lock is not used.
_store_lock = threading.Lock()


FeaturePayload = dict[str, Any]


class LocalOnlineStore:
    """Small JSON-backed stand-in for Redis used by the local PoC."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or settings.online_store_path

    def read_all(self) -> dict[str, FeaturePayload]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return {str(key): dict(value) for key, value in data.items()}

    def get(self, customer_key: str) -> FeaturePayload | None:
        return self.read_all().get(customer_key)

    def bulk_upsert(self, payloads: dict[str, FeaturePayload], replace: bool = False) -> int:
        with _store_lock:
            current = {} if replace else self._read_locked()
            for customer_key, payload in payloads.items():
                existing = current.get(customer_key, {})
                current[customer_key] = {**existing, **payload}
            self._write(current)
        return len(payloads)

    def upsert(self, customer_key: str, payload: FeaturePayload) -> None:
        self.bulk_upsert({customer_key: payload})

    def _read_locked(self) -> dict[str, FeaturePayload]:
        """Read without acquiring the lock — caller must hold _store_lock."""
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return {str(key): dict(value) for key, value in data.items()}

    def _write(self, data: dict[str, FeaturePayload]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, sort_keys=True)
        # Windows does not allow renaming over an open file (WinError 5/32).
        # Retry with brief backoff; the lock is always transient (held only
        # during the json.dump above, which is protected by _store_lock).
        import time as _time
        for attempt in range(5):
            try:
                temp_path.replace(self.path)
                return
            except OSError:
                if attempt == 4:
                    raise
                _time.sleep(0.01 * (attempt + 1))
