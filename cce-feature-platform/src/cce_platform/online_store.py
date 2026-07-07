from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import settings


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
        current = {} if replace else self.read_all()
        for customer_key, payload in payloads.items():
            existing = current.get(customer_key, {})
            current[customer_key] = {**existing, **payload}
        self._write(current)
        return len(payloads)

    def upsert(self, customer_key: str, payload: FeaturePayload) -> None:
        self.bulk_upsert({customer_key: payload})

    def _write(self, data: dict[str, FeaturePayload]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, sort_keys=True)
        temp_path.replace(self.path)
