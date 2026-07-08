from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    sqlite_path: Path = BASE_DIR / "data" / "oms_oltp.sqlite"
    frontend_dir: Path = BASE_DIR / "frontend"
    default_reservation_ttl_minutes: int = 15


settings = Settings()
