from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    sqlite_path: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    frontend_dir: Path


def load_settings() -> Settings:
    base_dir = Path(os.getenv("OEE_BASE_DIR", Path(__file__).resolve().parents[2]))
    sqlite_path = Path(os.getenv("OEE_SQLITE_PATH", base_dir / "data" / "warehouse" / "oee_platform.sqlite"))
    if not sqlite_path.is_absolute():
        sqlite_path = base_dir / sqlite_path

    return Settings(
        base_dir=base_dir,
        sqlite_path=sqlite_path,
        bronze_dir=base_dir / "data" / "bronze",
        silver_dir=base_dir / "data" / "silver",
        gold_dir=base_dir / "data" / "gold",
        frontend_dir=base_dir / "frontend",
    )


settings = load_settings()
