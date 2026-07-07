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
    online_store_path: Path
    cdc_events_path: Path
    frontend_dir: Path


def load_settings() -> Settings:
    base_dir = Path(os.getenv("CCE_BASE_DIR", Path(__file__).resolve().parents[2]))
    sqlite_path = Path(os.getenv("CCE_SQLITE_PATH", base_dir / "data" / "warehouse" / "cce_platform.sqlite"))
    if not sqlite_path.is_absolute():
        sqlite_path = base_dir / sqlite_path
    online_store_path = Path(os.getenv("CCE_ONLINE_STORE_PATH", base_dir / "data" / "online" / "feature_store.json"))
    if not online_store_path.is_absolute():
        online_store_path = base_dir / online_store_path
    cdc_events_path = Path(os.getenv("CCE_CDC_EVENTS_PATH", base_dir / "data" / "bronze" / "cdc_events.jsonl"))
    if not cdc_events_path.is_absolute():
        cdc_events_path = base_dir / cdc_events_path

    return Settings(
        base_dir=base_dir,
        sqlite_path=sqlite_path,
        bronze_dir=base_dir / "data" / "bronze",
        silver_dir=base_dir / "data" / "silver",
        gold_dir=base_dir / "data" / "gold",
        online_store_path=online_store_path,
        cdc_events_path=cdc_events_path,
        frontend_dir=base_dir / "frontend",
    )


settings = load_settings()
