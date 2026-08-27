from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    sqlite_path: Path
    oltp_sqlite_path: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    online_store_path: Path
    cdc_events_path: Path
    frontend_dir: Path
    policy_path: Path
    runtime_env: str
    require_redis: bool


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    base_dir = Path(os.getenv("CCE_BASE_DIR", Path(__file__).resolve().parents[2]))
    sqlite_path = Path(os.getenv("CCE_SQLITE_PATH", base_dir / "data" / "warehouse" / "cce_platform.sqlite"))
    if not sqlite_path.is_absolute():
        sqlite_path = base_dir / sqlite_path
    # Operational (OLTP) state lives in its own file. The analytics warehouse is
    # truncated and rebuilt on every pipeline run; unsettled trades and
    # unpublished events are not recomputable and must not share a blast radius
    # with it. Separate files also make the write-authority boundary visible at
    # the filesystem level, which is what L0_schema/ops.py already documents.
    oltp_sqlite_path = Path(os.getenv("CCE_OLTP_SQLITE_PATH", base_dir / "data" / "oltp" / "cce_oltp.sqlite"))
    if not oltp_sqlite_path.is_absolute():
        oltp_sqlite_path = base_dir / oltp_sqlite_path
    online_store_path = Path(os.getenv("CCE_ONLINE_STORE_PATH", base_dir / "data" / "online" / "feature_store.json"))
    if not online_store_path.is_absolute():
        online_store_path = base_dir / online_store_path
    cdc_events_path = Path(os.getenv("CCE_CDC_EVENTS_PATH", base_dir / "data" / "bronze" / "cdc_events.jsonl"))
    if not cdc_events_path.is_absolute():
        cdc_events_path = base_dir / cdc_events_path
    # Compliance thresholds, cart ranking and settlement cycles are business
    # policy, not code. Absent file = built-in defaults (see policy.py), so the
    # PoC runs unchanged while a deployment can mount a ConfigMap here.
    policy_path = Path(os.getenv("CCE_POLICY_PATH", base_dir / "config" / "business_policy.json"))
    if not policy_path.is_absolute():
        policy_path = base_dir / policy_path

    runtime_env = os.getenv("CCE_RUNTIME_ENV", "local").strip().lower()
    # Falling back to the local JSON store is correct for the PoC but wrong for
    # a deployed environment: the fallback is per-process, so replicas silently
    # diverge instead of sharing state. Staging and production therefore require
    # a reachable Redis by default; CCE_REQUIRE_REDIS overrides either way.
    require_redis_raw = os.getenv("CCE_REQUIRE_REDIS")
    require_redis = (
        _parse_bool(require_redis_raw)
        if require_redis_raw is not None
        else runtime_env in {"staging", "production"}
    )

    return Settings(
        base_dir=base_dir,
        sqlite_path=sqlite_path,
        oltp_sqlite_path=oltp_sqlite_path,
        bronze_dir=base_dir / "data" / "bronze",
        silver_dir=base_dir / "data" / "silver",
        gold_dir=base_dir / "data" / "gold",
        online_store_path=online_store_path,
        cdc_events_path=cdc_events_path,
        frontend_dir=base_dir / "frontend",
        policy_path=policy_path,
        runtime_env=runtime_env,
        require_redis=require_redis,
    )


settings = load_settings()
