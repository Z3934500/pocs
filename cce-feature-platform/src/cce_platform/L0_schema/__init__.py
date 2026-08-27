"""Schema registry — the single source of truth for table DDL and layer membership.

Each layer module declares two things:
  TABLES: list[str]  — the tables it owns, in creation order
  DDL:    str        — the CREATE statements for exactly those tables

`reset_tables()` derives its truncation list from `ANALYTICS_LAYERS` rather
than a hand-maintained copy, so adding a table to a layer module is enough for
the rebuild to clear it. Previously the list lived in `db.py` and had to be
kept in sync with the DDL by hand; two tables were already missing from it.

The analytics/operational split is the important boundary here:

  ANALYTICS_LAYERS   deterministic output of the pipeline. Every row can be
                     recomputed from source, so a rebuild truncates them.
  OPERATIONAL_LAYERS in-flight state that nothing can recompute — unpublished
                     outbox events, unsettled trades. Never truncated.

`ANALYTICS_TABLES` and `OPERATIONAL_TABLES` are asserted disjoint at import
time, so a table cannot silently end up in both sets.

Module naming clarification
----------------------------
`mlops.py` and `quality.py` are SCHEMA modules (DDL), not runtime contracts:
  - mlops.py   : CREATE TABLE gold_customer_model_scores, ml_model_runs, ml_feature_drift
  - quality.py : CREATE TABLE dq_issues

They define TABLE STRUCTURE, not validation logic. The validation itself lives
in L2_olap/pipeline.py (build_quality_layer) and L2_olap/mlops.py (scoring).

If these modules contained runtime checks (e.g., "assert score in [0,1]"), they
would belong in a separate `L0_contracts/` package. But they don't — they're
pure DDL, so they correctly live here.

Think of this package as "what tables exist," not "how to validate their data."
"""

from __future__ import annotations

from . import bronze, gold, mlops, ops, quality, silver

# Order matters: tables are created in layer order so that any future foreign
# keys point at tables that already exist.
ANALYTICS_LAYERS = (bronze, silver, gold, mlops, quality)
OPERATIONAL_LAYERS = (ops,)

ALL_LAYERS = ANALYTICS_LAYERS + OPERATIONAL_LAYERS


def _collect(layers) -> list[str]:
    names: list[str] = []
    for layer in layers:
        names.extend(layer.TABLES)
    return names


ANALYTICS_TABLES: list[str] = _collect(ANALYTICS_LAYERS)
OPERATIONAL_TABLES: list[str] = _collect(OPERATIONAL_LAYERS)
ALL_TABLES: list[str] = ANALYTICS_TABLES + OPERATIONAL_TABLES

# A table in both sets would be truncated by a rebuild while being treated as
# durable operational state — catch that at import rather than at runtime.
_overlap = set(ANALYTICS_TABLES) & set(OPERATIONAL_TABLES)
if _overlap:
    raise RuntimeError(
        f"tables declared in both analytics and operational layers: {sorted(_overlap)}"
    )

_duplicates = [name for name in ALL_TABLES if ALL_TABLES.count(name) > 1]
if _duplicates:
    raise RuntimeError(f"tables declared by more than one layer: {sorted(set(_duplicates))}")

# Full DDL for every layer, analytics first.
SCHEMA: str = "\n".join(layer.DDL for layer in ALL_LAYERS)

ANALYTICS_SCHEMA: str = "\n".join(layer.DDL for layer in ANALYTICS_LAYERS)
OPERATIONAL_SCHEMA: str = "\n".join(layer.DDL for layer in OPERATIONAL_LAYERS)

__all__ = [
    "ALL_LAYERS",
    "ALL_TABLES",
    "ANALYTICS_LAYERS",
    "ANALYTICS_SCHEMA",
    "ANALYTICS_TABLES",
    "OPERATIONAL_LAYERS",
    "OPERATIONAL_SCHEMA",
    "OPERATIONAL_TABLES",
    "SCHEMA",
]
