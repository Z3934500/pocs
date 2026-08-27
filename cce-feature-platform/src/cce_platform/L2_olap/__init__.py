"""Analytical (OLAP) domain — the read-and-derive side of the platform.

Sibling of `cce_platform.L2_oltp`, not its caller and not its callee. Both sit at
layer 2; both reach down into layers 0 and 1; neither imports the other. The
import direction is asserted, not merely intended: `tests/test_oltp.py`
::BoundaryTest fails if any module here reaches into the transactional package.

What lives here owns no irreplaceable state. Everything is recomputable — the
pipeline truncates and rebuilds Gold on every run (C6 in
docs/ARCHITECTURE_OLTP_BOUNDARY.md), so a lost row is a rerun away from being
restored. That is precisely what distinguishes this package from `L2_oltp`, whose
settlement obligations and undelivered outbox events cannot be derived from
anything.

  pipeline.py             Bronze -> Silver -> Gold medallion rebuild
  api.py                  FastAPI serving surface
  batch_importer.py       Gold -> online store export
  realtime.py             CDC consumer, at-least-once safe
  flink_cdc_pipeline.py   streaming simulation and Flink submission
  online_store.py         feature serving projection (Redis or JSON file)
  cart_zset.py            quote-cart ranking and expiry
  segmentation.py         cluster assignment
  mlops.py                scoring and drift
  graph_identity.py       identity candidate discovery
  metrics.py              Prometheus surface

Deliberately no re-exports
--------------------------
This module is a docstring and nothing else. Re-exporting the submodules the way
`L2_oltp/__init__.py` does would make `import cce_platform.L2_olap.pipeline` pull
in `api.py`, and with it FastAPI and uvicorn — turning a batch job's import into
a web-framework dependency. Today `python -m cce_platform.L2_olap.pipeline` runs
without FastAPI installed. Keeping this file empty of imports is what preserves
that, so the convenience of a flat namespace is declined on purpose.
"""

from __future__ import annotations

__all__: list[str] = []
