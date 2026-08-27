# ADR-004: No re-exports in L2_olap/__init__.py

**Status**: Accepted  
**Date**: 2026-08-27  
**Decision Makers**: Architecture team

## Context

The `L2_olap` package contains 11 modules (pipeline, api, batch_importer, etc.). A common Python practice is to re-export main symbols in `__init__.py` to allow:

```python
# With re-exports
from cce_platform.L2_olap import run_pipeline, FastAPIApp, make_online_store

# Without re-exports (current)
from cce_platform.L2_olap.pipeline import run_pipeline
from cce_platform.L2_olap.api import app
from cce_platform.L2_olap.online_store import make_online_store
```

The question: why is `L2_olap/__init__.py` empty (`__all__ = []`) while `L2_oltp/__init__.py` **re-exports** its symbols?

## Decision

**`L2_olap/__init__.py` remains empty (no re-exports).** Unlike `L2_oltp`, which does re-export.

## Reasoning

### Key difference: Nature of dependencies

#### L2_oltp (re-exports OK)
```python
# L2_oltp/__init__.py
from .state_machine import TransactionStateMachine, TxnState
from .outbox import write_outbox_event, EventPublisher
# ...
```

**Why it works**:
- All OLTP modules share **same lightweight dependencies**
- Redis client, dataclasses, enum → fast to import
- No heavy frameworks (no FastAPI, no PyFlink)

#### L2_olap (re-exports **problematic**)
```python
# L2_olap/__init__.py (hypothetical, rejected)
from .pipeline import run_pipeline        # OK, lightweight
from .api import app                      # ❌ imports FastAPI + uvicorn
from .flink_cdc_pipeline import submit    # ❌ imports PyFlink (500MB)
```

**Concrete problem**:
```bash
# Batch job that only needs pipeline
python -m cce_platform.L2_olap.pipeline

# With re-exports, this would ALSO import:
# - FastAPI (web framework, not needed in batch)
# - uvicorn (ASGI server, not needed)
# - PyFlink (optional, may not be installed)
```

### Principle: Import what you need

**Import what you need, not the entire package.**

```python
# Batch job
from cce_platform.L2_olap.pipeline import run_pipeline  # 50ms import

# API server
from cce_platform.L2_olap.api import app  # 200ms import (FastAPI)

# CDC submitter
from cce_platform.L2_olap.flink_cdc_pipeline import submit  # 1500ms import (PyFlink)
```

Each workload pays only for its own dependencies.

## Consequences

### Positive
1. **Fast startup**: Batch pipeline starts in 50ms, not 2 seconds
2. **Optional dependencies**: PyFlink can be absent if CDC not used
3. **Isolation**: Error in `api.py` doesn't break batch
4. **Clarity**: `from .pipeline import` is explicit about what's imported

### Negative
1. **More verbose imports**: 3 lines instead of 1
   ```python
   # Current (verbose)
   from cce_platform.L2_olap.pipeline import run_pipeline
   from cce_platform.L2_olap.online_store import make_online_store
   
   # With re-exports (rejected)
   from cce_platform.L2_olap import run_pipeline, make_online_store
   ```

2. **Less "batteries included"**: Developers must know which module to import

### Accepted trade-off
**Performance and isolation > import convenience.**

## When re-exports would be acceptable

If `L2_olap` were **homogeneous**:
- All pure algorithms (no frameworks)
- All lightweight dependencies
- No optional dependencies

**But that's not the case**: OLAP mixes batch, API, and streaming with different stacks.

## Comparison with other Python packages

### Examples that DON'T re-export (like us)
```python
# Django
from django.http.response import HttpResponse  # not from django import HttpResponse
from django.db.models import Model             # not from django import Model

# Reason: http, db, admin have different stacks
```

### Examples that DO re-export (unlike us)
```python
# requests (homogeneous, single responsibility)
from requests import get, post

# numpy (homogeneous, all array operations)
from numpy import array, zeros
```

## Workaround for developers

For developers who want convenience:

```python
# Create local shortcuts.py file
from cce_platform.L2_olap.pipeline import run_pipeline
from cce_platform.L2_olap.online_store import make_online_store
from cce_platform.L2_olap.api import app

# Then import from shortcuts
from shortcuts import run_pipeline, make_online_store, app
```

This gives choice to developer without forcing global strategy.

## References
- `src/cce_platform/L2_olap/__init__.py` lines 27-35 (docstring "Deliberately no re-exports")
- `src/cce_platform/L2_oltp/__init__.py` (counter-example with re-exports)
- PEP 8: "Explicit is better than implicit"
