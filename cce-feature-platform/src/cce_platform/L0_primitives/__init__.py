"""Pure primitives — no configuration, no state, no I/O.

Layer 0 of the package: nothing here imports anything else from
`cce_platform`. A module qualifies only if it is a deterministic function of its
arguments, which is what makes it safe for both sides of the write-authority
boundary to share.

`ids.stable_issue_id` is the whole layer today. It lives here rather than in
`pipeline.py` because the batch pipeline and `cce_platform.L2_oltp` both mint IDs
and both must mint them the *same* way; two subsystems deriving "stable" IDs
from independent copies drift silently, and the symptom surfaces much later as
a deduplication miss.
"""

from __future__ import annotations

from .ids import stable_issue_id

__all__ = ["stable_issue_id"]
