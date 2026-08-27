"""Deterministic identifier minting, shared by the analytics and OLTP sides.

This lives on its own rather than in `pipeline.py` because both sides of the
write-authority boundary mint IDs and both must mint them the *same* way. The
batch pipeline uses it to make reruns idempotent (`record_issue`, model runs,
drift metrics); `cce_platform.L2_oltp` uses it for outbox event IDs.

Duplicating this function into the OLTP package would be the obvious
alternative and the wrong one: two subsystems deriving "stable" IDs from
independent copies drift silently, and the symptom only shows up much later as
a deduplication miss. Keeping one implementation makes the ID space shared by
construction.
"""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5


def stable_issue_id(*parts: str) -> str:
    """Derive a UUID5 from the given parts, stable across processes and reruns.

    The parts are joined with "|" before hashing, so callers must not pass a
    part that can itself contain "|" in a position where it would change the
    grouping. Every current caller passes fixed-vocabulary tokens or hashed
    identifiers.
    """
    value = "|".join(parts)
    return str(uuid5(NAMESPACE_URL, value))
