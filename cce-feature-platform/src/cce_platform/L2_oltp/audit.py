"""Audit trail encoding for the transactional state machine.

Category 3 of the three OLTP gaps (see `KNOWN_GAPS.md`). This module owns one
decision: what a single entry in `txn:state:{txn_id}` actually says.

The trail was append-only from the start — `ALLOWED_TRANSITIONS` forbids
backward edges and nothing ever issues `ZREM` — so it recorded *what* changed
and *when*. It did not record *who* or *why*: the member was
`{state}:{event_id}`, `actor` and `reason` were accepted by `advance()`, used in
one log line, returned to the immediate caller, and then dropped. `get_history()`
reconstructed entries from the member alone and therefore returned `actor=""`
and `reason=""` — for every entry, including compliance holds.

An audit trail that cannot name the operator is not an audit trail. "This
transaction was held" without "held by whom, on what basis" is the exact
question an auditor asks, so the attribution has to survive the write.

Why the attribution lives *in* the member
-----------------------------------------
The obvious alternative is a side hash — `txn:audit:{txn_id}` keyed by
`event_id` — which keeps members small. It was rejected: the attribution would
then be a second write that can fail independently of the transition it
describes, reintroducing inside category 3 the same split-write problem that
category 1 already documents. One member is one `ZADD`, so the transition and
its attribution commit together or not at all.

The cost is real and bounded: members carry their metadata, so an unbounded
`metadata` dict would bloat the index Redis sorts on. Callers pass small
provenance maps (evaluator version, threshold), not payloads.

Encoding
--------
JSON with sorted keys and short field names, which makes the entry
self-describing and removes the delimiter question — a `reason` containing `:`
would have corrupted a positional format, and reasons are free text.

Members are compared as whole strings by the optimistic lock, never
re-serialised for comparison, so byte-stability across a round trip is enough.

Backward compatibility
----------------------
Local store files and Redis instances written before this change hold
`{state}:{event_id}` members. Those still decode, with `attributed=False` —
the machine-readable statement that this entry predates attribution rather than
that its operator was empty. Same convention as `RiskDecision.features_used=()`
in `risk.py`: absence of evidence is recorded explicitly, not as a blank that
reads like a value.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

_LEGACY_SEP = ":"


@dataclass(frozen=True)
class AuditRecord:
    """One decoded entry of the transition history.

    `attributed` is False for legacy members, where `actor` and `reason` are
    unknown rather than empty. Callers rendering an audit view should surface
    that difference instead of printing a blank operator.
    """

    state: str
    event_id: str
    actor: str = ""
    reason: str = ""
    metadata: dict[str, str] = field(default_factory=dict)
    attributed: bool = True


def encode_member(
    state: str,
    event_id: str,
    actor: str = "",
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> str:
    """Encode one history entry as a ZSET member.

    Empty fields are omitted so that an entry with no metadata stays compact.
    `event_id` is what keeps two transitions to the same state in the same
    second from colliding into one member.
    """
    payload: dict[str, Any] = {"s": str(state), "e": str(event_id)}
    if actor:
        payload["a"] = str(actor)
    if reason:
        payload["r"] = str(reason)
    if metadata:
        payload["m"] = {str(k): str(v) for k, v in metadata.items()}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def decode_member(member: str) -> AuditRecord:
    """Decode a ZSET member, accepting both the current and the legacy format.

    A member that parses as neither is not allowed to take down a read of the
    surrounding history: it degrades to the legacy path, which cannot raise.
    """
    if member.startswith("{"):
        try:
            payload = json.loads(member)
        except (ValueError, TypeError):
            payload = None
        if isinstance(payload, dict) and "s" in payload and "e" in payload:
            raw_meta = payload.get("m")
            metadata = (
                {str(k): str(v) for k, v in raw_meta.items()}
                if isinstance(raw_meta, dict)
                else {}
            )
            return AuditRecord(
                state=str(payload["s"]),
                event_id=str(payload["e"]),
                actor=str(payload.get("a", "")),
                reason=str(payload.get("r", "")),
                metadata=metadata,
                attributed=True,
            )

    state_str, _, event_id = member.partition(_LEGACY_SEP)
    return AuditRecord(state=state_str, event_id=event_id, attributed=False)
