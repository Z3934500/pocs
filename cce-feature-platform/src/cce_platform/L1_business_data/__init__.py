"""Business data — the numbers, plus the mechanism that loads them.

Layer 1: depends on `L0_configuration` for the policy file path, nothing else.

This layer is deliberately *data and loading*, not decisions. `policy.py` holds
compliance hold thresholds, cart display priorities, quote validity windows and
settlement cycles — values set by regulation, by whoever owns the campaign, and
by the market, none of them by this codebase. It exposes them as lookups
(`compliance_hold_threshold(product)`), and every actual judgement stays with
the caller:

  amount >= threshold → COMPLIANCE_HOLD      L2_oltp/risk.py
  trade date + T+N over holidays             L2_oltp/outbox.py
  cart ordering by priority                  cart_zset.py

Keeping the judgement out is what lets both sides of the write-authority
boundary read the same numbers without either owning them — and it is why there
is no `decision/` layer: the decisions live with the domain code that makes
them, which is where they are legible.

Values load from JSON at `settings.policy_path` merged over built-in defaults,
so an absent file leaves the PoC working while a deployment can mount a
ConfigMap. A malformed file is logged and ignored rather than raised: every
caller is on a serving path, and failing to start over a bad campaign edit is
worse than serving the previous known-good numbers.

LATERAL COUPLING FORBIDDEN
---------------------------
`L1_business_data` and `L1_mechanism` are SIBLINGS at layer 1. Neither may
import the other. Both reach DOWN into L0 only.

This is enforced by `tests/test_layers.py::test_layer_1_reaches_only_layer_0`.

Why no lateral coupling:
  - Business data (thresholds, priorities) should not depend on I/O mechanisms
  - Mechanism (db, kv) should not depend on business domain concepts
  - Separation enables reuse: mechanism is shared by OLAP/OLTP, business_data
    is specific to CCE financial products

If business_data needs I/O, the caller (L2) bridges:
  ✓ thresholds = load_policy(); db.store(thresholds)  # L2 orchestrates
  ✗ policy.save_to_database()                         # would couple to mechanism

If mechanism needs business rules, the caller (L2) passes them:
  ✓ reset_tables(ANALYTICS_TABLES)  # caller decides which tables are "analytics"
  ✗ reset_analytics_tables()         # mechanism shouldn't know "analytics" concept

See also: docs/ADR/ADR-001-olap-oltp-siblings.md (similar sibling pattern at L2)
"""

from __future__ import annotations

from .policy import (
    COMPLIANCE_HOLD_THRESHOLD,
    PRODUCT_PRIORITY,
    PRODUCT_T_PLUS,
    QUOTE_VALIDITY_MINUTES,
    compliance_hold_threshold,
    product_priority,
    quote_validity_minutes,
    reload_policy,
    settlement_t_plus,
)

__all__ = [
    "COMPLIANCE_HOLD_THRESHOLD",
    "PRODUCT_PRIORITY",
    "PRODUCT_T_PLUS",
    "QUOTE_VALIDITY_MINUTES",
    "compliance_hold_threshold",
    "product_priority",
    "quote_validity_minutes",
    "reload_policy",
    "settlement_t_plus",
]
