"""Risk-control seam for the transaction lifecycle.

`RISK_CHECK` has been a state label with no evaluator. `run_auto_advance()`
moved every transaction from `RISK_CHECK` straight to `APPROVED` unless a flat
amount-vs-threshold comparison tripped `COMPLIANCE_HOLD`, and `actor="risk_engine"`
appeared in this repo only as a string literal in the chaos suite. There was no
risk engine.

This module does not add one. It gives the empty slot a type, so the gap is
visible in the signature of `run_auto_advance` rather than only in prose, and so
a real evaluator can be dropped in without touching the state machine.

What a feature-reading evaluator would need, and the platform does not have
--------------------------------------------------------------------------
The join key is already there and already unused: `init_transaction()` stores
`customer_key` in `txn:meta:{txn_id}`, and nothing reads it. Wiring it to the
online store is therefore a small code change and a large correctness claim, so
it is deliberately not made here. The missing pieces:

  Per-field freshness. `cce:features:{key}` merges batch and stream writes
  field by field and carries only `feature_source`. A decision cannot tell
  whether the value it just read is four seconds or four days old, so
  `feature_age_s` below has nowhere to get its value from yet.

  Sub-second velocity. `velocity_7d` is a *count* over seven days anchored on
  the newest row in the batch, not a rate and not wall-clock relative. The
  five-transactions-in-five-minutes check is documented in
  `flink_cdc_pipeline`'s docstring and in README but does not exist in code.

  A trustworthy label. The only fraud signal in the repo, `is_fraud_label`, is
  planted by the synthetic generator at `pmod(txn_num, 997) == 0`. Nothing has
  been learned from real outcomes.

  Feature semantics that mean what they say. `risk_score` sums only positive
  terms, so an active high-value customer outranks a dormant one — it is a
  behavioural-intensity composite, not a loss probability. `risk_band` buckets a
  *propensity* score, so "high" means high buying intent. And the local and
  Spark definitions of `risk_score` differ by a fourth term. Feeding any of
  these into a hold decision under its current name would be a semantic-drift
  bug, not a feature.

Until those hold, the honest default is the threshold rule that was already
here. `features_used` and `feature_age_s` are present and empty on purpose:
they record that a decision was made without consulting a single feature.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from ..L1_business_data import compliance_hold_threshold

__all__ = [
    "RiskDecision",
    "RiskEvaluator",
    "ThresholdRiskEvaluator",
]


@dataclass(frozen=True)
class RiskDecision:
    """The outcome of a risk evaluation, with enough provenance to defend it.

    `hold` alone would be sufficient to drive the state machine. The rest exists
    because a regulator asking "why was this transaction held in March" needs an
    answer, and "the code said so" is not one. Which rule version fired, which
    features it read and how stale they were is the difference between an
    auditable decision and an opaque one.
    """

    hold: bool
    reason: str
    evaluator_version: str
    # Feature names actually read to reach this decision. Empty means the
    # decision consulted no customer features at all — see module docstring.
    features_used: tuple[str, ...] = ()
    # Age in seconds of the freshest feature read, or None when no feature was
    # read or when the store cannot report per-field freshness (it currently
    # cannot).
    feature_age_s: float | None = None
    metadata: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class RiskEvaluator(Protocol):
    """Decides whether a transaction needs a compliance hold.

    An implementation must be side-effect free and must not raise for missing
    or malformed metadata: it is called inside the transition path, and a
    transaction that cannot be evaluated must still reach a defined state. Fail
    closed — return a hold — rather than propagating an exception.
    """

    def evaluate(self, txn_id: str, meta: dict[str, str]) -> RiskDecision:
        """Evaluate `txn_id` given its `txn:meta:{txn_id}` hash."""
        ...


class ThresholdRiskEvaluator:
    """Amount-vs-threshold hold rule — the behaviour that was already in place.

    Thresholds come from `policy.compliance_hold_threshold()`, so a regulatory
    change is a config edit rather than a release. Products with no configured
    threshold are never held on amount alone: the policy default is infinity,
    which is a deliberate choice to avoid blocking an unknown product line, and
    also means a new product silently gets no amount gate until policy is
    updated.

    This reads `product` and `amount` from transaction metadata and nothing
    else. It does not know who the customer is.
    """

    VERSION = "threshold-1"

    def evaluate(self, txn_id: str, meta: dict[str, str]) -> RiskDecision:
        product = meta.get("product", "")
        try:
            amount = float(meta.get("amount", 0))
        except (ValueError, TypeError):
            # Unparseable amount is treated as zero rather than as an error, to
            # match the prior behaviour of should_compliance_hold(). Note this
            # fails *open* on malformed data: a corrupted amount field yields no
            # hold. Worth revisiting when a real evaluator lands.
            amount = 0.0

        threshold = compliance_hold_threshold(product)
        hold = amount >= threshold
        return RiskDecision(
            hold=hold,
            reason="amount_threshold" if hold else "below_threshold",
            evaluator_version=self.VERSION,
            features_used=(),
            feature_age_s=None,
            metadata={"product": product, "amount": f"{amount:.2f}", "threshold": str(threshold)},
        )
