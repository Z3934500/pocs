"""Business policy — the numbers that change without a code release.

Three sets of rules used to live as literal dicts inside the infrastructure
modules that happened to read them:

  COMPLIANCE_HOLD_THRESHOLD  oltp.risk — SGD amounts above which a
                             transaction is routed through COMPLIANCE_HOLD
  PRODUCT_PRIORITY,          cart_zset — cart display ranking, and how long a
  DEFAULT_QUOTE_VALIDITY     product's quote stays valid
  PRODUCT_T_PLUS             oltp.outbox — settlement cycle per product

None of these are infrastructure concerns. Compliance thresholds are set by
regulation, priorities and quote windows by whoever owns the campaign, and
settlement cycles by the market. Changing one number meant editing Python and
shipping a release.

They now load from JSON at `settings.policy_path` (default
`config/business_policy.json`, overridable with `CCE_POLICY_PATH`) and are
merged over the built-in defaults below — so an absent file keeps the PoC
working unchanged, while a deployed environment can mount a ConfigMap. Call
`reload_policy()` to pick up an edited file in a running process.

Products are keyed by their uppercase code. Unknown products fall back to the
conservative default in each accessor rather than raising, so a new product
code cannot break settlement or cart ranking before its policy row exists.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ..L0_configuration import settings

logger = logging.getLogger(__name__)


# Defaults; a policy file only needs to carry the values it overrides.
_DEFAULTS: dict[str, dict[str, Any]] = {
    # Amount (SGD) at or above which a transaction routes through COMPLIANCE_HOLD.
    # Products absent here are never held on amount alone.
    "compliance_hold_threshold_sgd": {
        "PREMIUM_FINANCING": 1000.0,
        "INVESTMENT":        500.0,
        "INVESTMENT_LINKED": 500.0,
        "INSURANCE":         2000.0,
    },
    # Cart display weight; higher is shown first. High-margin products rank up.
    "product_priority": {
        "PREMIUM_FINANCING": 10.0,
        "INVESTMENT_LINKED":  9.0,
        "INVESTMENT":         8.0,
        "INSURANCE":          7.0,
        "TRAVEL_INSURANCE":   6.0,
        "SAVINGS":            5.0,
        "CARD":               4.0,
    },
    # How long a quote stays valid, in minutes. 0 = never expires.
    "quote_validity_minutes": {
        "PREMIUM_FINANCING":  60,    # rate-sensitive
        "INVESTMENT_LINKED":  30,    # NAV changes daily
        "INVESTMENT":         30,
        "INSURANCE":        1440,    # stable pricing
        "TRAVEL_INSURANCE":  120,
        "SAVINGS":             0,
        "CARD":                0,
    },
    # Settlement cycle: business days between trade date and settlement.
    "product_t_plus": {
        "PREMIUM_FINANCING": 2,
        "INVESTMENT":        2,
        "INVESTMENT_LINKED": 2,
        "INSURANCE":         1,
        "TRAVEL_INSURANCE":  0,
        "SAVINGS":           0,
        "CARD":              0,
    },
}

# Fallback per section for a product the policy does not mention.
_MISSING_PRODUCT_DEFAULT: dict[str, Any] = {
    "compliance_hold_threshold_sgd": float("inf"),   # no amount-based hold
    "product_priority":              1.0,            # ranks last
    "quote_validity_minutes":        0,              # no expiry
    "product_t_plus":                2,              # slowest common cycle
}


def _load_policy() -> dict[str, dict[str, Any]]:
    """Merge the policy file over the defaults, section by section.

    A malformed or unreadable file is logged and ignored rather than raised:
    every caller here is on a serving path, and failing to start over a bad
    campaign edit is worse than serving the previous known-good numbers.
    """
    merged: dict[str, dict[str, Any]] = {
        section: dict(values) for section, values in _DEFAULTS.items()
    }

    path = settings.policy_path
    if not path.exists():
        logger.debug("business policy: %s not present, using built-in defaults", path)
        return merged

    try:
        with path.open("r", encoding="utf-8") as fh:
            loaded = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("business policy: could not read %s (%s), using defaults", path, exc)
        return merged

    if not isinstance(loaded, dict):
        logger.error("business policy: %s is not a JSON object, using defaults", path)
        return merged

    for section, overrides in loaded.items():
        # Keys starting with "_" are comments; the shipped policy file uses them
        # to document each section, since JSON has no comment syntax.
        if section.startswith("_"):
            continue
        if section not in merged:
            logger.warning("business policy: ignoring unknown section %r", section)
            continue
        if not isinstance(overrides, dict):
            logger.warning("business policy: section %r is not an object, ignoring", section)
            continue
        merged[section].update({str(k).upper(): v for k, v in overrides.items()})

    logger.info("business policy: loaded overrides from %s", path)
    return merged


_policy = _load_policy()


def reload_policy() -> None:
    """Re-read the policy file and refresh the exported tables in place."""
    global _policy
    _policy = _load_policy()
    for name, section in _EXPORTED_TABLES.items():
        table = globals()[name]
        table.clear()
        table.update(_policy[section])


def _lookup(section: str, product: str) -> Any:
    return _policy[section].get(
        str(product).upper(), _MISSING_PRODUCT_DEFAULT[section]
    )


def compliance_hold_threshold(product: str) -> float:
    """SGD amount at or above which `product` requires a compliance hold."""
    return float(_lookup("compliance_hold_threshold_sgd", product))


def product_priority(product: str) -> float:
    """Cart display weight for `product`; higher sorts first."""
    return float(_lookup("product_priority", product))


def quote_validity_minutes(product: str) -> int:
    """Minutes a quote for `product` stays valid; 0 means it never expires."""
    return int(_lookup("quote_validity_minutes", product))


def settlement_t_plus(product: str) -> int:
    """Business days between trade and settlement for `product`."""
    return int(_lookup("product_t_plus", product))


# Read-only views kept for callers that want the whole table (tests, chaos
# checks, admin endpoints). Mutated in place by reload_policy() so importers
# holding a reference still observe reloads.
COMPLIANCE_HOLD_THRESHOLD: dict[str, float] = dict(_policy["compliance_hold_threshold_sgd"])
PRODUCT_PRIORITY: dict[str, float] = dict(_policy["product_priority"])
QUOTE_VALIDITY_MINUTES: dict[str, int] = dict(_policy["quote_validity_minutes"])
PRODUCT_T_PLUS: dict[str, int] = dict(_policy["product_t_plus"])

_EXPORTED_TABLES = {
    "COMPLIANCE_HOLD_THRESHOLD": "compliance_hold_threshold_sgd",
    "PRODUCT_PRIORITY":          "product_priority",
    "QUOTE_VALIDITY_MINUTES":    "quote_validity_minutes",
    "PRODUCT_T_PLUS":            "product_t_plus",
}
