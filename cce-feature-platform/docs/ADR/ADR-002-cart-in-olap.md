# ADR-002: cart_zset in L2_olap instead of L2_oltp

**Status**: Accepted  
**Date**: 2026-08-27  
**Decision Makers**: Architecture team

## Context

`cart_zset.py` manages financial product shopping cart state. It's mutable, authoritative state for the user. The natural question: why is it in `L2_olap` (analytics) and not `L2_oltp` (transactional)?

## Decision

**`cart_zset.py` stays in `L2_olap`** despite managing mutable state.

## Reasoning

### Why OLAP?
1. **No regulatory obligation**: A lost cart has no legal consequence
   - Unlike `outbox_events` (delivery obligation)
   - Unlike `settlement_schedule` (contractual T+2)
   
2. **Derivable state**: Cart can be reconstructed from user behavior
   - CDC events `cart:add` / `cart:remove` pass through Kafka
   - Pipeline could recreate carts from these events
   - Even if we don't do it today, it's **possible**

3. **Analytical nature**: Cart primarily serves:
   - Computing `rt_cart_value_1d` features for propensity scoring
   - Detecting abandoned carts for marketing retargeting
   - Frontend display (read-heavy)

4. **No saga/compensation**: A `cart.clear()` doesn't require complex rollback

### Key distinction: Replaceable vs. Irreplaceable State
```
OLAP (replaceable):
  - Cart: can be reconstructed from events
  - Aggregated features: recomputable from Gold
  - Segmentation: re-run clustering

OLTP (irreplaceable):
  - Undelivered outbox: must be replayed, not recalculated
  - Settlement schedule: contractual T+2 obligation
  - Transaction history: append-only, immutable
```

## Consequences

### Positive
1. **Simplicity**: Analytics features and cart live together
2. **Deployment**: Frontend can read cart without OLTP active
3. **No boundary cross**: Pipeline doesn't need to import OLTP

### Negative
1. **Semantic confusion**: "Analytics" seems odd for a shopping cart
2. **Loss risk**: If Redis fails and local fallback is per-process, carts diverge
   - **Mitigated by**: `cart_zset.py` docstring explains replica constraint

### Accepted trade-off
Cart is **closer to cache than system of record**.

## Alternatives Considered

### Alternative 1: Move cart_zset to L2_oltp
**Rejected** because:
- Would break OLAP/OLTP independence (pipeline would import OLTP for cart features)
- Would add complexity without regulatory benefit
- Carts don't require ACID guarantees of OLTP

### Alternative 2: Create separate L2_session_state
**Rejected** because:
- Overhead of entire package for one module
- No other obvious candidate for this package
- Cart is conceptually "state derived from behavior"

## References
- `src/cce_platform/L2_olap/cart_zset.py` docstring
- `src/cce_platform/L2_olap/__init__.py` line 21
- `src/cce_platform/L2_oltp/__init__.py` section "What is deliberately excluded"
