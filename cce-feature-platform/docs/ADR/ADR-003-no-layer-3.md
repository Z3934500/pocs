# ADR-003: Why no Layer 3

**Status**: Accepted  
**Date**: 2026-08-27  
**Decision Makers**: Architecture team

## Context

The CCE platform uses a 3-layer stratification (L0, L1, L2). The natural question: why stop at L2? Why not have an L3 that orchestrates OLAP and OLTP together?

## Decision

**No Layer 3.** The platform stops at L2, with OLAP and OLTP as siblings.

## Reasoning

### What would a Layer 3 contain?

A hypothetical L3 would need to:
- Depend on both `L2_olap` and `L2_oltp`
- Orchestrate workflows touching both domains
- Example: "create OLTP transaction, then trigger OLAP recalculation"

### Why it's not necessary

1. **Event-driven coupling, not direct calls**
   - OLTP publishes events (outbox → Kafka)
   - OLAP consumes these events (CDC → realtime.py)
   - No need for a layer that "calls both"

2. **Complex orchestrations live elsewhere**
   - **Frontend BFF**: orchestrates OLAP + OLTP API calls from client
   - **Saga patterns**: managed in `L2_oltp/state_machine.py` (compensation)
   - **Airflow/Databricks workflows**: for complex batch jobs

3. **Single Responsibility Principle**
   - L2_olap: "derive and serve features"
   - L2_oltp: "manage authoritative state"
   - L3 would be: "orchestrate both" → but that's the **application layer** role (frontend, API gateway)

4. **Testability**
   - Today: independent OLAP tests, independent OLTP tests
   - With L3: integration tests required for everything

### Where does orchestration live today?

```
[Frontend/BFF]  ← "virtual L3", not in this repo
    ↓       ↓
  OLAP    OLTP   ← siblings, communicate via events
```

Concrete example:
```typescript
// Frontend (virtual L3)
async function submitOrder(customerId: string, products: Product[]) {
  // 1. OLTP call: create transaction
  const txn = await fetch('/oltp/transactions', { ... });
  
  // 2. OLAP call: get features for scoring
  const features = await fetch('/olap/features/' + customerId);
  
  // 3. Business logic in frontend
  if (features.propensity_score < 0.3) {
    showUpsellOffer();
  }
}
```

This code **doesn't live in `cce_platform`** because it's application logic, not platform logic.

## Consequences

### Positive
1. **Clear separation**: Platform provides primitives, application orchestrates them
2. **Flexible deployment**: OLAP and OLTP scale independently
3. **Simple tests**: No mandatory integration tests

### Negative
1. **No "one-stop shop"**: Developers must orchestrate from client
2. **Potential duplication**: Multiple clients might duplicate same orchestration
   - **Mitigated by**: If orchestration becomes common, create separate BFF service

## When to reconsider this decision

Create L3 **if and only if**:
1. **Recurring pattern**: 5+ identical workflows repeated in 3+ clients
2. **Complex business logic**: Orchestration contains business rules that must be centralized
3. **Transactional constraint**: Need distributed ACID transaction cross-domain

**For now, none of these criteria are met.**

## Alternatives Considered

### Alternative 1: L3 with Saga orchestrations
**Rejected** because:
- Sagas already in `L2_oltp/state_machine.py` (compensation)
- No need for separate layer for single pattern

### Alternative 2: L3 = BFF in this repo
**Rejected** because:
- Mixes concerns (platform vs. application)
- Forces monolithic deployment
- BFFs evolve faster than platform

### Alternative 3: L3 = Event orchestrator (workflow engine)
**Rejected** because:
- Airflow/Temporal/Step Functions already do this
- No need to reinvent workflow engine

## References
- ADR-001: OLAP and OLTP as siblings
- `docs/ARCHITECTURE_OLTP_BOUNDARY.md` section "The boundary is enforced"
- Martin Fowler, "Event-Driven Architecture"
