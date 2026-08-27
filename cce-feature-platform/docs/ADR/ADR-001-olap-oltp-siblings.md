# ADR-001: L2_olap and L2_oltp as siblings, not stack

**Status**: Accepted  
**Date**: 2026-08-27  
**Decision Makers**: Architecture team

## Context

The CCE platform separates code into two layer-2 domains:
- `L2_olap`: analytical side (read-and-derive)
- `L2_oltp`: transactional side (write-authority)

The question was: should these two packages be stacked (one depends on the other) or siblings (independent)?

## Decision

**`L2_olap` and `L2_oltp` are siblings at the same level (layer 2), not stacked.**

- Both depend on L0 and L1
- **Neither imports the other**
- This rule is verified by `tests/test_oltp.py::BoundaryTest`

```
L2_olap/    L2_oltp/     ← siblings, not stack
    ↓          ↓
  L1_mechanism/  L1_business_data/
         ↓
  L0_configuration/  L0_schema/  L0_primitives/
```

## Consequences

### Positive
1. **Independent deployment**: Batch pipeline and API can run without `L2_oltp` present
2. **No cycles**: Impossible to have OLAP→OLTP→OLAP
3. **Testability**: OLAP tests don't need to mock OLTP
4. **Scalability**: Analytical and transactional workloads scale independently

### Negative
1. **Potential duplication**: If logic is needed by both, it must move down to L1 or L0
2. **No direct sharing**: OLAP cannot call an OLTP function (must go through events/CDC)

### Verification
This decision is **measurable and tested**:
```bash
# This must pass
PYTHONPATH=src python -c "from cce_platform.L2_olap import pipeline"

# This too (L2_oltp absent from filesystem)
rm -rf src/cce_platform/L2_oltp
PYTHONPATH=src python -c "from cce_platform.L2_olap import pipeline"
```

## Alternatives Considered

### Alternative 1: OLAP depends on OLTP (stack)
**Rejected** because:
- Batch pipeline would depend on Redis/transactional state
- OLAP tests would require full OLTP mocking
- Violates "analytics = recomputable" principle

### Alternative 2: OLTP depends on OLAP (inverted stack)
**Rejected** because:
- OLTP is the system of record (source of truth)
- It cannot depend on derived data
- Violates write-authority principle

### Alternative 3: Layer 3 that depends on both
**Rejected** because:
- Would add a layer without clear benefit
- Would force deploying both together
- See ADR-003 for details

## References
- `README.md` section "One Naming Convention, In Two Halves"
- `docs/ARCHITECTURE_OLTP_BOUNDARY.md`
- `tests/test_oltp.py::BoundaryTest`
