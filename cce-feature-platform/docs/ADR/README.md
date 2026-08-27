# Architecture Decision Records (ADR)

This directory contains important architectural decisions made for the CCE Feature Platform project.

## Format

Each ADR follows this structure:
- **Context**: Why this decision was necessary
- **Decision**: What was chosen
- **Consequences**: Impact of this decision
- **Alternatives Considered**: What was rejected and why

## ADR Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [ADR-001](ADR-001-olap-oltp-siblings.md) | L2_olap and L2_oltp as siblings, not stack | Accepted | 2026-08-27 |
| [ADR-002](ADR-002-cart-in-olap.md) | cart_zset in OLAP instead of OLTP | Accepted | 2026-08-27 |
| [ADR-003](ADR-003-no-layer-3.md) | Why no Layer 3 | Accepted | 2026-08-27 |
| [ADR-004](ADR-004-no-reexports-olap.md) | No re-exports in L2_olap/__init__.py | Accepted | 2026-08-27 |

## Conventions

- ADRs are **immutable** once accepted
- A revised decision creates a new ADR that supersedes the old one
- Status can be: Proposed, Accepted, Deprecated, Superseded by ADR-XXX
