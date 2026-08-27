---
name: harness
description: Build an automated quality gate for generated code - input set, execution environment, assertion logic, quantified score. Use when validating an implementation against a spec, when replacing subjective code review with a measurable gate, or when a claimed invariant has no test guarding it.
---

# Harness

A harness replaces subjective code review with a measurable gate. The point is
not "tests exist" — it is that a number comes out the other end, and that number
is comparable across runs. Human review is expensive and inconsistent; a harness
is neither.

Four parts, all four required. A harness missing any one of them is just a test
file.

## 1. Input set — the scenarios

Enumerate the data the run needs. Cover, deliberately:

- the happy path
- empty / absent input
- boundary values (exactly at a threshold, one either side)
- the adversarial case (concurrent writer, duplicate delivery, dependency down)

The adversarial case is the one that gets skipped and the one that finds real
defects. If a scenario cannot be produced against a real dependency, that is a
signal to inject a fake — see the fake rule below.

## 2. Execution environment — how it runs

State the mechanism plainly: unit process, SQL against a temp database, HTTP
against a live process, subprocess. State the isolation: temp dir, fresh schema,
no shared global state between cases. A harness that passes only when run in a
particular order is measuring the order, not the code.

## 3. Assertion logic — what "correct" means

Each assertion maps to a numbered clause of the [spec](../spec/SKILL.md). If an
assertion maps to nothing in the spec, either the spec is incomplete or the
assertion is inventing a requirement — resolve which before continuing.

Assert on **observable state**, not on internals. `the row is absent` survives a
refactor; `_cache_dict has 0 keys` does not.

## 4. Score — the quantified result

Emit `TOTAL / PASSED / FAILED` and a non-zero exit code on any failure. Per-check
names must be stable strings so a diff between two runs is readable. The score is
what makes the gate objective; without it you have output that still needs a human
to interpret, which is the thing being replaced.

## The workflow this serves

```
evaluate -> find defect -> write a test that fails on it -> classify -> fix critical
```

Each arrow has a rule:

- **find defect**: read the code against the spec. Name the defect as a failure
  scenario — concrete inputs, concrete wrong outcome. "Error handling is weak" is
  not a defect; "a second delivery of the same event_id inserts a duplicate row"
  is.
- **write a failing test**: the test must fail *before* the fix. A test written
  after the fix, never observed red, proves nothing — it may assert the wrong
  thing and pass for the wrong reason. Run it, see it red, then fix.
- **classify**: critical / non-critical, stated with a reason.
  - **critical** = silent wrong data, lost writes, a boundary invariant broken, a
    security hole, or a failure that degrades without surfacing
  - **non-critical** = cosmetic, a resource warning, an inefficiency with a
    bounded cost, a missing nicety
- **fix critical**: fix those, re-run, confirm the test flips to green. Leave the
  non-critical ones **listed and unfixed** rather than silently repaired — the
  list is evidence of judgement, and scope creep during a fix pass is how a gate
  loses its meaning.

## The fake rule

Some invariants cannot be provoked against a real dependency: retry exhaustion,
a lost optimistic-lock race, a partial write. Inject a fake through a
consumer-declared interface (`typing.Protocol` in Python — structural, so the
fake needs no base class) and drive the failure deterministically.

Two checks keep the fake honest:

1. **Conformance**: assert `isinstance(real_adapter, ThePort)` for every real
   adapter, so an adapter cannot drift from the port the fake implements.
2. **Behavioural equivalence** where it is cheap: run the same scenario against
   fake and real, assert the same observable outcome.

Without these, a passing harness may only be proving the fake works.

## Invariant harnesses

An architectural claim stated only in prose decays. When a claim is structural —
import direction, layer depth, no cycles, a schema excluding certain tables —
assert it by *scanning the tree*, not by reviewing it:

```python
for path in pkg.rglob("*.py"):
    # collect violations, assert the list is empty
```

Prefer asserting an empty violation list over a count. `assertEqual(offenders, [])`
names what broke; `assertEqual(len(offenders), 0)` does not.

## Reporting

Report what ran and what came out. If a check failed, say so with the output. If
a check was skipped, say that and why. Do not describe a harness as green without
having run it, and do not round a partial result up. When a claim in the docs
turns out to be wrong, correcting the doc is part of the fix — a false inventory
of checks is worse than no inventory.
