---
name: spec
description: Write a falsifiable spec before implementation - functionality, constraints, output contract. Use when starting a new module, interface, or feature, or when a request is stated in vague natural language that needs pinning down before code is written.
---

# Spec

A spec is prompt engineering with an engineering discipline applied to it.
Natural language is ambiguous by default; the spec exists to remove the ambiguity
*before* implementation, not to describe implementation after the fact.

The test of a spec is falsifiability: **every clause must be something a harness
can later pass or fail.** If a clause cannot fail, delete it — it is decoration.

Bad: "the API should be performant and robust."
Good: "p99 under 200ms at 50 concurrent readers; a Redis outage degrades to the
local store and returns 200, never 500."

## The three sections

### 1. Functionality — what it does

Enumerate the capabilities as a numbered list. One capability per item, each
independently checkable. No item may contain "and" joining two behaviours; split
them instead, because a harness cannot half-fail one assertion.

### 2. Constraints — the boundaries the implementation must respect

This is the section that most often gets skipped and most often causes rework.
State explicitly:

- **Technology**: language, framework, libraries permitted and forbidden
- **Storage**: memory vs database vs file, and who owns the write authority
- **Dependency direction**: what this module may import, and what may import it
- **Boundary conditions**: empty input, absent dependency, concurrent access,
  duplicate delivery, clock skew — name the ones that apply
- **Failure mode**: for each dependency that can be unreachable, say whether the
  correct behaviour is fail-fast or degrade, and why
- **Extra artefacts**: docs, migrations, config samples that must be produced

### 3. Output contract — the shape of the result

Exact response format, field names, types, and error shape. If the caller is
code, give the schema. If the caller is a human, give a sample. Ambiguity here
is what produces the "it works but not how I meant" outcome.

## Template

```markdown
# Spec: <module or feature>

## 1. Functionality
1. <capability>
2. <capability>

## 2. Constraints
- Technology: <...>
- Storage / write authority: <...>
- Dependency direction: may import <...>; must not be imported by <...>
- Boundary conditions: <...>
- Failure mode: <dependency> unreachable -> <fail-fast | degrade to ...> because <...>
- Extra artefacts: <...>

## 3. Output contract
<schema or exact format, including the error shape>

## 4. Out of scope
<what this deliberately does not do, so absence is not read as a defect>
```

## Rules

- Write the spec **before** the code, and keep it in the conversation or in a
  file — not in your head. A spec that was never written down cannot be violated,
  which means it also cannot guide anything.
- Section 4 (out of scope) is not optional. Without it, every missing feature
  looks like a bug, and the harness score becomes meaningless.
- When the user's request is vague, do not guess the whole spec silently. Draft
  it, state the assumptions you filled in, and let them correct the draft. A
  wrong assumption caught at spec time costs one sentence; caught after
  implementation it costs a rewrite.
- A spec constrains the implementation, it does not *contain* the
  implementation. Do not write code in a spec.
- Pair this with [harness](../harness/SKILL.md): the spec says what "correct"
  means, the harness decides whether the code is correct. A spec with no harness
  is an opinion.
