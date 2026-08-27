"""Harness for the package's layer invariants.

The README declares a dependency stratification: `L0_primitives`,
`L0_configuration` and `L0_schema` reach nothing internal; `L1_mechanism` and
`L1_business_data` reach only into those; `L2_olap` and `L2_oltp` sit above both.
Stated only in prose, that table decays — the first import that contradicts it
would be invisible until someone re-derived the graph by hand.

Structured as a harness:

  Input set    every .py file under src/cce_platform/, parsed as an AST so that
               deferred and TYPE_CHECKING imports count the same as top-level
  Environment  in-process, filesystem-only; nothing under test is imported, so a
               module that fails to import still gets checked
  Assertions   layer 0 reaches nothing, layer 1 reaches only layer 0, the graph
               is acyclic, every folder carries an `L<n>_` prefix, and each
               prefix equals the depth actually measured from the graph
  Score        unittest's own pass count; all must pass

Related to `BoundaryTest` in test_oltp.py but not implied by it: that one asserts
the analytics side never imports `L2_oltp` (a write-authority rule), this one
asserts dependency depth (a layering rule). Either can break while the other
holds.

AST rather than line matching: `from ..L0_configuration import settings` nested
inside a function body is still a dependency, and a regex anchored to the start
of a line would miss it.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "src" / "cce_platform"
PKG_NAME = "cce_platform"

# Named here rather than globbed off disk so that deleting or renaming a folder
# fails loudly instead of shrinking the checked set to nothing.
DECLARED = {
    "L0_configuration",
    "L0_primitives",
    "L0_schema",
    "L1_business_data",
    "L1_mechanism",
    "L2_olap",
    "L2_oltp",
}

PREFIX = re.compile(r"^L(?P<layer>\d)_")


def _declared_layer(unit: str) -> int | None:
    """The layer a folder name claims, or None for an unprefixed unit."""
    hit = PREFIX.match(unit)
    return int(hit.group("layer")) if hit else None


LAYER_0 = {u for u in DECLARED if _declared_layer(u) == 0}
LAYER_1 = {u for u in DECLARED if _declared_layer(u) == 1}


def _unit_of(rel: Path) -> str:
    """The layering unit a file belongs to: its folder, or its own name.

    Folder packages are one unit (`L1_mechanism/db.py` and
    `L1_mechanism/kv_backend.py` are both "L1_mechanism") because the layer table
    is stated per folder. Flat top-level modules are their own unit.
    """
    return rel.parts[0] if len(rel.parts) > 1 else rel.stem


def _package_parts(rel: Path) -> tuple[str, ...]:
    """The dotted package a file's relative imports resolve against."""
    return (PKG_NAME,) + rel.parts[:-1]


def _internal_unit(full: tuple[str, ...]) -> str | None:
    """Map a resolved dotted module to its layering unit, or None if external."""
    if len(full) < 2 or full[0] != PKG_NAME:
        return None
    return full[1]


def _targets(node: ast.AST, base: tuple[str, ...]) -> list[tuple[str, ...]]:
    """Resolve one import node to fully-qualified dotted paths.

    Relative imports are resolved against `base`, the package the file lives in,
    so `from ..L0_configuration import settings` inside `L1_mechanism/db.py`
    resolves to `cce_platform.L0_configuration` rather than being guessed from
    the text.
    """
    if isinstance(node, ast.Import):
        return [tuple(alias.name.split(".")) for alias in node.names]

    if not isinstance(node, ast.ImportFrom):
        return []

    if node.level == 0:
        return [tuple(node.module.split("."))] if node.module else []

    # level=1 is the current package, level=2 its parent, and so on.
    anchor = base[: len(base) - (node.level - 1)]
    if node.module:
        return [anchor + tuple(node.module.split("."))]

    # `from . import x` / `from .. import x`: each name is itself a submodule.
    return [anchor + (alias.name,) for alias in node.names]


def _records() -> list[tuple[str, int, str, str]]:
    """(file, line, importing unit, imported unit) for every internal import.

    Parsed from the AST so that imports inside function bodies and
    `if TYPE_CHECKING:` blocks are counted — a dependency is a dependency
    regardless of where the statement sits.
    """
    out: list[tuple[str, int, str, str]] = []
    for path in sorted(PKG.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(PKG)
        own = _unit_of(rel)
        base = _package_parts(rel)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            for full in _targets(node, base):
                dep = _internal_unit(full)
                if dep is not None and dep != own:
                    out.append((rel.as_posix(), node.lineno, own, dep))
    return out


class LayerTest(unittest.TestCase):
    """The layering invariant: dependency arrows point down, never up or sideways."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.records = _records()

    def test_declared_layers_exist_on_disk(self) -> None:
        """Guards the harness itself: a renamed folder must not silently pass.

        Without this, deleting `L0_primitives/` would make every layer-0
        assertion vacuously true — the set would be empty and nothing would fail.
        """
        present = {p.name for p in PKG.iterdir() if p.is_dir() and p.name != "__pycache__"}
        missing = sorted(DECLARED - present)
        self.assertEqual(
            missing, [],
            "the layer table names folders that do not exist; update tests/test_layers.py "
            "and the README table together, or the assertions below prove nothing",
        )

    def test_every_folder_carries_a_layer_prefix(self) -> None:
        """No unprefixed folder may appear, or the scheme becomes decorative.

        A single folder without an `L<n>_` prefix reintroduces exactly the
        ambiguity the prefixes were added to remove: a reader cannot tell whether
        it is unlayered or merely unlabelled.
        """
        present = {p.name for p in PKG.iterdir() if p.is_dir() and p.name != "__pycache__"}
        unprefixed = sorted(u for u in present if _declared_layer(u) is None)
        self.assertEqual(
            unprefixed, [],
            "every package folder must carry an L<n>_ prefix; an unlabelled folder "
            "makes the scheme decorative rather than total",
        )

    def test_prefix_matches_the_measured_depth(self) -> None:
        """The number in the folder name must equal the computed depth.

        This is the test that makes the prefixes worth having. `L1_business_data`
        is layer 1 only because it imports nothing above layer 0 — that is a
        measured property, not an identity. Add one import from
        `L1_business_data` into `L1_mechanism` and the true depth becomes 2 while
        the name still reads `L1_`, so the folder name would be a false claim
        with nothing to contradict it.

        Depth is the longest chain from a unit down to one with no internal
        imports, computed over the same AST records as the checks above rather
        than assumed from the names.
        """
        graph: dict[str, set[str]] = {}
        for _f, _n, src, dep in self.records:
            graph.setdefault(src, set()).add(dep)

        # Cycles are caught by their own test; guard here so a cycle surfaces
        # there as one clear failure rather than as a RecursionError.
        depth_cache: dict[str, int] = {}

        def depth(unit: str, trail: frozenset[str] = frozenset()) -> int:
            if unit in trail:
                return 0
            if unit in depth_cache:
                return depth_cache[unit]
            deps = graph.get(unit, set())
            value = 0 if not deps else 1 + max(
                depth(d, trail | {unit}) for d in sorted(deps)
            )
            depth_cache[unit] = value
            return value

        offenders = []
        for unit in sorted(DECLARED):
            claimed = _declared_layer(unit)
            measured = depth(unit)
            if claimed != measured:
                arrows = sorted(
                    f"{f}:{n} -> {dep}"
                    for f, n, src, dep in self.records
                    if src == unit
                )
                offenders.append(
                    f"{unit} claims L{claimed} but measures {measured} "
                    f"(imports: {', '.join(arrows) or 'none'})"
                )
        self.assertEqual(
            offenders, [],
            "a folder's L<n>_ prefix must equal its measured dependency depth; "
            "otherwise the name is an unchecked claim that drifts the first time "
            "someone adds an import",
        )

    def test_layer_0_imports_nothing_internal(self) -> None:
        """Layer 0 is what makes sharing safe: importing it drags in nothing."""
        offenders = [
            f"{f}:{n}: {src} -> {dep}"
            for f, n, src, dep in self.records
            if src in LAYER_0
        ]
        self.assertEqual(
            offenders, [],
            "layer 0 must have no internal dependencies at all; both sides of the "
            "write-authority boundary import it, so a dependency here propagates "
            "into every consumer",
        )

    def test_layer_1_reaches_only_layer_0(self) -> None:
        """Layer 1 may reach down, never up and never sideways to its peers."""
        offenders = [
            f"{f}:{n}: {src} -> {dep}"
            for f, n, src, dep in self.records
            if src in LAYER_1 and dep not in LAYER_0
        ]
        self.assertEqual(
            offenders, [],
            "layer 1 may import only layer 0 (primitives, configuration, schema); "
            "a sideways edge between mechanism and business_data would couple the "
            "means of reaching a store to the numbers being stored, and an upward "
            "edge would invert the stratification entirely",
        )

    def test_import_graph_is_acyclic(self) -> None:
        """A cycle would make 'layer' meaningless: no unit could be ordered."""
        graph: dict[str, set[str]] = {}
        for _f, _n, src, dep in self.records:
            graph.setdefault(src, set()).add(dep)

        cycles: list[str] = []
        WHITE, GREY, BLACK = 0, 1, 2
        colour: dict[str, int] = {}

        def visit(unit: str, trail: list[str]) -> None:
            colour[unit] = GREY
            for nxt in sorted(graph.get(unit, ())):
                state = colour.get(nxt, WHITE)
                if state == GREY:
                    start = trail.index(nxt) if nxt in trail else 0
                    cycles.append(" -> ".join(trail[start:] + [nxt]))
                elif state == WHITE:
                    visit(nxt, trail + [nxt])
            colour[unit] = BLACK

        for unit in sorted(graph):
            if colour.get(unit, WHITE) == WHITE:
                visit(unit, [unit])

        self.assertEqual(
            sorted(set(cycles)), [],
            "the internal import graph must be a DAG; a cycle means the layer "
            "table in the README cannot be derived, because no consistent depth "
            "exists for the units involved",
        )

    def test_records_are_not_empty(self) -> None:
        """Guards against a silent parse failure making every check vacuous."""
        self.assertGreater(
            len(self.records), 20,
            "the AST scan found almost no internal imports, which means the scan "
            "is broken rather than the package being unusually clean",
        )


if __name__ == "__main__":
    unittest.main()
