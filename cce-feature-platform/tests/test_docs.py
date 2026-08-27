"""Guard the claims the documentation makes about this repository.

Every check here exists because the drift it detects actually happened. The
README told readers to `cd 01_foundation/01_poc_pilot_users/dev/local_app`, a
directory that exists on this machine, is listed in `.gitignore`, and is tracked
by zero files -- so the first command of "Local Run" failed for anyone who
cloned the repository, while passing for the author. Prose cannot be trusted to
stay true on its own: the test inventory in these documents was corrected from
43 to 48 to 50 to 52 by hand over the course of one refactor, once per file,
every time by noticing rather than by measuring.

So the numbers and paths below are measured at run time and compared against
what the documents assert. A stale inventory is a failing test, not a reading
error waiting to be spotted.
"""

from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout


def _docs() -> list[Path]:
    """Markdown a clone is meant to receive: tracked, plus staged-to-be.

    `--others --exclude-standard` picks up files that are new but not ignored,
    which is what a document added in the current change looks like before it is
    committed. Ignored files are excluded on purpose -- they are the ones a clone
    will not have, and pointing at them is the defect this module detects.
    """
    listing = _git("ls-files", "--cached", "--others", "--exclude-standard", "*.md")
    return [ROOT / line for line in listing.split() if line]


def _is_ignored(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return subprocess.run(
        ["git", "check-ignore", "-q", rel], cwd=ROOT, capture_output=True
    ).returncode == 0


LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
CD = re.compile(r"^\s*cd\s+([^\s;&|]+)", re.MULTILINE)


def _navigable_targets(text: str) -> set[tuple[str, str]]:
    """Paths a reader is told to open or `cd` into, tagged with how they resolve.

    Deliberately narrower than "every path-shaped string in the file". Runtime
    output directories are named in prose too, and a gitignored `data/warehouse/`
    is correct -- it is generated. A `cd` target or a link is different: the
    reader is being sent there, so it has to be in their clone.
    """
    found = {("cd", m.group(1)) for m in CD.finditer(text)}
    for m in LINK.finditer(text):
        target = m.group(1)
        if target.startswith(("http://", "https://", "#", "mailto:")):
            continue
        found.add(("link", target.split("#")[0]))
    return {
        (kind, t)
        for kind, t in found
        if t and not any(c in t for c in "<>{}*$") and "\\" not in t
    }


def _resolve(doc: Path, kind: str, target: str) -> Path | None:
    """Where the target lands, or None if no plausible base contains it.

    A markdown link resolves against its containing document -- that is the
    format's rule, and resolving it against the repository root was this
    module's own first defect. A `cd` inside a fenced block has no such rule:
    it is relative to wherever the prose has left the reader standing, which
    here is either the repository root or the workspace above it. Accepting
    any of those keeps this check pointed at paths that exist nowhere, rather
    than turning it into a guess about the reader's shell.
    """
    bases = [doc.parent] if kind == "link" else [ROOT, ROOT.parent, doc.parent]
    for base in bases:
        candidate = base / target
        if candidate.exists():
            return candidate.resolve()
    return None


def _measured_counts() -> dict[str, int]:
    """Count tests per file by loading them, not by parsing or trusting prose."""
    loader = unittest.TestLoader()
    counts: dict[str, int] = {}
    for path in sorted(TESTS.glob("test_*.py")):
        suite = loader.discover(str(TESTS), pattern=path.name, top_level_dir=str(TESTS))
        counts[path.name] = suite.countTestCases()
    return counts


# `... unittest discover -s tests -p test_oltp.py   # 42 tests` and friends.
CLAIM = re.compile(
    r"unittest\s+(?P<cmd>[^\n#]*?)\s*#\s*(?P<n>\d+)\s*(?:tests|checks)", re.IGNORECASE
)
TARGET = re.compile(r"-p\s+(test_\w+\.py)|tests[./](test_\w+)")

# The same fact stated in prose instead of as a command: an inventory table row
# `[`tests/test_oltp.py`](tests/test_oltp.py) (42)`, or a sentence reading
# "`tests/test_oltp.py`, 42 tests". These drift exactly like the command
# comments do, and were the form the README's own table used, so matching only
# the command form would leave the table unguarded.
INLINE = re.compile(
    r"test_(?P<name>\w+)\.py`?(?:\]\([^)]*\))?`?"
    r"(?:\s*\((?P<paren>\d+)\)|,\s*(?P<comma>\d+)\s*tests)",
    re.IGNORECASE,
)


def _claims() -> list[tuple[Path, str, int, str | None]]:
    """Every documented test count, tagged with the file it speaks for.

    Fourth element is the test filename the claim is scoped to, or None when the
    command discovers everything and the number is therefore the whole-suite
    total.
    """
    out: list[tuple[Path, str, int, str | None]] = []
    for doc in _docs():
        text = doc.read_text(encoding="utf-8")
        for m in CLAIM.finditer(text):
            hit = TARGET.search(m.group("cmd"))
            scope = None
            if hit:
                scope = hit.group(1) or f"{hit.group(2)}.py"
            out.append((doc, m.group(0).strip(), int(m.group("n")), scope))
        for m in INLINE.finditer(text):
            count = m.group("paren") or m.group("comma")
            out.append(
                (doc, m.group(0).strip(), int(count), f"test_{m.group('name')}.py")
            )
    return out


class NavigableTargetsTest(unittest.TestCase):
    """A path a document sends the reader to must exist in a fresh clone."""

    def test_docs_were_found(self) -> None:
        """Vacuity guard: the checks below pass trivially on an empty list."""
        self.assertGreater(
            len(_docs()), 5, "no markdown discovered -- the git listing must be wrong"
        )

    def test_no_document_points_into_an_ignored_tree(self) -> None:
        offenders = []
        for doc in _docs():
            for kind, target in _navigable_targets(doc.read_text(encoding="utf-8")):
                resolved = _resolve(doc, kind, target)
                if resolved is None:
                    continue
                try:
                    resolved.relative_to(ROOT)
                except ValueError:
                    continue
                if _is_ignored(resolved):
                    offenders.append(f"{doc.relative_to(ROOT).as_posix()} -> {target}")
        self.assertEqual(
            sorted(offenders),
            [],
            "these paths exist here but are gitignored, so a clone will not have "
            "them; the instruction works for the author and fails for everyone else",
        )

    def test_no_document_points_at_a_missing_path(self) -> None:
        offenders = []
        for doc in _docs():
            for kind, target in _navigable_targets(doc.read_text(encoding="utf-8")):
                if _resolve(doc, kind, target) is None:
                    offenders.append(f"{doc.relative_to(ROOT).as_posix()} -> {target}")
        self.assertEqual(
            sorted(offenders),
            [],
            "documented paths that resolve to nothing from any plausible base",
        )


class TestInventoryTest(unittest.TestCase):
    """Documented test counts must equal what the loader actually finds.

    The inventory is repeated across README.md, docs/ARCHITECTURE_OLTP_BOUNDARY.md,
    L2_oltp/README.md and L2_oltp/KNOWN_GAPS.md. Four copies of one number drift
    independently, and each one is a sentence a reviewer might rely on rather
    than re-measure.
    """

    def test_index_contains_working_tree_sources(self) -> None:
        """Every .py file and every test/*.py must be tracked or staged.

        A clone receives the git index, not the working tree. If a source file
        exists here, works here, and is not tracked, the clone gets an incomplete
        package. That is the most structural defect the documentation harness can
        catch: the working tree passes every test, but the index would not.
        """
        tracked = set(
            ROOT / line
            for line in _git("ls-files", "--cached").splitlines()
            if line
        )
        present = set((ROOT / "src" / "cce_platform").rglob("*.py")) | set(
            (ROOT / "tests").rglob("test_*.py")
        )

        untracked = sorted(p.relative_to(ROOT) for p in (present - tracked))
        self.assertEqual(
            untracked,
            [],
            "these sources exist in the working tree but are not in the git index; "
            "a clone would not receive them, and imports that work here would fail there",
        )

    def test_claims_were_found(self) -> None:
        """Vacuity guard: a broken regex would silently approve every document."""
        self.assertGreater(
            len(_claims()), 3, "no documented test counts parsed -- check CLAIM"
        )

    def test_scoped_counts_match_the_suite(self) -> None:
        measured = _measured_counts()
        offenders = []
        for doc, snippet, claimed, scope in _claims():
            if scope is None:
                continue
            actual = measured.get(scope)
            if actual is None:
                offenders.append(
                    f"{doc.relative_to(ROOT).as_posix()}: names {scope}, which does not exist"
                )
            elif actual != claimed:
                offenders.append(
                    f"{doc.relative_to(ROOT).as_posix()}: {scope} claims {claimed}, "
                    f"loader finds {actual} ({snippet})"
                )
        self.assertEqual(offenders, [], "stale per-file test inventory")

    def test_whole_suite_totals_match(self) -> None:
        total = sum(_measured_counts().values())
        offenders = [
            f"{doc.relative_to(ROOT).as_posix()}: claims {claimed}, suite has {total} ({snippet})"
            for doc, snippet, claimed, scope in _claims()
            if scope is None and claimed != total
        ]
        self.assertEqual(offenders, [], "stale whole-suite test inventory")


if __name__ == "__main__":
    unittest.main()
