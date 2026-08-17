"""The README tables are a deliverable, so they are tested like one.

The rule is one line — adding a test file means also adding its row to the test matrix in
README.md — and until now the only thing enforcing it was whoever remembered. That is the
same class of guarantee as "remember to run the clean-start check": correct, written
down, and dependent on a human under time pressure.

The failure it prevents is quiet and asymmetric. A missing row costs a reviewer nothing
they will notice; a *stale* row is worse, because it describes coverage that no longer
exists. A reader of the matrix has no way to tell either from the outside, which is
exactly why it belongs in CI rather than in a checklist.

Two directions, both cheap:

  - every `tests/test_*.py` on disk is named somewhere in README.md
  - every `tests/test_*.py` named in README.md is on disk

Plus the same shape for `docs/`, which drifted the other way once already: a document
that exists but is linked from nowhere is a document nobody opens.

And then the citations that name a *specific test*, anywhere in the prose. Those are the
ones that rot silently, because the two checks above cannot see them: they only read
README rows, and only at file granularity. Reorganising the suite into topic files
deleted a file that `docs/OVERVIEW.md` cited by name; the citation had to come out by
hand, and nothing here would have failed if it had not, because a `.md` is not imported
by anything. A citation is the only claim in this repo that nothing executes, which is
exactly why it needs a test rather than a habit.
"""

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent
README = REPO / "README.md"

# Rows cite paths in backticks (`tests/test_flood.py`); the Docs section uses markdown
# links. One expression per shape, both anchored on the path so prose cannot match.
TEST_CITATION = re.compile(r"`tests/(test_[a-z0-9_]+\.py)`")
DOC_CITATION = re.compile(r"\(docs/([A-Za-z0-9_]+\.md)\)")

# Anywhere in the prose, backticked or bare, with an optional pytest node id. Anchored on
# the `tests/` prefix so no sentence can match by accident.
ANY_CITATION = re.compile(r"tests/(test_[a-z0-9_]+\.py)(?:::([A-Za-z0-9_]+))?")

# `raft.py::submit` — a shipped document naming a symbol in the tree. Anchored on the
# backticks AND on the `::`, so ordinary prose about a module ("see raft.py") cannot
# match; only the deliberate form is a claim this test has to honour.
SOURCE_CITATION = re.compile(r"`([a-z_]+\.py)::([A-Za-z_][A-Za-z0-9_]*)`")

# Where a cited module may live. docs/CODE_TOUR.md cites the package and the one script
# that is a documented entrypoint; nothing else is addressable by module name alone.
SOURCE_ROOTS = [REPO / "src" / "raftkv", REPO / "scripts"]

# Every markdown file a reader is handed, which is exactly the set that can make a claim
# about the tree. The glob is non-recursive because `docs/` is flat by design.
PROSE = [p for p in (README, *sorted((REPO / "docs").glob("*.md"))) if p.is_file()]


def readme() -> str:
    return README.read_text(encoding="utf-8")


def citations() -> list[tuple[pathlib.Path, str, str | None]]:
    """(document, test file, test name or None) for every `tests/...` reference shipped."""
    found = []
    for doc in PROSE:
        for filename, test_name in ANY_CITATION.findall(doc.read_text(encoding="utf-8")):
            found.append((doc, filename, test_name or None))
    # A regex that stopped matching would make both tests below pass forever while
    # checking nothing, so pin the floor. The count only ever grows.
    assert len(found) > 50, f"the citation regex found only {len(found)} references; it is broken"
    return found


def test_every_test_file_has_a_readme_row():
    on_disk = {p.name for p in (REPO / "tests").glob("test_*.py")}
    cited = set(TEST_CITATION.findall(readme()))
    missing = sorted(on_disk - cited)
    assert not missing, (
        f"{len(missing)} test file(s) exist but appear nowhere in README.md: "
        f"{', '.join(missing)}. The repo map and the test matrix are the deliverable — "
        "add a row describing what the file proves."
    )


def test_no_readme_row_describes_a_test_that_was_deleted():
    on_disk = {p.name for p in (REPO / "tests").glob("test_*.py")}
    cited = set(TEST_CITATION.findall(readme()))
    stale = sorted(cited - on_disk)
    assert not stale, (
        f"README.md still describes {len(stale)} test file(s) that no longer exist: "
        f"{', '.join(stale)}. A row that outlives its file claims coverage the repo "
        "does not have."
    )


def test_every_doc_is_linked_from_the_readme():
    on_disk = {p.name for p in (REPO / "docs").glob("*.md")}
    linked = set(DOC_CITATION.findall(readme()))
    unlinked = sorted(on_disk - linked)
    assert not unlinked, (
        f"docs/ contains {len(unlinked)} document(s) the README never links: "
        f"{', '.join(unlinked)}. An unlinked document is one nobody opens."
    )


def test_every_readme_doc_link_resolves():
    linked = set(DOC_CITATION.findall(readme()))
    broken = sorted(name for name in linked if not (REPO / "docs" / name).is_file())
    assert not broken, (
        f"README.md links {len(broken)} document(s) that do not exist: {', '.join(broken)}"
    )


def test_every_cited_test_file_exists():
    """The README pair above only reads README rows. This reads every document, so a
    citation buried in a paragraph of docs/ is held to the same standard."""
    broken = sorted(
        {
            f"{doc.name} cites tests/{filename}"
            for doc, filename, _ in citations()
            if not (REPO / "tests" / filename).is_file()
        }
    )
    assert not broken, (
        f"{len(broken)} citation(s) name a test file that does not exist:\n  "
        + "\n  ".join(broken)
        + "\nA deleted or renamed test file leaves the prose pointing at nothing, and "
        "markdown is the one thing here that no import can catch."
    )


def source_citations() -> list[tuple[pathlib.Path, str, str]]:
    """(document, module file, symbol) for every `module.py::symbol` reference shipped."""
    found = []
    for doc in PROSE:
        for filename, symbol in SOURCE_CITATION.findall(doc.read_text(encoding="utf-8")):
            found.append((doc, filename, symbol))
    # Same floor reasoning as citations(): a regex that quietly stopped matching would
    # leave both checks below passing forever while verifying nothing.
    assert len(found) > 40, (
        f"the source-citation regex found only {len(found)} references; it is broken"
    )
    return found


def resolve_module(filename: str) -> pathlib.Path | None:
    for root in SOURCE_ROOTS:
        candidate = root / filename
        if candidate.is_file():
            return candidate
    return None


# A symbol is defined if it is a function, a method, a class, or a module-level constant.
# All four appear in the tour: `raft.py::submit`, `models.py::MAX_ENTRIES_PER_APPEND`,
# `transport.py::Transport`, `storage.py::_SCHEMA`.
DEFINITION = (
    r"^[ \t]*(?:async[ \t]+)?def[ \t]+{name}[ \t]*\("
    r"|^[ \t]*class[ \t]+{name}\b"
    r"|^[ \t]*{name}[ \t]*(?::[^=\n]+)?="
)


def test_every_cited_source_module_exists():
    """`docs/CODE_TOUR.md` navigates by symbol rather than by line number, because a line
    number in prose rots on the first edit to the file it points at and nothing notices.
    A symbol can be checked against the tree; that is the entire reason for the form."""
    broken = sorted(
        {
            f"{doc.name} cites {filename}"
            for doc, filename, _ in source_citations()
            if resolve_module(filename) is None
        }
    )
    assert not broken, (
        f"{len(broken)} citation(s) name a module that does not exist:\n  "
        + "\n  ".join(broken)
        + "\nA renamed or deleted module leaves the navigation pointing at nothing."
    )


def test_every_cited_source_symbol_exists():
    """The stronger half, and the one that catches an ordinary refactor: the module
    survives, the function inside it is renamed, and every document that walked a reader
    to it now walks them somewhere else. This is `test_every_cited_test_name_exists`
    pointed at `src/` instead of `tests/`."""
    broken = []
    for doc, filename, symbol in source_citations():
        path = resolve_module(filename)
        if path is None:
            continue  # already reported above; do not double-count
        pattern = DEFINITION.format(name=re.escape(symbol))
        if not re.search(pattern, path.read_text(encoding="utf-8"), re.MULTILINE):
            broken.append(f"{doc.name} cites {filename}::{symbol}, which is not defined there")
    assert not broken, (
        f"{len(broken)} citation(s) name a symbol that no longer exists:\n  "
        + "\n  ".join(sorted(set(broken)))
        + "\nThe cited symbol is where the paragraph sends the reader; without it the "
        "navigation is worse than absent, because it looks precise."
    )


def test_every_cited_test_name_exists():
    """`file.py::test_name` is a stronger claim than `file.py`, and it fails in a way the
    file-level check cannot see: the file survives a refactor, the test inside it does
    not. Renaming a test is the ordinary move that breaks this."""
    definition = "(?:async +)?def +{} *\\("
    broken = []
    for doc, filename, test_name in citations():
        if test_name is None:
            continue
        path = REPO / "tests" / filename
        if not path.is_file():
            continue  # already reported by the check above; do not double-count
        if not re.search(definition.format(re.escape(test_name)), path.read_text(encoding="utf-8")):
            broken.append(f"{doc.name} cites {filename}::{test_name}, which is not defined there")
    assert not broken, (
        f"{len(broken)} citation(s) name a test that no longer exists:\n  "
        + "\n  ".join(sorted(set(broken)))
        + "\nThe cited test is the evidence for the claim around it; without it the "
        "paragraph asserts something the suite does not check."
    )
