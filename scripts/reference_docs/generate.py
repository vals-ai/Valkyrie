"""Write or check generated reference files without touching handwritten content."""

from __future__ import annotations

import argparse
import difflib
from collections.abc import Sequence
from pathlib import Path

from .model import DOCS_ROOT, GENERATED_MARKER, REFERENCE_ROOT
from .render import render_reference

_MAX_DIFF_LINES = 80


def _obsolete_files(docs_root: Path, expected: set[Path]) -> tuple[Path, ...]:
    root = docs_root / REFERENCE_ROOT
    if not root.exists():
        return ()
    obsolete = (
        path.relative_to(docs_root)
        for path in root.rglob("*.mdx")
        if path.relative_to(docs_root) not in expected and GENERATED_MARKER in path.read_text(encoding="utf-8")
    )
    return tuple(sorted(obsolete))


def write_reference(docs_root: Path = DOCS_ROOT) -> tuple[Path, ...]:
    """Write the manifest and remove only obsolete marked files."""
    rendered = render_reference()
    for relative_path in rendered:
        output = docs_root / relative_path
        if output.suffix == ".mdx" and output.exists() and GENERATED_MARKER not in output.read_text(encoding="utf-8"):
            raise FileExistsError(f"Refusing to overwrite unmarked file: {relative_path}")
    obsolete = _obsolete_files(docs_root, set(rendered))
    for relative_path in obsolete:
        (docs_root / relative_path).unlink()
    for relative_path, content in rendered.items():
        output = docs_root / relative_path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(content, encoding="utf-8", newline="\n")
    return obsolete


def check_reference(docs_root: Path = DOCS_ROOT) -> tuple[str, ...]:
    """Return bounded diagnostics for generated-file drift."""
    rendered = render_reference()
    diagnostics: list[str] = []
    for relative_path, expected in rendered.items():
        output = docs_root / relative_path
        if not output.exists():
            diagnostics.append(f"missing: {relative_path}")
            continue
        actual = output.read_text(encoding="utf-8")
        if actual == expected:
            continue
        diagnostics.append(f"stale: {relative_path}")
        diff = difflib.unified_diff(
            actual.splitlines(),
            expected.splitlines(),
            fromfile=str(relative_path),
            tofile=f"generated/{relative_path}",
            lineterm="",
        )
        diagnostics.extend(list(diff)[:_MAX_DIFF_LINES])
    diagnostics.extend(f"unexpected generated file: {path}" for path in _obsolete_files(docs_root, set(rendered)))
    return tuple(diagnostics)


def main(argv: Sequence[str] | None = None, docs_root: Path = DOCS_ROOT) -> int:
    """Generate reference pages or verify committed output."""
    parser = argparse.ArgumentParser(
        description="Generate deterministic Mintlify reference pages from the public CLI and SDK APIs."
    )
    parser.add_argument("--check", action="store_true", help="Check generated files without writing them")
    arguments = parser.parse_args(argv)
    if arguments.check:
        diagnostics = check_reference(docs_root)
        if diagnostics:
            print("Generated documentation reference is stale:")
            print("\n".join(diagnostics))
            print("Run `make docs-reference` and commit the generated files.")
            return 1
        print("Generated documentation reference is current.")
        return 0
    obsolete = write_reference(docs_root)
    print("Generated documentation reference.")
    if obsolete:
        print(f"Removed {len(obsolete)} obsolete generated files.")
    return 0
