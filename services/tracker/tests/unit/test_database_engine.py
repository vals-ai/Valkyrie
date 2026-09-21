"""Every engine in this service must come from the time-zone-pinned factory.

Run: uv run pytest tests/unit/test_database_engine.py
"""

import ast
from pathlib import Path

SERVICE = Path(__file__).resolve().parents[2]
PINNED_FACTORY = SERVICE / "src" / "tracker" / "database" / "engine.py"
ENGINE_CONSTRUCTORS = {"create_async_engine", "create_engine", "engine_from_config"}
DRIVER_PACKAGES = {"sqlalchemy", "sqlmodel"}


def raw_engine_references(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    references: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] in DRIVER_PACKAGES:
            references += [alias.name for alias in node.names if alias.name in ENGINE_CONSTRUCTORS]

        if isinstance(node, ast.Attribute) and node.attr in ENGINE_CONSTRUCTORS:
            root = node.value
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name) and root.id in DRIVER_PACKAGES:
                references.append(f"{root.id}.{node.attr}")

    return references


def test_only_the_pinned_factory_names_a_raw_engine_constructor() -> None:
    sources = [path for root in ("src/tracker", "scripts") for path in sorted((SERVICE / root).rglob("*.py"))]
    unpinned = {
        str(path.relative_to(SERVICE)): references
        for path in sources
        if path != PINNED_FACTORY and (references := raw_engine_references(path))
    }

    assert unpinned == {}, "these modules must build engines through tracker.database.engine"
    assert raw_engine_references(PINNED_FACTORY), "the exempt module no longer holds the raw constructors"
