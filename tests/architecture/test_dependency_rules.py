"""The domain and port packages must stay independent of implementations."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


SOURCE_ROOT = Path(__file__).parents[2] / "src" / "cluster_doctor"
DOMAIN_ROOT = SOURCE_ROOT / "domain"
PORTS_ROOT = SOURCE_ROOT / "application" / "ports"


def _imports_from(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


@pytest.mark.parametrize(
    ("root", "forbidden"),
    [
        (DOMAIN_ROOT, ("cluster_doctor.application", "cluster_doctor.adapters")),
        (
            PORTS_ROOT,
            (
                "cluster_doctor.adapters",
                "deepagents",
                "langchain",
                "langgraph",
            ),
        ),
    ],
)
def test_boundary_packages_do_not_depend_on_forbidden_layers(
    root: Path, forbidden: tuple[str, ...]
) -> None:
    assert root.is_dir(), f"missing boundary package: {root.relative_to(SOURCE_ROOT)}"

    violations = {
        path.relative_to(SOURCE_ROOT): sorted(
            imported
            for imported in _imports_from(path)
            if any(imported == prefix or imported.startswith(f"{prefix}.") for prefix in forbidden)
        )
        for path in root.rglob("*.py")
    }
    violations = {path: imported for path, imported in violations.items() if imported}

    assert not violations, f"forbidden boundary imports: {violations}"
