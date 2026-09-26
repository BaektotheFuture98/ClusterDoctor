"""The domain and port packages must stay independent of implementations."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


SOURCE_ROOT = Path(__file__).parents[2] / "src" / "cluster_doctor"
DOMAIN_ROOT = SOURCE_ROOT / "domain"
APPLICATION_ROOT = SOURCE_ROOT / "application"
ADAPTERS_ROOT = SOURCE_ROOT / "adapters"
DEEPAGENTS_ROOT = ADAPTERS_ROOT / "outbound" / "deepagents"
BOOTSTRAP_ROOT = SOURCE_ROOT / "bootstrap"

_AGENT_FRAMEWORKS = ("deepagents", "langchain", "langgraph", "litellm")


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
            APPLICATION_ROOT,
            (
                "cluster_doctor.adapters",
                "aiokafka",
                "deepagents",
                "langchain",
                "langgraph",
                "litellm",
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


def test_agent_framework_imports_are_confined_to_the_deepagents_adapter() -> None:
    violations = {
        path.relative_to(SOURCE_ROOT): sorted(
            imported
            for imported in _imports_from(path)
            if any(
                imported == framework or imported.startswith(f"{framework}.")
                for framework in _AGENT_FRAMEWORKS
            )
        )
        for path in SOURCE_ROOT.rglob("*.py")
        if not path.is_relative_to(DEEPAGENTS_ROOT)
    }
    violations = {path: imported for path, imported in violations.items() if imported}

    assert not violations, f"agent framework imports escaped deepagents: {violations}"


def test_outbound_adapters_do_not_import_concrete_siblings() -> None:
    outbound_root = ADAPTERS_ROOT / "outbound"
    violations: dict[Path, list[str]] = {}
    for path in outbound_root.rglob("*.py"):
        own_adapter = path.relative_to(outbound_root).parts[0]
        forbidden = []
        for imported in _imports_from(path):
            prefix = "cluster_doctor.adapters.outbound."
            if imported.startswith(prefix):
                sibling = imported.removeprefix(prefix).split(".", 1)[0]
                if sibling != own_adapter:
                    forbidden.append(imported)
        if forbidden:
            violations[path.relative_to(SOURCE_ROOT)] = sorted(forbidden)

    assert not violations, f"outbound adapters import concrete siblings: {violations}"


def test_bootstrap_uses_only_the_public_deepagents_api() -> None:
    allowed = "cluster_doctor.adapters.outbound.deepagents"
    violations = {
        path.relative_to(SOURCE_ROOT): sorted(
            imported
            for imported in _imports_from(path)
            if imported.startswith(f"{allowed}.")
        )
        for path in BOOTSTRAP_ROOT.rglob("*.py")
    }
    violations = {path: imported for path, imported in violations.items() if imported}

    assert not violations, f"bootstrap imports deepagents internals: {violations}"
