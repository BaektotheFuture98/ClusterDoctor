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


def _module_name(path: Path) -> str:
    relative = path.relative_to(SOURCE_ROOT).with_suffix("")
    parts = ("cluster_doctor", *relative.parts)
    return ".".join(parts[:-1] if relative.name == "__init__" else parts)


def _resolve_from_import(path: Path, node: ast.ImportFrom) -> set[str]:
    if node.level == 0:
        return {node.module} if node.module else set()

    package = _module_name(path)
    if path.name != "__init__.py":
        package = package.rpartition(".")[0]
    package_parts = package.split(".")
    base = package_parts[: len(package_parts) - (node.level - 1)]
    if node.module:
        return {".".join((*base, *node.module.split(".")))}
    return {".".join((*base, alias.name)) for alias in node.names}


def _imports_from(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.update(_resolve_from_import(path, node))
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


def test_bootstrap_uses_exactly_the_public_deepagents_factory_import() -> None:
    public_module = "cluster_doctor.adapters.outbound.deepagents"
    expected_names = {"DeepAgentsConfig", "build_deepagents_incident_analyzer"}
    violations: dict[Path, list[str]] = {}

    for path in BOOTSTRAP_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        invalid: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                invalid.extend(
                    alias.name
                    for alias in node.names
                    if alias.name == public_module
                    or alias.name.startswith(f"{public_module}.")
                    or public_module.startswith(f"{alias.name}.")
                )
            elif isinstance(node, ast.ImportFrom):
                imported = _resolve_from_import(path, node)
                names = {alias.name for alias in node.names}
                has_alias = any(alias.asname is not None for alias in node.names)
                for module in imported:
                    imported_names = {f"{module}.{name}" for name in names}
                    touches_deepagents = (
                        module == public_module
                        or module.startswith(f"{public_module}.")
                        or any(
                            name == public_module
                            or public_module.startswith(f"{name}.")
                            for name in imported_names
                        )
                    )
                    if not touches_deepagents:
                        continue
                    if module != public_module or names != expected_names or has_alias:
                        invalid.append(
                            f"from {node.module!r} import {sorted(names)!r}"
                        )
        if invalid:
            violations[path.relative_to(SOURCE_ROOT)] = sorted(invalid)

    assert not violations, f"bootstrap imports non-public deepagents API: {violations}"
    tree = ast.parse(
        (DEEPAGENTS_ROOT / "__init__.py").read_text(),
        filename=str(DEEPAGENTS_ROOT / "__init__.py"),
    )
    exported = next(
        (
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__all__"
                for target in node.targets
            )
        ),
        None,
    )
    assert exported == ["DeepAgentsConfig", "build_deepagents_incident_analyzer"]

    adapter_tree = ast.parse(
        (DEEPAGENTS_ROOT / "adapter.py").read_text(),
        filename=str(DEEPAGENTS_ROOT / "adapter.py"),
    )
    classes = {node.name for node in adapter_tree.body if isinstance(node, ast.ClassDef)}
    assert "DeepAgentIncidentAnalyzer" not in classes
    assert "_DeepAgentIncidentAnalyzer" in classes
