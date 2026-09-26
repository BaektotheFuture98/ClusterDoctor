import ast
from pathlib import Path


def test_manual_script_uses_manual_diagnosis_without_private_composition_internals():
    path = Path(__file__).parents[2] / "scripts" / "run_diagnosis.py"
    tree = ast.parse(path.read_text())
    source = path.read_text()

    assert "RunManualDiagnosis" in source
    assert "._orchestrator" not in source
    assert "._pending" not in source
    assert any(
        isinstance(node, ast.Attribute)
        and node.attr == "handle"
        and isinstance(node.value, ast.Name)
        and node.value.id == "manual_diagnosis"
        for node in ast.walk(tree)
    )
