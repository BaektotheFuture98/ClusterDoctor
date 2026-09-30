from cluster_doctor.incident_analysis_agent.agent.state import AnalysisAgentState
from cluster_doctor.incident_orchestrator_agent.agent.state import MainAgentState
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.state import (
    MinuteAnalysisState,
)
import ast
from pathlib import Path


def test_agent_owners_have_one_explicit_state_schema_each():
    assert MainAgentState.__name__ == "MainAgentState"
    assert AnalysisAgentState.__name__ == "AnalysisAgentState"
    assert MinuteAnalysisState.__name__ == "MinuteAnalysisState"


def test_analysis_state_keeps_execution_values_as_replacement_channels():
    annotations = AnalysisAgentState.__annotations__
    for field in (
        "request",
        "evidence",
        "report",
        "gaps",
        "evidence_sequence",
        "timeline",
    ):
        assert field in annotations


def test_main_state_owns_lifecycle_budget_and_output_channels():
    annotations = MainAgentState.__annotations__
    for field in (
        "analysis_call_count",
        "analyzed_minutes",
        "window_results",
        "evidence",
        "status",
    ):
        assert field in annotations


def test_state_modules_define_only_their_owner_state_and_obsolete_wrappers_are_absent():
    source = Path(__file__).resolve().parents[4] / "src" / "cluster_doctor"
    owners = {
        "incident_orchestrator_agent/agent/state.py": "MainAgentState",
        "incident_analysis_agent/agent/state.py": "AnalysisAgentState",
        "incident_analysis_agent/workflow/minute_analysis/state.py": "MinuteAnalysisState",
    }
    for path, expected in owners.items():
        tree = ast.parse((source / path).read_text())
        assert [n.name for n in tree.body if isinstance(n, ast.ClassDef)] == [expected]
    for owner in ("incident_analysis_agent", "incident_orchestrator_agent"):
        assert not (source / owner / "model" / "basemodel").exists()
        assert not (source / owner / "model" / "state").exists()
    assert not (source / "incident_analysis_agent/agent/session.py").exists()


def test_analysis_owner_has_no_reverse_import_of_orchestrator():
    source = Path(__file__).resolve().parents[4] / "src" / "cluster_doctor"
    for path in (source / "incident_analysis_agent").rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert "incident_orchestrator_agent" not in (node.module or ""), path
            elif isinstance(node, ast.Import):
                assert all(
                    "incident_orchestrator_agent" not in alias.name
                    for alias in node.names
                ), path
