from datetime import datetime, timezone

from cluster_doctor.incident_orchestrator_agent.agent import tools
from cluster_doctor.incident_orchestrator_agent.model.state.incident_state import IncidentState


def test_finalize_report_tool_removed():
    assert not hasattr(tools, "make_finalize_report_tool")


def test_validate_final_report_tool_removed():
    assert not hasattr(tools, "make_validate_final_report_tool")


def test_remaining_tools_exist():
    assert callable(tools.make_finish_incident_tool)
    assert callable(tools.make_list_candidate_windows_tool)
    assert callable(tools.make_propose_analysis_tool)


def test_list_candidate_windows_reports_when_inflow_stopped():
    state = IncidentState(
        incident_id="INC-1",
        observed_end=datetime(2026, 1, 1, 13, 55, tzinfo=timezone.utc),
    )
    tool = tools.make_list_candidate_windows_tool(state=state)

    result = tool.invoke({})

    assert "유입이 멎은 시각" in result


def test_list_candidate_windows_omits_line_when_observed_end_unset():
    state = IncidentState(incident_id="INC-1")
    tool = tools.make_list_candidate_windows_tool(state=state)

    result = tool.invoke({})

    assert "유입이 멎은 시각" not in result
