from cluster_doctor.incident_orchestrator_agent.agent import tools


def test_only_runtime_state_tools_are_exposed():
    assert callable(tools.make_finish_incident_tool)
    assert callable(tools.make_list_candidate_windows_tool)
    assert callable(tools.make_propose_analysis_tool)
    assert "state" not in str(tools.make_propose_analysis_tool().args_schema)
