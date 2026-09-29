from cluster_doctor.adapters.outbound.deepagents.supervisor import tools


def test_finalize_report_tool_removed():
    assert not hasattr(tools, "make_finalize_report_tool")


def test_validate_final_report_tool_removed():
    assert not hasattr(tools, "make_validate_final_report_tool")


def test_remaining_tools_exist():
    assert callable(tools.make_finish_incident_tool)
    assert callable(tools.make_list_candidate_windows_tool)
    assert callable(tools.make_propose_analysis_tool)
