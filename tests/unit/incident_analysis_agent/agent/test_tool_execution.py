from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode

from cluster_doctor.incident_analysis_agent.agent.state import AnalysisAgentState
from cluster_doctor.incident_analysis_agent.agent.tools import _build_tools
from cluster_doctor.incident_analysis_agent.agent.tools import (
    collect_update,
    report_update,
)
from cluster_doctor.incident_analysis_agent.agent.state import (
    initial_analysis_agent_state,
)
from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    LogAnalysisRequest,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.observation.builder import (
    ObservationBuilder,
)
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


@pytest.mark.parametrize(
    ("name", "args", "extra"),
    [
        ("collect_evidence", {}, {"collected": True}),
        ("write_report", {"focus": ""}, {"collected": False}),
        (
            "write_report",
            {"focus": ""},
            {"collected": True, "evidence": (object(),), "report_attempts": 2},
        ),
        ("report_insufficient", {"reason": "missing", "suggested_windows": []}, {}),
    ],
)
def test_every_tool_path_returns_a_matching_message(name, args, extra):
    builder = StateGraph(AnalysisAgentState)
    builder.add_node("tools", ToolNode(_build_tools(MagicMock())))
    builder.add_edge(START, "tools")
    builder.add_edge("tools", END)
    final = builder.compile().invoke(
        {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[{"name": name, "args": args, "id": "call-1"}],
                )
            ],
            **extra,
        }
    )
    assert isinstance(final["messages"][-1], ToolMessage)
    assert final["messages"][-1].tool_call_id == "call-1"


def request():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return LogAnalysisRequest(
        incident_id="i",
        cluster="c",
        analysis_window=TimeRange(start=start, end=start + timedelta(minutes=1)),
        evidence_sequence_start=10,
    )


def test_draft_suggestions_are_tool_output_not_agent_state():
    import json
    from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
    from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport

    req = request()
    seams = MagicMock()
    draft = seams.report_writer.draft_report.return_value
    draft.parsed_windows.return_value = [req.analysis_window]
    draft.to_domain.return_value = LogAnalysisReport(incident_id='i', analyzed_from=req.analysis_window.start, analyzed_to=req.analysis_window.end)
    graph = StateGraph(AnalysisAgentState)
    graph.add_node('tools', ToolNode(_build_tools(seams)))
    graph.add_edge(START, 'tools')
    graph.add_edge('tools', END)
    final = graph.compile().invoke({
        **initial_analysis_agent_state(req), 'collected':True,
        'evidence':(Evidence(evidence_id='E-i-1', event_time=req.analysis_window.start, source=EvidenceSource.NODE_LOG, message='a'),),
        'messages':[AIMessage(content='', tool_calls=[{'name':'write_report','args':{'focus':''},'id':'write'}])],
    })
    assert json.loads(final['messages'][-1].content)['draft_suggested_windows']
    assert 'draft_suggested_windows' not in final
    assert 'draft_suggested_windows' not in AnalysisAgentState.__annotations__


def test_collection_ids_continue_and_cache_does_not_fetch_again():
    req = request()
    state = initial_analysis_agent_state(req)
    ids = []

    def collection(window, builder):
        ids.append(collector.call_args.kwargs["new_evidence_id"]())
        return MagicMock(evidence=[], failed_minutes=set(), investigated_nodes=[])

    with patch(
        "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
    ) as collector:
        collector.return_value.collect.side_effect = collection
        update = collect_update(MagicMock(), state)
        assert collect_update(MagicMock(), {**state, **update}) == {}
        assert collector.return_value.collect.call_count == 1
    assert ids == ["E-i-11"]
    assert update["evidence_sequence"] == 11


def test_failed_minutes_are_preserved_as_structured_gaps():
    req = request()
    with patch(
        "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
    ) as collector:
        collector.return_value.collect.return_value = MagicMock(
            evidence=[],
            failed_minutes={req.analysis_window.start},
            investigated_nodes=[],
        )
        update = collect_update(MagicMock(), initial_analysis_agent_state(req))
    assert update["unresolved_gaps"] == (req.analysis_window,)
    assert update["failed_minutes"] == (req.analysis_window.start,)


def test_collection_exception_still_preserves_consumed_ids():
    req = request()

    def fail(window, builder):
        collector.call_args.kwargs["new_evidence_id"]()
        raise RuntimeError("source broke")

    with patch(
        "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
    ) as collector:
        collector.return_value.collect.side_effect = fail
        update = collect_update(MagicMock(), initial_analysis_agent_state(req))
    assert update["evidence_sequence"] == 11
    assert update["degraded"]
    assert update["unresolved_gaps"] == (req.analysis_window,)


def test_failed_report_attempts_still_consume_the_limit():
    state = {
        **initial_analysis_agent_state(request()),
        "collected": True,
        "evidence": (MagicMock(evidence_id="E-i-1"),),
    }
    seams = MagicMock()
    seams.report_writer.draft_report.side_effect = RuntimeError("writer failed")
    state = {**state, **report_update(seams, state, "")}
    state = {**state, **report_update(seams, state, "")}
    assert report_update(seams, state, "") == {}
    assert seams.report_writer.draft_report.call_count == 2


def test_observation_round_trip_retains_untruncated_master_count():
    req = request()
    builder = ObservationBuilder.from_state(
        req.analysis_window, {"master_log_total": 500}
    )
    assert builder.state_update()["master_log_total"] == 500
