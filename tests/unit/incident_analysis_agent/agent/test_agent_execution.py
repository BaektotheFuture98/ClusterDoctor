from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from langchain.agents import create_agent
from langchain.tools import ToolRuntime, tool
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.types import Command

from cluster_doctor.incident_analysis_agent.agent.runtime.tool_batch import (
    AnalysisToolAdmissionMiddleware,
)
from cluster_doctor.incident_analysis_agent.agent.state import (
    AnalysisAgentState,
    initial_analysis_agent_state,
)
from cluster_doctor.incident_analysis_agent.agent.subagent import run_analysis_agent
from cluster_doctor.incident_analysis_agent.agent.tools import _build_tools
from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    LogAnalysisRequest,
)
from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.report_generation.schema import (
    DraftReport,
)
from cluster_doctor.incident_orchestrator_agent.agent.middleware import (
    DelegationGuardrailMiddleware,
)
from cluster_doctor.incident_orchestrator_agent.agent.state import (
    MainAgentState,
    initial_main_agent_state,
)
from cluster_doctor.incident_orchestrator_agent.agent.tools import (
    make_finish_incident_tool,
    make_propose_analysis_tool,
)
from cluster_doctor.incident_orchestrator_agent.agent.adapter import (
    _DeepAgentIncidentAnalyzer,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
)


class ScriptedModel(BaseChatModel):
    responses: list[AIMessage]
    index: int = 0

    @property
    def _llm_type(self):
        return "scripted"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        response = self.responses[self.index]
        self.index += 1
        return ChatResult(generations=[ChatGeneration(message=response)])


def call(name, args, id):
    return {"name": name, "args": args, "id": id}


def request():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return LogAnalysisRequest(
        incident_id="i",
        cluster="c",
        analysis_window=TimeRange(start=start, end=start + timedelta(minutes=1)),
        analysis_goal="find the queue failure",
    )


@pytest.mark.parametrize(
    "name,args", [("collect_evidence", {}), ("write_report", {"focus": ""})]
)
def test_analysis_graph_executes_only_first_mutating_tool(name, args):
    seams = MagicMock()
    seams.report_writer.draft_report.return_value = DraftReport(summary="draft")
    req = request()
    evidence = Evidence(
        evidence_id="E-i-1",
        event_time=req.analysis_window.start,
        source=EvidenceSource.NODE_LOG,
        message="a",
    )
    initial = {
        **initial_analysis_agent_state(req),
        "messages": [HumanMessage(content="start")],
    }
    if name == "write_report":
        initial = {**initial, "collected": True, "evidence": (evidence,)}
    model = ScriptedModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[call(name, args, "one"), call(name, args, "two")],
            ),
            AIMessage(content="done"),
        ]
    )
    graph = create_agent(
        model,
        tools=_build_tools(seams),
        state_schema=AnalysisAgentState,
        middleware=[AnalysisToolAdmissionMiddleware()],
    )
    with patch(
        "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
    ) as collector:
        collector.return_value.collect.return_value = MagicMock(
            evidence=[evidence], failed_minutes=set(), investigated_nodes=[]
        )
        final = graph.invoke(initial)
    operation = (
        collector.return_value.collect
        if name == "collect_evidence"
        else seams.report_writer.draft_report
    )
    assert operation.call_count == 1
    assert any(
        isinstance(m, ToolMessage) and m.tool_call_id == "two" and m.status == "error"
        for m in final["messages"]
    )


@pytest.mark.parametrize("batch", ["two_tasks", "task_finish", "propose_task"])
def test_main_graph_reserves_one_call_before_execution(batch):
    invoked = []

    @tool
    def task(subagent_type: str, description: str, runtime: ToolRuntime) -> Command:
        """A stub analysis delegation that reads the committed reservation."""
        invoked.append(
            (runtime.state["analysis_call_count"], runtime.state["analyzed_minutes"])
        )
        return Command(
            update={
                "messages": [ToolMessage("done", tool_call_id=runtime.tool_call_id)]
            }
        )

    req = request()
    state = initial_main_agent_state(
        incident_id="i",
        observed_end=req.analysis_window.end,
        pending_windows=(req.analysis_window,),
        total_wait_seconds=0,
    )
    state = {
        **state,
        "analyzed_minutes": 59,
        "admitted_window": {
            "start": req.analysis_window.start.isoformat(),
            "end": req.analysis_window.end.isoformat(),
        },
        "messages": [HumanMessage(content="start")],
    }
    task_call = call(
        "task", {"subagent_type": "analysis", "description": "goal"}, "task-1"
    )
    if batch == "two_tasks":
        calls = [task_call, call("task", task_call["args"], "task-2")]
    elif batch == "task_finish":
        calls = [
            task_call,
            call("finish_incident", {"outcome": "FAILED", "reason": "done"}, "finish"),
        ]
    else:
        calls = [
            call(
                "propose_analysis",
                {
                    "start_kst": req.analysis_window.start.isoformat(),
                    "end_kst": req.analysis_window.end.isoformat(),
                    "goal": "goal",
                },
                "propose",
            ),
            task_call,
        ]
    model = ScriptedModel(
        responses=[AIMessage(content="", tool_calls=calls), AIMessage(content="done")]
    )
    graph = create_agent(
        model,
        tools=[task, make_finish_incident_tool(), make_propose_analysis_tool()],
        state_schema=MainAgentState,
        middleware=[DelegationGuardrailMiddleware()],
    )
    final = graph.invoke(state)
    assert len(invoked) == (0 if batch == "propose_task" else 1)
    if invoked:
        assert invoked == [(1, 60)]
        assert final["analysis_call_count"] == 1
        assert final["analyzed_minutes"] == 60
        assert final["admitted_window"] is None


def test_deepagent_restores_goal_and_runs_validation_in_same_graph():
    req = request()
    seams = MagicMock()
    seams.report_writer.draft_report.return_value = DraftReport(summary="draft")
    evidence = Evidence(
        evidence_id="E-i-1",
        event_time=req.analysis_window.start,
        source=EvidenceSource.NODE_LOG,
        message="a",
    )
    model = ScriptedModel(
        responses=[
            AIMessage(content="", tool_calls=[call("collect_evidence", {}, "collect")]),
            AIMessage(
                content="", tool_calls=[call("write_report", {"focus": ""}, "write")]
            ),
            AIMessage(content="done"),
        ]
    )
    with (
        patch("cluster_doctor.incident_analysis_agent.agent.subagent.restrict_harness"),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
        ) as collector,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
    ):
        collector.return_value.collect.return_value = MagicMock(
            evidence=[evidence], failed_minutes=set(), investigated_nodes=[]
        )
        grounding.return_value.validate.return_value = []
        result = run_analysis_agent(seams=seams, request=req, model=model)
    assert result.report is not None
    assert result.verification_status is VerificationStatus.PASSED
    assert (
        seams.report_writer.draft_report.call_args.args[0].analysis_goal
        == req.analysis_goal
    )


def test_deepagent_error_after_draft_preserves_artifacts_but_is_failed():
    req = request()
    seams = MagicMock()
    seams.report_writer.draft_report.return_value = DraftReport(summary="draft")
    evidence = Evidence(
        evidence_id="E-i-1",
        event_time=req.analysis_window.start,
        source=EvidenceSource.NODE_LOG,
        message="a",
    )
    model = ScriptedModel(
        responses=[
            AIMessage(content="", tool_calls=[call("collect_evidence", {}, "collect")]),
            AIMessage(
                content="", tool_calls=[call("write_report", {"focus": ""}, "write")]
            ),
        ]
    )
    with (
        patch("cluster_doctor.incident_analysis_agent.agent.subagent.restrict_harness"),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
        ) as collector,
    ):
        collector.return_value.collect.return_value = MagicMock(
            evidence=[evidence], failed_minutes=set(), investigated_nodes=[]
        )
        result = run_analysis_agent(seams=seams, request=req, model=model)
    assert result.status is LogAnalysisStatus.FAILED
    assert result.report.summary == "draft"
    assert result.evidence == (evidence,)
    assert result.gaps


def test_main_task_exception_commits_failure_and_consumes_admission():
    @tool
    def task(subagent_type: str, description: str) -> str:
        """Fail during delegation."""
        raise RuntimeError("offline")

    req = request()
    initial = {
        **initial_main_agent_state(
            incident_id="i",
            observed_end=req.analysis_window.end,
            pending_windows=(),
            total_wait_seconds=0,
        ),
        "messages": [HumanMessage(content="start")],
        "admitted_window": {
            "start": req.analysis_window.start.isoformat(),
            "end": req.analysis_window.end.isoformat(),
        },
        "latest_analysis_status": LogAnalysisStatus.COMPLETED,
        "latest_verification_status": VerificationStatus.PASSED,
    }
    model = ScriptedModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    call(
                        "task",
                        {"subagent_type": "analysis", "description": "goal"},
                        "t",
                    )
                ],
            ),
            AIMessage(content="done"),
        ]
    )
    final = create_agent(
        model,
        tools=[task],
        state_schema=MainAgentState,
        middleware=[DelegationGuardrailMiddleware()],
    ).invoke(initial)
    assert final["latest_analysis_status"] is LogAnalysisStatus.FAILED
    assert final["latest_verification_status"] is VerificationStatus.NOT_VERIFIED
    assert final["analysis_call_count"] == 1
    assert final["admitted_window"] is None
    assert final["accumulated_gaps"]


def test_composite_deepagents_use_fresh_child_state_and_unique_incident_ids():
    req = request()
    second = TimeRange(
        start=req.analysis_window.end,
        end=req.analysis_window.end + timedelta(minutes=1),
    )

    def propose(window, id):
        return AIMessage(
            content="",
            tool_calls=[
                call(
                    "propose_analysis",
                    {
                        "start_kst": window.start.isoformat(),
                        "end_kst": window.end.isoformat(),
                        "goal": id,
                    },
                    id,
                )
            ],
        )

    def delegate(id):
        return AIMessage(
            content="",
            tool_calls=[
                call(
                    "task", {"subagent_type": "analysis", "description": "analyze"}, id
                )
            ],
        )

    def child(prefix):
        return [
            AIMessage(
                content="", tool_calls=[call("collect_evidence", {}, prefix + "c")]
            ),
            AIMessage(
                content="",
                tool_calls=[call("write_report", {"focus": ""}, prefix + "w")],
            ),
            AIMessage(content="child done"),
        ]

    model = ScriptedModel(
        responses=[
            propose(req.analysis_window, "goal-one"),
            delegate("t1"),
            *child("one"),
            propose(second, "goal-two"),
            delegate("t2"),
            *child("two"),
            AIMessage(
                content="",
                tool_calls=[
                    call(
                        "finish_incident",
                        {"outcome": "COMPLETED", "reason": "done"},
                        "finish",
                    )
                ],
            ),
            AIMessage(content="done"),
        ]
    )
    seams = MagicMock()
    seams.report_writer.draft_report.return_value = DraftReport(summary="draft")
    analyzer = object.__new__(_DeepAgentIncidentAnalyzer)
    analyzer._model, analyzer._seams, analyzer._recursion_limit = model, seams, 60
    incident = Incident(
        incident_id="i",
        cluster="c",
        trigger_time=req.analysis_window.start,
        kafka_receive_time=req.analysis_window.start,
    )

    def collected(window, builder):
        collector = collector_factory.call_args.kwargs
        item = Evidence(
            evidence_id=collector["new_evidence_id"](),
            event_time=window.start,
            source=EvidenceSource.NODE_LOG,
            message="a",
        )
        return MagicMock(evidence=[item], failed_minutes=set(), investigated_nodes=[])

    with (
        patch(
            "cluster_doctor.incident_orchestrator_agent.agent.graph.restrict_harness"
        ),
        patch("cluster_doctor.incident_analysis_agent.agent.subagent.restrict_harness"),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
        ) as collector_factory,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
    ):
        collector_factory.return_value.collect.side_effect = collected
        grounding.return_value.validate.return_value = []
        result = analyzer.analyze(
            IncidentAnalysisRequest(
                incident=incident,
                observed_start=req.analysis_window.start,
                observed_end=second.end,
                settling_wait_seconds=0,
            )
        )
    assert result.status is IncidentStatus.COMPLETED
    assert not result.failed
    assert result.analysis_calls == 2
    assert [e.evidence_id for e in result.evidence] == ["E-i-1", "E-i-2"]
    writer_requests = [
        c.args[0] for c in seams.report_writer.draft_report.call_args_list
    ]
    assert [r.analysis_goal for r in writer_requests] == ["goal-one", "goal-two"]
    assert [
        len(c.args[1]) for c in seams.report_writer.draft_report.call_args_list
    ] == [1, 1]
    assert writer_requests[1].prior_report is not None
    assert len(result.observations.requested) == 2
