from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from langchain.agents import create_agent
from langchain.tools import ToolRuntime, tool
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.types import Command

from cluster_doctor.incident_analysis_agent.agent.dependencies import AnalysisSeams
from cluster_doctor.incident_orchestrator_agent.agent import analysis_subagent as subagent_adapter

from cluster_doctor.incident_analysis_agent.agent.runtime.tool_batch import (
    AnalysisToolAdmissionMiddleware,
)
from cluster_doctor.incident_analysis_agent.agent.state import (
    AnalysisAgentState,
    initial_analysis_agent_state,
)
from cluster_doctor.incident_analysis_agent.agent import subagent as analysis_module
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
from cluster_doctor.incident_orchestrator_agent.agent import adapter as main_adapter
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
    seams.report_writer.draft_report.side_effect = lambda request, evidence, state: DraftReport(summary="draft", summary_evidence_refs=[e.evidence_id for e in evidence])
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
    seams.report_writer.draft_report.side_effect = lambda request, evidence, state: DraftReport(summary="draft", summary_evidence_refs=[e.evidence_id for e in evidence])
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
        result = analysis_module.AnalysisAgent(seams=seams, model=model).analyze(req)
    assert result.report is not None
    assert result.verification_status is VerificationStatus.PASSED
    assert (
        seams.report_writer.draft_report.call_args.args[0].analysis_goal
        == req.analysis_goal
    )


def test_deepagent_error_after_draft_preserves_artifacts_but_is_failed():
    req = request()
    seams = MagicMock()
    seams.report_writer.draft_report.side_effect = lambda request, evidence, state: DraftReport(summary="draft", summary_evidence_refs=[e.evidence_id for e in evidence])
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
        result = analysis_module.AnalysisAgent(seams=seams, model=model).analyze(req)
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
    from types import SimpleNamespace
    from cluster_doctor import main as app
    from cluster_doctor.bootstrap.settings import Settings

    settings = Settings(
        _env_file=None, clickhouse_url="http://unused:8123/default", es_host="unused",
        llm_provider="nvidia_nim", nvidia_api_key="test-key",
        report_sftp_host="",
    )
    writer = MagicMock()
    writer.draft_report.side_effect = lambda request, evidence, state: DraftReport(
        summary="draft", summary_evidence_refs=[e.evidence_id for e in evidence]
    )
    with (
        patch.object(app, "build_chat_model", return_value=model),
        patch.object(app, "build_structured_call", return_value=MagicMock()),
        patch.object(app, "ReportWriter", return_value=writer),
        patch(
            "cluster_doctor.incident_orchestrator_agent.agent.adapter.restrict_harness",
            wraps=main_adapter.restrict_harness,
        ) as harness,
    ):
        analyzer = app.compose_analysis_service(
            settings, SimpleNamespace(clickhouse_client=object(), es_client=object())
        )._analyzer
    seams = analyzer._seams
    analysis_agent = analyzer._analysis_subagent._analysis_agent
    assert isinstance(analyzer, main_adapter.MainAgent)
    assert isinstance(analyzer._analysis_subagent, subagent_adapter.AnalysisSubagentAdapter)
    assert isinstance(analysis_agent, analysis_module.AnalysisAgent)
    harness.assert_called_once_with(model)
    assert model.index == 0
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
        patch.object(
            analysis_agent, "analyze",
            wraps=analysis_agent.analyze,
        ) as analysis_execution,
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
    assert analysis_execution.call_count == 2
    assert analyzer._analysis_subagent._analysis_agent is analysis_agent
    assert [c.args[0].analysis_goal for c in analysis_execution.call_args_list] == ["goal-one", "goal-two"]
    assert model.index == len(model.responses)
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


def test_bound_analysis_delegate_keeps_its_incident_after_another_binding():
    """A later bind must not replace an earlier runnable's incident."""
    req = request()
    model = ScriptedModel(responses=[
        AIMessage(content="", tool_calls=[call("collect_evidence", {}, "collect")]),
        AIMessage(content="", tool_calls=[call("write_report", {"focus": ""}, "write")]),
        AIMessage(content="done"),
    ] * 2)
    writer = MagicMock()
    writer.draft_report.side_effect = lambda request, evidence, state: DraftReport(
        summary="bound A report", summary_evidence_refs=[e.evidence_id for e in evidence]
    )
    seams = AnalysisSeams(
        fetch_logs=MagicMock(), fetch_node_logs=MagicMock(), cluster=MagicMock(),
        node_resolver=MagicMock(), node_log_fetcher=MagicMock(), call_llm=MagicMock(),
        report_writer=writer,
    )
    analysis_agent = analysis_module.AnalysisAgent(model=model, seams=seams)
    adapter = subagent_adapter.AnalysisSubagentAdapter(analysis_agent=analysis_agent)
    incident_a = Incident(incident_id="A", cluster="cluster-a",
                          trigger_time=req.analysis_window.start,
                          kafka_receive_time=req.analysis_window.start)
    incident_b = Incident(incident_id="B", cluster="cluster-b",
                          trigger_time=req.analysis_window.start,
                          kafka_receive_time=req.analysis_window.start)
    bound_a = adapter.bind(incident_a)
    bound_b = adapter.bind(incident_b)
    parent = {
        **initial_main_agent_state(incident_id="A", observed_end=req.analysis_window.end,
                                   pending_windows=(), total_wait_seconds=0),
        "admitted_window": {"start": req.analysis_window.start.isoformat(),
                            "end": req.analysis_window.end.isoformat()},
        "admitted_goal": "A goal",
    }
    evidence = Evidence(evidence_id="E-A-1", event_time=req.analysis_window.start,
                        source=EvidenceSource.NODE_LOG, message="A evidence")
    with (
        patch("cluster_doctor.incident_analysis_agent.agent.subagent.restrict_harness"),
        patch("cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector") as collector,
        patch("cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator") as grounding,
    ):
        collector.return_value.collect.return_value = MagicMock(
            evidence=[evidence], failed_minutes=set(), investigated_nodes=[])
        grounding.return_value.validate.return_value = []
        result = bound_a["runnable"].invoke(parent)
        evidence_b = evidence.model_copy(update={"evidence_id": "E-B-1", "message": "B evidence"})
        collector.return_value.collect.return_value = MagicMock(
            evidence=[evidence_b], failed_minutes=set(), investigated_nodes=[])
        result_b = bound_b["runnable"].invoke({**parent, "incident_id": "B", "admitted_goal": "B goal"})
    child_request = writer.draft_report.call_args_list[0].args[0]
    assert (child_request.incident_id, child_request.cluster, child_request.analysis_goal) == (
        "A", "cluster-a", "A goal")
    assert result["window_results"][0].report.incident_id == "A"
    assert result["evidence"] == (evidence,)
    assert result["admitted_window"] is None
    request_b = writer.draft_report.call_args_list[1].args[0]
    assert (request_b.incident_id, request_b.cluster, request_b.analysis_goal) == (
        "B", "cluster-b", "B goal")
    assert result_b["window_results"][0].report.incident_id == "B"
    assert result_b["evidence"] == (evidence_b,)
    assert result_b["admitted_window"] is None
    assert model.index == 6
