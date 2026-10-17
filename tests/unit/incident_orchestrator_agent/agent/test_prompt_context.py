from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from pydantic import Field

from cluster_doctor.incident_analysis_agent.agent.dependencies import AnalysisSeams
from cluster_doctor.incident_analysis_agent.agent.subagent import AnalysisAgent
from cluster_doctor.incident_orchestrator_agent.agent.adapter import MainAgent
from cluster_doctor.incident_orchestrator_agent.agent.analysis_subagent import (
    AnalysisSubagentAdapter,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
)
from cluster_doctor.incident_orchestrator_agent.agent.state import initial_main_agent_state


class CapturingModel(BaseChatModel):
    received: list[list] = Field(default_factory=list)
    tool_specs: dict = Field(default_factory=dict)
    finish_with_tool: bool = True

    @property
    def _llm_type(self):
        return "orchestrator-prompt-context-test"

    def bind_tools(self, tools, **kwargs):
        for tool in tools:
            self.tool_specs[tool.name] = {
                "description": tool.description,
                "parameters": tool.args,
            }
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.received.append(list(messages))
        response = AIMessage(content="done")
        if self.finish_with_tool and len(self.received) == 1:
            response = AIMessage(content="", tool_calls=[{
                "name": "finish_incident",
                "args": {"outcome": "FAILED", "reason": "No report collected"},
                "id": "finish",
            }])
        return ChatResult(generations=[ChatGeneration(message=response)])


def _analyzer(model):
    seams = MagicMock(spec=AnalysisSeams)
    return MainAgent(
        model=model,
        seams=seams,
        analysis_subagent=AnalysisSubagentAdapter(
            analysis_agent=AnalysisAgent(model=model, seams=seams),
        ),
    )


@pytest.mark.parametrize("same_moment", [False, True])
def test_model_receives_observed_bounds_separately_from_trigger_time(same_moment):
    model = CapturingModel()
    analyzer = _analyzer(model)
    start = datetime(2026, 10, 17, 4, 0, 37, tzinfo=UTC)
    end = start if same_moment else datetime(2026, 10, 17, 4, 8, 29, tzinfo=UTC)
    trigger = start if same_moment else datetime(2026, 10, 17, 4, 3, tzinfo=UTC)
    incident = Incident(
        incident_id="prompt-context",
        cluster="test-cluster",
        trigger_time=trigger,
        kafka_receive_time=trigger,
    )

    result = analyzer.analyze(IncidentAnalysisRequest(incident, start, end, 0))

    assert result.status is IncidentStatus.FAILED
    kickoff = next(m.content for m in model.received[0] if isinstance(m, HumanMessage))
    assert "클러스터: test-cluster" in kickoff
    expected_trigger = "13:00:37" if same_moment else "13:03:00"
    assert f"트리거 시각: 2026-10-17T{expected_trigger}" in kickoff
    assert "최초 관측 시각: 2026-10-17T13:00:37" in kickoff
    expected_end = "13:00:37" if same_moment else "13:08:29"
    assert f"마지막 관측 시각: 2026-10-17T{expected_end}" in kickoff
    assert "KST(UTC+09:00)" in kickoff


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_model_receives_approval_based_task_description_and_argument_help(asynchronous):
    # 모델에 전달되는 설명만 검사한다. Main의 도구 실행은 동기 경로를 사용한다.
    model = CapturingModel(finish_with_tool=False)
    analyzer = _analyzer(model)
    moment = datetime(2026, 10, 18, tzinfo=UTC)
    incident = Incident(
        incident_id="tool-metadata",
        cluster="test-cluster",
        trigger_time=moment,
        kafka_receive_time=moment,
    )
    graph = analyzer._compile(incident)
    state = {
        **initial_main_agent_state(
            incident_id=incident.incident_id,
            observed_end=moment,
            pending_windows=(),
            total_wait_seconds=0,
        ),
        "messages": [HumanMessage(content="Check tool metadata")],
    }

    if asynchronous:
        final = await graph.ainvoke(state)
    else:
        final = graph.invoke(state)

    assert final["messages"][-1].content == "done"
    task = model.tool_specs["task"]
    assert "propose_analysis" in task["description"]
    assert "concurrently" not in task["description"]
    assert "goal" in task["parameters"]["description"]["description"]
    assert "output format" not in task["parameters"]["description"]["description"]
    assert "analysis" in task["parameters"]["subagent_type"]["description"]
    for name in ("list_candidate_windows", "propose_analysis", "finish_incident"):
        for field in model.tool_specs[name]["parameters"].values():
            assert field.get("description"), (name, field)
