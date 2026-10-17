from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.utils.function_calling import convert_to_openai_tool
from pydantic import Field

from cluster_doctor.incident_analysis_agent.agent.dependencies import AnalysisSeams
from cluster_doctor.incident_analysis_agent.agent.subagent import AnalysisAgent
from cluster_doctor.incident_analysis_agent.model.analysis_contract import LogAnalysisRequest
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange


class ToolSchemaCapturingModel(BaseChatModel):
    tool_specs: dict = Field(default_factory=dict)

    @property
    def _llm_type(self):
        return "analysis-tool-schema-test"

    def bind_tools(self, tools, **kwargs):
        self.tool_specs = {
            tool.name: convert_to_openai_tool(tool)["function"] for tool in tools
        }
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="done"))])


def test_analysis_model_receives_public_argument_help_without_runtime_state():
    model = ToolSchemaCapturingModel()
    agent = AnalysisAgent(model=model, seams=MagicMock(spec=AnalysisSeams))
    start = datetime(2026, 10, 18, tzinfo=UTC)

    agent.analyze(
        LogAnalysisRequest(
            incident_id="tool-metadata",
            cluster="test-cluster",
            analysis_window=TimeRange(start=start, end=start + timedelta(minutes=1)),
            analysis_goal="Explain the initial symptom",
        )
    )

    assert set(model.tool_specs) == {
        "collect_evidence", "write_report", "report_insufficient"
    }
    expected_arguments = {
        "collect_evidence": set(),
        "write_report": {"focus"},
        "report_insufficient": {"reason", "suggested_windows"},
    }
    for name, expected in expected_arguments.items():
        spec = model.tool_specs[name]
        assert spec["description"]
        parameters = spec["parameters"]
        assert set(parameters["properties"]) == expected
        assert set(parameters.get("required", [])) == expected
        for argument, field in parameters["properties"].items():
            assert field.get("description"), (name, argument)
