from datetime import UTC, datetime

from cluster_doctor.incident_orchestrator_agent.model.incident import Incident
from cluster_doctor.incident_orchestrator_agent.service.manual_analysis.manual_analysis import (
    RunManualAnalysis,
)


def test_default_incident_serializes_problem_log_origin():
    now = datetime(2026, 10, 5, tzinfo=UTC)
    incident = Incident(
        incident_id="automatic", cluster="test-cluster",
        trigger_time=now, kafka_receive_time=now,
    )
    assert incident.model_dump(mode="json")["trigger_type"] == "PROBLEM_LOG"


async def test_manual_analysis_preserves_manual_origin():
    received = []

    class AnalysisService:
        async def handle(self, command, *, cancellation):
            received.append(command.incident)

    manual = RunManualAnalysis(analyze_incident=AnalysisService())
    await manual.handle([datetime(2026, 10, 5, tzinfo=UTC)])
    assert received[0].model_dump(mode="json")["trigger_type"] == "MANUAL"
