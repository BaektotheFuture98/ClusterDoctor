from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.agent.state import (
    initial_analysis_agent_state,
)
from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    LogAnalysisRequest,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange


def test_each_delegation_gets_fresh_analysis_agent_state():
    request = LogAnalysisRequest(
        incident_id="INC-1",
        cluster="c1",
        analysis_window=TimeRange(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc),
        ),
    )
    first = initial_analysis_agent_state(request, evidence_sequence=3)
    second = initial_analysis_agent_state(request, evidence_sequence=3)
    first["gaps"] = ("first only",)
    assert second["gaps"] == ()
    assert first["evidence_sequence"] == second["evidence_sequence"] == 3
