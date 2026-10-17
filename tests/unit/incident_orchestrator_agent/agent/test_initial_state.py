from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.agent.state import initial_main_agent_state
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
_T1 = datetime(2024, 1, 1, 13, 10, tzinfo=timezone.utc)


def test_main_agent_state_has_complete_incident_lifecycle_at_start():
    state = initial_main_agent_state(
        incident_id="INC-003", observed_end=_T1,
        pending_windows=(TimeRange(start=_T0, end=_T1),), total_wait_seconds=0,
    )
    assert state["status"] is IncidentStatus.ANALYZING
    assert state["window_results"] == ()
    assert state["evidence_counter"] == 0
