from langchain_core.messages import AIMessage
import pytest

from datetime import UTC, datetime, timedelta
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.agent.state import (
    initial_main_agent_state,
)

from cluster_doctor.incident_orchestrator_agent.agent.middleware import (
    DelegationGuardrailMiddleware,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus


def test_parallel_analysis_tasks_after_the_first_are_fulfilled_as_errors():
    state = {
        "status": IncidentStatus.ANALYZING,
        "rejected_decision_count": 0,
        "admitted_window": {
            "start": "2024-01-01T00:00:00+00:00",
            "end": "2024-01-01T00:01:00+00:00",
        },
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "task",
                        "args": {"subagent_type": "analysis"},
                        "id": "one",
                    },
                    {
                        "name": "task",
                        "args": {"subagent_type": "analysis"},
                        "id": "two",
                    },
                ],
            )
        ],
    }
    update = DelegationGuardrailMiddleware().after_model(state, runtime=None)
    assert update["rejected_decision_count"] == 1
    assert [message.tool_call_id for message in update["messages"]] == ["two"]


@pytest.mark.parametrize("condition", ["minutes", "calls", "duplicate", "oversize"])
def test_invalid_task_never_reserves_budget(condition):
    start = datetime(2024, 1, 1, tzinfo=UTC)
    window = TimeRange(start=start, end=start + timedelta(minutes=2))
    state = initial_main_agent_state(
        incident_id="i",
        observed_end=window.end,
        pending_windows=(),
        total_wait_seconds=0,
    )
    state = {
        **state,
        "admitted_window": {"start": start.isoformat(), "end": window.end.isoformat()},
        "messages": [
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "task", "args": {"subagent_type": "analysis"}, "id": "t"}
                ],
            )
        ],
    }
    if condition == "minutes":
        state["analyzed_minutes"] = 60
    elif condition == "calls":
        state["analysis_call_count"] = 12
    elif condition == "duplicate":
        state["analyzed_windows"] = (window,)
    else:
        state["analyzed_minutes"] = 59
    update = DelegationGuardrailMiddleware().after_model(state, None)
    assert "analysis_call_count" not in update
    assert "analyzed_minutes" not in update
    assert update["admitted_task_call_id"] is None
    assert update["messages"][0].tool_call_id == "t"
    assert update["messages"][0].status == "error"
