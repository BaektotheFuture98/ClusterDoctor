from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import PSEUDONYMS
from cluster_doctor.incident_orchestrator_agent.agent.adapter import (
    _DeepAgentIncidentAnalyzer,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import Incident
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
)


@pytest.mark.parametrize("fail_graph", [False, True])
def test_next_incident_does_not_restore_previous_incident_identifiers(fail_graph):
    analyzer = object.__new__(_DeepAgentIncidentAnalyzer)
    analyzer._recursion_limit = 40
    restored = []
    alias_from_first = []

    def compile_graph(incident):
        if incident.incident_id == "first":
            PSEUDONYMS.register("req", "request-only-in-first-incident")
            alias_from_first.append(PSEUDONYMS.mask("request-only-in-first-incident"))

        def stream(state, config, stream_mode):
            if incident.incident_id == "first":
                alias = PSEUDONYMS.mask("request-only-in-first-incident")
                restored.append(PSEUDONYMS.restore(alias))
                if fail_graph:
                    raise RuntimeError("graph failed")
            else:
                restored.append(PSEUDONYMS.restore(alias_from_first[0]))
            yield state

        return SimpleNamespace(stream=stream)

    analyzer._compile = compile_graph
    start = datetime(2026, 10, 1, tzinfo=UTC)
    for incident_id in ("first", "second"):
        analyzer.analyze(
            IncidentAnalysisRequest(
                incident=Incident(
                    incident_id=incident_id,
                    cluster="test",
                    trigger_time=start,
                    kafka_receive_time=start,
                ),
                observed_start=start,
                observed_end=start,
                settling_wait_seconds=0,
            )
        )
    assert restored == ["request-only-in-first-incident", alias_from_first[0]]
