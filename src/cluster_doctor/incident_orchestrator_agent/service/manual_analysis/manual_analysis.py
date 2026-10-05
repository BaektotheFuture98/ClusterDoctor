"""Direct, queue-free manual analysis entry point."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    TriggerType,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    CancellationToken,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    AnalyzeIncident,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentOutcome,
    StartIncident,
)


class RunManualAnalysis:
    def __init__(
        self, *, analyze_incident: AnalyzeIncident, cluster: str = "elasticsearch"
    ) -> None:
        self._analysis_service = analyze_incident
        self._cluster = cluster

    async def handle(
        self,
        moments: Sequence[datetime],
        *,
        cancellation: CancellationToken | None = None,
    ) -> IncidentOutcome:
        if not moments:
            raise ValueError("manual analysis requires at least one moment")
        observed_start = min(moments)
        observed_end = max(moments)
        incident = Incident(
            incident_id=uuid.uuid4().hex[:12],
            cluster=self._cluster,
            trigger_time=observed_start,
            kafka_receive_time=datetime.now(UTC),
            trigger_type=TriggerType.MANUAL,
        )
        return await self._analysis_service.handle(
            StartIncident(incident, observed_start, observed_end, 0),
            cancellation=cancellation,
        )
