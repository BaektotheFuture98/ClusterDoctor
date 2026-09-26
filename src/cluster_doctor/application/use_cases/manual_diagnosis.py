"""Direct, queue-free manual diagnosis entry point."""

from __future__ import annotations

import uuid
from datetime import datetime

from cluster_doctor.application.commands import StartIncident
from cluster_doctor.application.use_cases.diagnose_incident import (
    DiagnoseIncident,
    IncidentOutcome,
)
from cluster_doctor.domain.incident.guardrails import CancellationToken
from cluster_doctor.domain.incident.models import Incident, TriggerType


class RunManualDiagnosis:
    def __init__(
        self, *, diagnose_incident: DiagnoseIncident, cluster: str = "elasticsearch"
    ) -> None:
        self._diagnose = diagnose_incident
        self._cluster = cluster

    async def handle(
        self, trigger_time: datetime, *, cancellation: CancellationToken | None = None
    ) -> IncidentOutcome:
        incident = Incident(
            incident_id=uuid.uuid4().hex[:12],
            cluster=self._cluster,
            trigger_time=trigger_time,
            kafka_receive_time=trigger_time,
            trigger_type=TriggerType.MANUAL,
        )
        return await self._diagnose.handle(
            StartIncident(incident, trigger_time, trigger_time, 0),
            cancellation=cancellation,
        )
