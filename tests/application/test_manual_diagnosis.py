from datetime import UTC, datetime

from cluster_doctor.application.commands import StartIncident
from cluster_doctor.application.use_cases.diagnose_incident import IncidentOutcome
from cluster_doctor.application.use_cases.manual_diagnosis import RunManualDiagnosis
from cluster_doctor.domain.incident.models import IncidentStatus, TriggerType


class RecordingDiagnosis:
    def __init__(self) -> None:
        self.commands: list[StartIncident] = []

    async def handle(
        self, command: StartIncident, *, cancellation=None
    ) -> IncidentOutcome:
        self.commands.append(command)
        return IncidentOutcome(
            incident_id=command.incident.incident_id,
            status=IncidentStatus.COMPLETED,
        )


async def test_수동_진단은_Kafka나_큐_없이_바로_진단_명령을_호출한다():
    diagnosis = RecordingDiagnosis()
    run = RunManualDiagnosis(diagnose_incident=diagnosis, cluster="es-prod")
    timestamp = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)

    outcome = await run.handle(timestamp)

    assert outcome.status is IncidentStatus.COMPLETED
    assert len(diagnosis.commands) == 1
    command = diagnosis.commands[0]
    assert command.incident.trigger_type is TriggerType.MANUAL
    assert command.observed_start == timestamp
    assert command.observed_end == timestamp
    assert command.settling_wait_seconds == 0
