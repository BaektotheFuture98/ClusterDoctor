"""프레임워크와 무관한 lifecycle command, outcome, analysis contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import Pseudonymizer
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)


class IncidentClose(StrEnum):
    """incident 범위를 마감한 이유. 보고서 개요와 Main 시작 메시지에 그대로 쓴다."""

    GREEN = "green 확인"
    SPAN_LIMIT = "범위 상한"
    HEALTH_UNKNOWN = "health 확인 불가"
    NOT_WAITED = "대기 안 함"


@dataclass(frozen=True)
class SessionContext:
    """같은 장애로 이어지는 incident들의 연결. kafka_consumer가 만들고 넘긴다.

    Main은 incident마다 새로 만들어지므로 세션을 기억하지 못한다. 이전 보고서와
    분석 구간은 코드가 outcome에서 옮겨 여기에 싣는다. ``pseudonyms``는 세션의
    incident들이 이어 쓰는 가명 대응표다(``pseudonym_scope``의 ``mapping``).
    ``onset``은 세션 첫 incident의 최초 관측 시각이다. 두 번째 incident부터
    ``observed_start``는 이전 incident의 끝이라 장애 시작이 아니다.
    """

    session_id: str
    sequence: int
    last_allowed: bool
    onset: datetime | None = None
    previous_incident_id: str = ""
    previous_report: LogAnalysisReport | None = None
    previous_windows: tuple[TimeRange, ...] = ()
    pseudonyms: Pseudonymizer = field(
        default_factory=Pseudonymizer, compare=False, repr=False
    )

    def following(
        self,
        *,
        incident_id: str,
        report: LogAnalysisReport | None,
        windows: tuple[TimeRange, ...],
        last_allowed: bool,
    ) -> SessionContext:
        return SessionContext(
            session_id=self.session_id,
            sequence=self.sequence + 1,
            last_allowed=last_allowed,
            onset=self.onset,
            previous_incident_id=incident_id,
            previous_report=report,
            previous_windows=self.previous_windows + windows,
            pseudonyms=self.pseudonyms,
        )


def tracking_stopped(close: IncidentClose, session: SessionContext | None) -> bool:
    """세션 상한의 마지막 incident가 green 확인 없이 마감됐는가.

    발행 전에 알 수 있는 값으로 정한다. 발행 뒤 health를 다시 보는 것은 세션을
    끝낼지만 정하고, 이미 나간 보고서를 고치지 않는다.
    """
    return session is not None and session.last_allowed and close is not IncidentClose.GREEN


@dataclass(frozen=True)
class IncidentAnalysisRequest:
    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float
    incident_end: datetime | None = None
    close: IncidentClose = IncidentClose.NOT_WAITED
    health_history: tuple[HealthPoint, ...] = ()
    session: SessionContext | None = None

    @property
    def onset(self) -> datetime:
        """장애의 최초 관측 시각. 시작 메시지와 사건 종합의 before·onset·after 기준이다.

        세션의 두 번째 incident부터 ``observed_start``는 이전 incident의 끝이므로
        세션의 ``onset``을 쓴다.
        """
        if self.session is not None and self.session.onset is not None:
            return self.session.onset
        return self.observed_start


@dataclass(frozen=True)
class IncidentAnalysisResult:
    status: IncidentStatus
    reason: str = ""
    failed: bool = False
    gaps: tuple[str, ...] = ()
    report: LogAnalysisReport | None = None
    observations: Observations = field(default_factory=Observations)
    evidence: tuple[Evidence, ...] = ()
    analysis_calls: int = 0


@dataclass(frozen=True)
class StartIncident:
    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float
    incident_end: datetime | None = None
    close: IncidentClose = IncidentClose.NOT_WAITED
    health_history: tuple[HealthPoint, ...] = ()
    health_gaps: tuple[str, ...] = ()
    session: SessionContext | None = None

    def to_request(self) -> IncidentAnalysisRequest:
        return IncidentAnalysisRequest(
            incident=self.incident,
            observed_start=self.observed_start,
            observed_end=self.observed_end,
            settling_wait_seconds=self.settling_wait_seconds,
            incident_end=self.incident_end,
            close=self.close,
            health_history=self.health_history,
            session=self.session,
        )


@dataclass(frozen=True)
class IncidentAnalysisDetails:
    report: LogAnalysisReport | None = None
    observations: Observations = field(default_factory=Observations)
    evidence: tuple[Evidence, ...] = ()
    publication: ReportPublication = field(default_factory=ReportPublication)


@dataclass(frozen=True)
class IncidentOutcome:
    incident_id: str
    status: IncidentStatus
    analysis_failed: bool = False
    gaps: tuple[str, ...] = ()
    report: LogAnalysisReport | None = None
    analysis_calls: int = 0
    reason: str = ""
    diagnostics: IncidentAnalysisDetails = field(
        default_factory=IncidentAnalysisDetails
    )
