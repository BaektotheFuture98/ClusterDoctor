"""Framework-independent lifecycle commands, outcomes and analysis contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)


@dataclass(frozen=True)
class IncidentAnalysisRequest:
    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float


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
