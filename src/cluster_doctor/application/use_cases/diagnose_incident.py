"""Run the complete diagnosis lifecycle for one settled incident."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from cluster_doctor.application.commands import StartIncident
from cluster_doctor.application.ports.artifact_store import ArtifactStore
from cluster_doctor.application.ports.incident_analyzer import IncidentAnalyzer
from cluster_doctor.application.ports.incident_state_repository import (
    IncidentStateRepository,
)
from cluster_doctor.application.ports.report_publisher import (
    ReportPublication,
    ReportPublisher,
)
from cluster_doctor.domain.diagnosis.report import LogAnalysisStatus, VerificationStatus
from cluster_doctor.domain.incident.guardrails import (
    INCIDENT_TIMEOUT_SECONDS,
    CancellationToken,
    Deadline,
)
from cluster_doctor.domain.incident.models import IncidentStatus
from cluster_doctor.application.report_finalization import finalize_incident_report
from cluster_doctor.domain.incident.state import IncidentState
from cluster_doctor.domain.incident.window_planner import initial_windows
from cluster_doctor.application.output_mapping import to_diagnosis_report
from cluster_doctor.domain.diagnosis.evidence import Evidence
from cluster_doctor.domain.diagnosis.observations import DiagnosisReport, Observations
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IncidentOutcome:
    incident_id: str
    status: IncidentStatus
    analysis_failed: bool = False
    gaps: tuple[str, ...] = ()
    report_ref: str | None = None
    analysis_calls: int = 0
    reason: str = ""
    diagnostics: "IncidentDiagnostics" = field(default_factory=lambda: IncidentDiagnostics())


@dataclass(frozen=True)
class IncidentDiagnostics:
    """Public diagnosis details for entrypoints that need a human readout."""

    report: LogAnalysisReport | None = None
    observations: Observations = field(default_factory=Observations)
    evidence: tuple[Evidence, ...] = ()
    publication: ReportPublication = field(default_factory=ReportPublication)


class DiagnoseIncident:
    """Create state, run the analyzer, close, publish, and clean up an incident."""

    def __init__(
        self,
        *,
        incident_analyzer: IncidentAnalyzer,
        state_repository: IncidentStateRepository,
        artifact_store: ArtifactStore,
        report_publisher: ReportPublisher,
        on_incident_complete: Callable[[str], None] | None = None,
        incident_timeout_seconds: float = INCIDENT_TIMEOUT_SECONDS,
    ) -> None:
        self._analyzer = incident_analyzer
        self._states = state_repository
        self._store = artifact_store
        self._report_publisher = report_publisher
        self._on_incident_complete = on_incident_complete
        self._incident_timeout_seconds = incident_timeout_seconds

    async def handle(
        self, command: StartIncident, *, cancellation: CancellationToken | None = None
    ) -> IncidentOutcome:
        incident = command.incident
        token = cancellation or CancellationToken()
        remaining_seconds = max(
            0.0,
            self._incident_timeout_seconds - command.settling_wait_seconds,
        )
        deadline = Deadline(remaining_seconds)
        state = IncidentState(
            incident_id=incident.incident_id,
            pending_windows=initial_windows(
                command.observed_start, command.observed_end
            ),
            total_wait_seconds=command.settling_wait_seconds,
            status=IncidentStatus.ANALYZING,
        )
        self._states.create(state)

        try:
            analysis_failed = False
            forced: tuple[IncidentStatus, str] | None = None
            if token.is_cancelled:
                forced = (IncidentStatus.CANCELLED, token.reason or "취소됨")
            elif deadline.expired:
                analysis_failed = True
                forced = (IncidentStatus.FAILED, self._timeout_reason())
            else:
                analysis_failed, forced = await self._run_analyzer(incident, deadline)

            state = self._states.get(incident.incident_id)
            if forced is not None:
                self._close(state, *forced)
            elif not state.status.is_terminal():
                self._close(state, IncidentStatus.COMPLETED, state.closing_reason)

            gaps = tuple(state.accumulated_gaps)
            diagnostics = await self._deliver(incident, state, analysis_failed, gaps)
            return IncidentOutcome(
                incident_id=incident.incident_id,
                status=state.status,
                analysis_failed=(
                    analysis_failed
                    or state.latest_analysis_status is LogAnalysisStatus.FAILED
                    or state.latest_verification_status is VerificationStatus.MISMATCH
                ),
                gaps=gaps,
                report_ref=state.final_report_ref,
                analysis_calls=state.analysis_call_count,
                reason=state.closing_reason,
                diagnostics=diagnostics,
            )
        finally:
            if self._on_incident_complete is not None:
                self._on_incident_complete(incident.incident_id)

    async def _run_analyzer(
        self, incident, deadline: Deadline
    ) -> tuple[bool, tuple[IncidentStatus, str] | None]:
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(self._analyzer.analyze, incident),
                timeout=deadline.remaining,
            )
        except TimeoutError:
            _logger.warning("[incident %s] execution timed out", incident.incident_id)
            return True, (IncidentStatus.FAILED, self._timeout_reason())
        except Exception:
            _logger.exception("[incident %s] analyzer raised", incident.incident_id)
            return True, (IncidentStatus.FAILED, "분석 Agent 실행이 예외로 끝났다")

        state = self._states.get(incident.incident_id)
        if not state.status.is_terminal():
            self._close(state, result.status, result.reason)
        if result.gaps:
            for gap in result.gaps:
                if gap not in state.accumulated_gaps:
                    state.accumulated_gaps.append(gap)
            self._states.save(state)
        return result.failed, None

    def _timeout_reason(self) -> str:
        return f"실행 시간 상한 {self._incident_timeout_seconds:.0f}초를 넘겼다"

    def _close(self, state: IncidentState, status: IncidentStatus, reason: str) -> None:
        state.status = status
        state.closing_reason = reason
        self._states.save(state)

    async def _deliver(
        self,
        incident,
        state: IncidentState,
        analysis_failed: bool,
        gaps: tuple[str, ...],
    ) -> IncidentDiagnostics:
        if state.final_report_ref is None and state.report_refs:
            fallback_ref = finalize_incident_report(
                incident.incident_id, state.report_refs, self._store
            )
            if fallback_ref is not None:
                state.final_report_ref = fallback_ref
                self._states.save(state)
        observations = self._store.get_observations(incident.incident_id)
        report = (
            self._store.get_report(state.final_report_ref)
            if state.final_report_ref
            else None
        )
        evidence = self._store.list_evidence(incident.incident_id)
        all_gaps = list(gaps)
        if report is not None and report.verification_issues:
            all_gaps.extend(
                f"리포트 검증 불일치: {issue}" for issue in report.verification_issues
            )
        if state.closing_reason and state.status is not IncidentStatus.COMPLETED:
            all_gaps.append(f"Incident 종료 사유: {state.closing_reason}")
        rendered_report = to_diagnosis_report(report, observations, evidence)
        publication = ReportPublication()
        try:
            publication = await self._report_publisher.publish(
                rendered_report,
                gaps=tuple(all_gaps),
                analysis_failed=analysis_failed,
            )
        except Exception:
            _logger.exception(
                "[incident %s] report delivery failed", incident.incident_id
            )
        return IncidentDiagnostics(
            report=report,
            observations=observations,
            evidence=tuple(evidence),
            publication=publication,
        )
