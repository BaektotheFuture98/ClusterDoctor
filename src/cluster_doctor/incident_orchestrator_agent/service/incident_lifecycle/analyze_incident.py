"""Run the complete analysis lifecycle for one settled incident."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.basemodel.observations import Observations
from cluster_doctor.incident_analysis_agent.model.basemodel.report import (
    LogAnalysisReport,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.basemodel.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.state.incident_state import IncidentState
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    INCIDENT_TIMEOUT_SECONDS,
    CancellationToken,
    Deadline,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    initial_windows,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.incident_analyzer import (
    IncidentAnalyzer,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.file.report_publisher import (
    ReportPublication,
    ReportPublisher,
)

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StartIncident:
    """정착된 유입을 Incident 진단으로 넘기는 명령.

    구간별 위임인 ``LogAnalysisRequest``와 달리 Incident 전체의 시작을
    요청한다. ``kafka_consumer``의 trigger_settling 서비스가 만들어 여기로
    넘긴다.
    """

    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float


@dataclass(frozen=True)
class IncidentOutcome:
    """분석 생명주기가 호출자에게 돌려주는 종료 상태와 산출물 정보.

    리포트 본문 대신 상태·누락·마지막 window 리포트를 요약하고 diagnostics로
    조회 상세를 제공한다.
    """

    incident_id: str
    status: IncidentStatus
    analysis_failed: bool = False
    gaps: tuple[str, ...] = ()
    report: LogAnalysisReport | None = None
    analysis_calls: int = 0
    reason: str = ""
    diagnostics: IncidentAnalysisDetails = field(default_factory=lambda: IncidentAnalysisDetails())


@dataclass(frozen=True)
class IncidentAnalysisDetails:
    """Public analysis details for entrypoints that need a human readout."""

    report: LogAnalysisReport | None = None
    observations: Observations = field(default_factory=Observations)
    evidence: tuple[Evidence, ...] = ()
    publication: ReportPublication = field(default_factory=ReportPublication)


class AnalyzeIncident:
    """Create state, run the analyzer, close, publish, and clean up an incident."""

    def __init__(
        self,
        *,
        incident_analyzer: IncidentAnalyzer,
        report_publisher: ReportPublisher,
        incident_timeout_seconds: float = INCIDENT_TIMEOUT_SECONDS,
    ) -> None:
        self._analyzer = incident_analyzer
        self._report_publisher = report_publisher
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

        analysis_failed = False
        forced: tuple[IncidentStatus, str] | None = None
        if token.is_cancelled:
            forced = (IncidentStatus.CANCELLED, token.reason or "취소됨")
        elif deadline.expired:
            analysis_failed = True
            forced = (IncidentStatus.FAILED, self._timeout_reason())
        else:
            analysis_failed, forced = await self._run_analyzer(incident, state, deadline)

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
            report=state.window_results[-1].report if state.window_results else None,
            analysis_calls=state.analysis_call_count,
            reason=state.closing_reason,
            diagnostics=diagnostics,
        )

    async def _run_analyzer(
        self, incident: Incident, state: IncidentState, deadline: Deadline
    ) -> tuple[bool, tuple[IncidentStatus, str] | None]:
        analyzer_task = asyncio.create_task(
            asyncio.to_thread(self._analyzer.analyze, incident, state)
        )
        try:
            result = await asyncio.wait_for(
                asyncio.shield(analyzer_task),
                timeout=deadline.remaining,
            )
        except TimeoutError:
            _logger.warning("[incident %s] execution timed out", incident.incident_id)
            # A running thread cannot be cancelled. Keep its worker occupied until
            # it finishes so client cleanup and analyzer concurrency remain safe.
            try:
                await asyncio.shield(analyzer_task)
            except Exception:
                _logger.exception(
                    "[incident %s] analyzer raised after timeout", incident.incident_id
                )
            return True, (IncidentStatus.FAILED, self._timeout_reason())
        except Exception:
            _logger.exception("[incident %s] analyzer raised", incident.incident_id)
            return True, (IncidentStatus.FAILED, "분석 Agent 실행이 예외로 끝났다")

        if not state.status.is_terminal():
            self._close(state, result.status, result.reason)
        for gap in result.gaps:
            if gap not in state.accumulated_gaps:
                state.accumulated_gaps.append(gap)
        return result.failed, None

    def _timeout_reason(self) -> str:
        return f"실행 시간 상한 {self._incident_timeout_seconds:.0f}초를 넘겼다"

    def _close(self, state: IncidentState, status: IncidentStatus, reason: str) -> None:
        state.status = status
        state.closing_reason = reason

    async def _deliver(
        self,
        incident: Incident,
        state: IncidentState,
        analysis_failed: bool,
        gaps: tuple[str, ...],
    ) -> IncidentAnalysisDetails:
        # window마다 검증된 리포트가 따로 있다. 합치지 않고 마지막 window의
        # 것을 대표로 전달하되, 어느 window의 검증 불일치든 운영자가 볼 수
        # 있게 gap에는 전부 싣는다.
        reports = [item.report for item in state.window_results]
        report = reports[-1] if reports else None
        observations = state.observations
        evidence = state.evidence
        all_gaps = list(gaps)
        for item in reports:
            all_gaps.extend(
                f"리포트 검증 불일치: {issue}" for issue in item.verification_issues
            )
        if state.closing_reason and state.status is not IncidentStatus.COMPLETED:
            all_gaps.append(f"Incident 종료 사유: {state.closing_reason}")
        rendered_report = to_incident_analysis_report(report, observations, evidence)
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
        return IncidentAnalysisDetails(
            report=report,
            observations=observations,
            evidence=tuple(evidence),
            publication=publication,
        )
