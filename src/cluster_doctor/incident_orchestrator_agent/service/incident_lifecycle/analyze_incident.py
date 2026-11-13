"""정착(settled)된 incident 하나의 전체 분석 lifecycle을 실행한다."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import datetime

from cluster_doctor.incident_orchestrator_agent.model.incident import (
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisDetails,
    IncidentAnalysisResult,
    IncidentOutcome,
    StartIncident,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    INCIDENT_TIMEOUT_SECONDS,
    CancellationToken,
    Deadline,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.incident_analyzer import (
    IncidentAnalyzer,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import verification_gap
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.report_publisher import (
    ReportPublisher,
)
from cluster_doctor.log_context import bind_incident_id

_logger = logging.getLogger(__name__)


class AnalyzeIncident:
    """입력/결과 경계를 실행하고 incident를 닫은 뒤 발행한다."""

    def __init__(
        self,
        *,
        incident_analyzer: IncidentAnalyzer,
        report_publisher: ReportPublisher,
        # 테스트가 타임아웃 경로를 빨리 타려고 덮어쓴다. 운영은 기본값.
        incident_timeout_seconds: float = INCIDENT_TIMEOUT_SECONDS,
    ) -> None:
        self._analyzer = incident_analyzer
        self._report_publisher = report_publisher
        self._incident_timeout_seconds = incident_timeout_seconds

    async def handle(
        self, command: StartIncident, *, cancellation: CancellationToken | None = None
    ) -> IncidentOutcome:
        incident = command.incident
        with bind_incident_id(incident.incident_id):
            return await self._handle(command, cancellation or CancellationToken())

    async def _handle(
        self, command: StartIncident, token: CancellationToken
    ) -> IncidentOutcome:
        incident = command.incident
        # 상한은 incident 하나의 분석·발행 시간이다. 정착·incident 구성 대기는 빼지
        # 않는다 — 대기가 최대 범위 상한(20분)까지 늘어 빼면 분석할 시간이 남지 않는다.
        deadline = Deadline(self._incident_timeout_seconds)
        analysis_failed = False
        result = IncidentAnalysisResult(status=IncidentStatus.ANALYZING)
        forced: tuple[IncidentStatus, str] | None = None
        if token.is_cancelled:
            forced = (IncidentStatus.CANCELLED, token.reason or "취소됨")
        elif deadline.expired:
            analysis_failed = True
            forced = (IncidentStatus.FAILED, self._timeout_reason())
        else:
            result, analysis_failed, forced = await self._run_analyzer(
                command, deadline
            )
        if forced is not None:
            result = replace(result, status=forced[0], reason=forced[1], failed=True)
        gaps = tuple(dict.fromkeys((*command.health_gaps, *result.gaps)))
        diagnostics = await self._deliver(
            result, analysis_failed or result.failed, gaps, cluster=incident.cluster,
            onset=command.observed_start,
        )
        return IncidentOutcome(
            incident_id=incident.incident_id,
            status=result.status,
            analysis_failed=(analysis_failed or result.failed),
            gaps=gaps,
            report=result.report,
            analysis_calls=result.analysis_calls,
            reason=result.reason,
            diagnostics=diagnostics,
        )

    async def _run_analyzer(
        self, command: StartIncident, deadline: Deadline
    ) -> tuple[IncidentAnalysisResult, bool, tuple[IncidentStatus, str] | None]:
        analyzer_task = asyncio.create_task(
            asyncio.to_thread(self._analyzer.analyze, command.to_request())
        )
        try:
            result = await asyncio.wait_for(
                asyncio.shield(analyzer_task),
                timeout=deadline.remaining,
            )
        except TimeoutError:
            _logger.warning("execution timed out")
            # 실행 중인 스레드는 취소할 수 없다. 끝날 때까지 워커를 붙잡아 두어야
            # client 정리와 analyzer 동시성 제한이 깨지지 않는다.
            try:
                result = await asyncio.shield(analyzer_task)
            except Exception:
                _logger.exception("analyzer raised after timeout")
                result = IncidentAnalysisResult(status=IncidentStatus.FAILED)
            return result, True, (IncidentStatus.FAILED, self._timeout_reason())
        except Exception:
            _logger.exception("analyzer raised")
            return (
                IncidentAnalysisResult(status=IncidentStatus.FAILED),
                True,
                (IncidentStatus.FAILED, "분석 Agent 실행이 예외로 끝났다"),
            )
        return result, result.failed, None

    def _timeout_reason(self) -> str:
        return f"실행 시간 상한 {self._incident_timeout_seconds:.0f}초를 넘겼다"

    async def _deliver(
        self,
        result: IncidentAnalysisResult,
        analysis_failed: bool,
        gaps: tuple[str, ...],
        *,
        cluster: str = "",
        onset: datetime | None = None,
    ) -> IncidentAnalysisDetails:
        # report는 사건 종합 리포트다. 그 검증 이슈도 운영자가 볼 수 있게
        # 발행 gap에 싣는다. 구간별 검증 이슈는 adapter가 이미 gaps에 넣었다.
        report = result.report
        observations = result.observations
        evidence = result.evidence
        all_gaps = list(gaps)
        if report is not None:
            all_gaps.extend(
                verification_gap(issue) for issue in report.verification_issues
            )
        if result.reason and result.status is not IncidentStatus.COMPLETED:
            all_gaps.append(f"Incident 종료 사유: {result.reason}")
        rendered_report = to_incident_analysis_report(
            report, observations, evidence, cluster=cluster, onset=onset
        )
        publication = ReportPublication()
        try:
            publication = await self._report_publisher.publish(
                rendered_report,
                gaps=tuple(dict.fromkeys(all_gaps)),
                analysis_failed=analysis_failed,
            )
        except Exception:
            _logger.exception("report delivery failed")
        return IncidentAnalysisDetails(
            report=report,
            observations=observations,
            evidence=tuple(evidence),
            publication=publication,
        )
