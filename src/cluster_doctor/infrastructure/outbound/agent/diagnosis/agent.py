"""Log Analysis SubAgent.

한 analysis window 안에서 **조사·RCA·리포트·검증을 전부** 책임진다. Supervisor는
이 경계 안을 보지 않는다.

    LogAnalysisRequest
        ↓
    EvidenceCollector  (datasource workflow 조율, Node Investigation 포함)
        ↓
    Evidence[]
        ↓
    Cross-source Analysis  (여기서 처음으로 원인을 묻는다)
        ↓
    Draft Report
        ↓
    Consistency Validator ──┐ MISMATCH
        │ PASS              ↓
        │            bounded revision (최대 MAX_REPORT_REVISIONS회)
        ↓                   │
    LogAnalysisResponse ←───┘

**tool loop가 아니다.** tool loop 오케스트레이터를 쓰지 않는 이유는 여기서
모델이 결정할 것이 "무엇이 의미 있는가"와 "원인이 무엇인가"뿐이기 때문이다.
어느 datasource를 어떤 순서로 부를지는 결정 사항이 아니라 절차이고, 절차를
모델에게 맡기면 상한이 프롬프트 문장이 된다.

**예외를 밖으로 내보내지 않는다.** 실패는 ``status=FAILED``로 표현한다. 예외로
올리면 Supervisor 사이클이 죽고, 그때까지 모은 Evidence와 관측값이 함께 사라진다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from functools import partial

from cluster_doctor.application.exception import (
    GuardrailViolation,
    LlmApiError,
    LlmResponseError,
)
from cluster_doctor.application.port.outbound.artifact_store import ArtifactStore
from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.node_log_fetcher import NodeLogFetcher
from cluster_doctor.application.port.outbound.node_resolver import NodeResolver
from cluster_doctor.application.service.guardrails import (
    MAX_REPORT_REVISIONS,
    validate_analysis_request,
)
from cluster_doctor.domain.model.evidence import Evidence
from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisRequest,
    LogAnalysisResponse,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.domain.model.log_analysis_report import LogAnalysisReport
from cluster_doctor.domain.model.log_entry import LogEntry, NodeLogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.common.litellm_client import complete
from cluster_doctor.infrastructure.outbound.agent.diagnosis.collector import (
    EvidenceCollector,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.prompts import (
    build_analysis_prompt,
    build_revision_prompt,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.run_state import (
    AnalysisRunState,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.schema import (
    DraftReport,
    parse_draft,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.validator import (
    validate_report,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.datasource.node_metric import (
    DEFAULT_THRESHOLDS,
    NodeMetricThresholds,
)

_logger = logging.getLogger(__name__)

_ANALYSIS_MAX_TOKENS = 8192


class DiagnosisAgentAdapter:
    """``LogAnalysisAgent`` 포트의 구현."""

    def __init__(
        self,
        *,
        provider: str,
        model: str,
        api_key: str,
        fetch_logs: Callable[[TimeRange], list[LogEntry]],
        fetch_node_logs: Callable[..., list[NodeLogEntry]],
        cluster: ClusterRepository,
        node_resolver: NodeResolver,
        node_log_fetcher: NodeLogFetcher,
        store: ArtifactStore,
        max_revisions: int = MAX_REPORT_REVISIONS,
        metric_thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS,
        call_llm: Callable[..., str] | None = None,
    ) -> None:
        # ``call_llm``을 주입할 수 있게 둔 것은 테스트를 위해서다. 기본값은
        # provider/model/api_key가 묶인 실제 호출자이고, 이 계층 아래(워크플로
        # 노드, Node Investigation)는 누구에게 묻는지 모른다 — 그 결정은
        # 조립 시점에 한 번만 이뤄진다.
        self._call_llm = call_llm or partial(
            _structured_call, provider=provider, model=model, api_key=api_key
        )
        self._fetch_logs = fetch_logs
        self._fetch_node_logs = fetch_node_logs
        self._cluster = cluster
        self._node_resolver = node_resolver
        self._node_log_fetcher = node_log_fetcher
        self._store = store
        self._max_revisions = max_revisions
        self._metric_thresholds = metric_thresholds

    def analyze(self, request: LogAnalysisRequest) -> LogAnalysisResponse:
        window = request.analysis_window
        try:
            validate_analysis_request(request)
        except GuardrailViolation as exc:
            _logger.error("[subagent] 요청 거절: %s", exc)
            return LogAnalysisResponse(
                status=LogAnalysisStatus.FAILED,
                analyzed_window=window,
                analysis_summary=f"요청이 런타임 제약을 어겼다: {exc}",
            )

        _logger.info(
            "[subagent] 분석 시작 %s ~ %s (목표: %s)",
            window.start.strftime("%Y-%m-%d %H:%M"),
            window.end.strftime("%H:%M"),
            request.analysis_goal or "(없음)",
        )

        state = AnalysisRunState(window)
        collector = EvidenceCollector(
            incident_id=request.incident_id,
            store=self._store,
            fetch_logs=self._fetch_logs,
            fetch_node_logs=self._fetch_node_logs,
            cluster=self._cluster,
            node_resolver=self._node_resolver,
            node_log_fetcher=self._node_log_fetcher,
            call_llm=self._call_llm,
            metric_thresholds=self._metric_thresholds,
        )
        collected = collector.collect(window, state)
        evidence = collected.evidence

        draft = self._draft_report(request, evidence, state)
        report = draft.to_domain(
            incident_id=request.incident_id,
            window=window,
            evidence_refs=tuple(item.evidence_id for item in evidence),
        )
        report = self._verify_and_revise(report, evidence, state.candidate_ids())

        self._store.merge_observations(request.incident_id, state.to_observations())
        report_ref = self._store.put_report(request.incident_id, report)

        status = self._final_status(report, draft, evidence, state)
        suggested = tuple(draft.parsed_windows()) if draft.needs_more_context else ()
        gaps = self._unresolved_gaps(collected, state, window)

        _logger.info(
            "[subagent] 분석 종료 status=%s verification=%s evidence=%d gaps=%d",
            status,
            report.verification_status,
            len(evidence),
            len(gaps),
        )
        return LogAnalysisResponse(
            status=status,
            analyzed_window=window,
            suggested_windows=suggested,
            unresolved_gaps=gaps,
            report_ref=report_ref,
            verification_status=report.verification_status,
            evidence_refs=tuple(item.evidence_id for item in evidence),
            gaps=tuple(state.gaps),
            analysis_summary=self._summary_for_supervisor(report, state),
        )

    # ── Cross-source Analysis ────────────────────────────────────────
    def _draft_report(
        self,
        request: LogAnalysisRequest,
        evidence: list[Evidence],
        state: AnalysisRunState,
    ) -> DraftReport:
        """모든 근거를 놓고 원인을 묻는다. 실패하면 빈 초안.

        근거가 하나도 없으면 부르지 않는다. 빈 목록을 주고 "원인을 찾으라"는
        호출은 비용만 들고, 그 답은 근거 없는 산문이 된다.
        """
        if not evidence:
            _logger.info("[subagent] 근거가 없어 원인 분석을 건너뛴다")
            return DraftReport()

        window = request.analysis_window
        prompt = build_analysis_prompt(
            cluster=request.cluster,
            window_label=(
                f"{window.start:%Y-%m-%d %H:%M} ~ {window.end:%H:%M} KST"
            ),
            analysis_goal=request.analysis_goal,
            evidence=evidence,
            observation_summary=state.summary_for_prompt(),
            candidates_for_prompt=state.candidates_for_prompt(),
            prior_summary=self._prior_summary(request.state_ref),
            gaps=tuple(state.gaps),
        )
        try:
            text = self._call_llm(
                [{"role": "user", "content": prompt}],
                _ANALYSIS_MAX_TOKENS,
                response_format=DraftReport,
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.error("[subagent] 원인 분석 실패: %s", exc)
            state.mark_gap(f"원인 분석 호출이 실패했다: {exc}")
            return DraftReport()
        return parse_draft(text)

    def _prior_summary(self, state_ref: str | None) -> str:
        """같은 Incident의 앞선 리포트 요약.

        **전문을 싣지 않는다.** 앞선 분석의 결론 한 문단이면 이번 구간을 어떤
        물음으로 볼지 정하는 데 충분하고, 전문을 실으면 호출마다 리포트가 하나씩
        더 붙어 Context가 분석 횟수만큼 불어난다.
        """
        if not state_ref:
            return ""
        prior = self._store.get_report(state_ref)
        if prior is None:
            return ""
        questions = "; ".join(prior.unresolved_questions[:3])
        return (
            f"[{prior.analyzed_from:%H:%M}~{prior.analyzed_to:%H:%M}] {prior.summary}"
            + (f" 미해결: {questions}" if questions else "")
        )

    # ── 검증과 수정 ──────────────────────────────────────────────────
    def _verify_and_revise(
        self,
        report: LogAnalysisReport,
        evidence: list[Evidence],
        candidate_ids: set[str],
    ) -> LogAnalysisReport:
        """불일치가 없을 때까지, **허용된 횟수 안에서만** 고친다.

        횟수 상한이 반드시 필요하다. 모델이 같은 지적을 이해하지 못하면 검증과
        수정이 서로를 부르며 끝나지 않고, 그 루프는 429가 날 때까지 돈다.
        상한에 닿으면 남은 불일치를 리포트에 **기록한 채** 내보낸다 — 지적을
        지우면 운영자가 검증을 통과한 리포트로 읽는다.
        """
        revisions = 0
        while True:
            result = validate_report(report, evidence, candidate_ids=candidate_ids)
            if result.passed:
                return report.model_copy(
                    update={
                        "verification_status": VerificationStatus.PASSED,
                        "verification_issues": (),
                        "revision_count": revisions,
                    }
                )

            if revisions >= self._max_revisions:
                _logger.warning(
                    "[subagent] revision 상한 %d회 도달 — 불일치 %d건을 남긴 채 종료",
                    self._max_revisions,
                    len(result.issues),
                )
                return report.model_copy(
                    update={
                        "verification_status": VerificationStatus.MISMATCH,
                        "verification_issues": tuple(result.issues),
                        "revision_count": revisions,
                    }
                )

            revisions += 1
            _logger.info(
                "[subagent] 불일치 %d건 — revision %d/%d",
                len(result.issues),
                revisions,
                self._max_revisions,
            )
            revised = self._revise(report, tuple(result.issues), evidence)
            if revised is None:
                # 수정 호출 자체가 실패했다. 더 시도하지 않는다 — 같은 실패가
                # 반복될 뿐이고, 원래 리포트는 그대로 유효하다.
                return report.model_copy(
                    update={
                        "verification_status": VerificationStatus.MISMATCH,
                        "verification_issues": tuple(result.issues),
                        "revision_count": revisions - 1,
                    }
                )
            report = revised

    def _revise(
        self,
        report: LogAnalysisReport,
        issues: tuple[str, ...],
        evidence: list[Evidence],
    ) -> LogAnalysisReport | None:
        prompt = build_revision_prompt(report=report, issues=issues, evidence=evidence)
        try:
            text = self._call_llm(
                [{"role": "user", "content": prompt}],
                _ANALYSIS_MAX_TOKENS,
                response_format=DraftReport,
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("[subagent] revision 호출 실패: %s", exc)
            return None
        return parse_draft(text).to_domain(
            incident_id=report.incident_id,
            window=TimeRange(start=report.analyzed_from, end=report.analyzed_to),
            evidence_refs=report.evidence_refs,
        )

    # ── 응답 조립 ────────────────────────────────────────────────────
    @staticmethod
    def _final_status(
        report: LogAnalysisReport,
        draft: DraftReport,
        evidence: list[Evidence],
        state: AnalysisRunState,
    ) -> LogAnalysisStatus:
        """어떤 상태로 끝났는가.

        우선순위가 있다. 분석이 성립하지 않은 것이 가장 무겁고, 그다음이 리포트를
        믿을 수 없는 것이며, 범위 확장 요청은 그 뒤다 — 세 가지가 함께 일어날 수
        있고, Supervisor에게는 가장 무거운 것을 먼저 알려야 한다.

        ``suggested_windows``는 상태와 무관하게 응답에 실리므로, 검증에 실패한
        분석도 Scope 확장 제안은 Supervisor에게 전달된다.
        """
        if state.degraded and not evidence:
            return LogAnalysisStatus.FAILED
        if report.verification_status is VerificationStatus.MISMATCH:
            return LogAnalysisStatus.VALIDATION_FAILED
        if draft.needs_more_context and draft.parsed_windows():
            return LogAnalysisStatus.NEED_MORE_CONTEXT
        return LogAnalysisStatus.COMPLETED

    @staticmethod
    def _unresolved_gaps(
        collected, state: AnalysisRunState, window: TimeRange
    ) -> tuple[TimeRange, ...]:
        """근거를 확보하지 못한 시간 범위.

        문장이 아니라 ``TimeRange``인 것이 요점이다. Supervisor가 다음 분석
        범위를 정할 때 쓰는 값이므로 계산 가능한 형태여야 한다. 사람이 읽을
        설명은 ``gaps``로 따로 남아 리포트 배너가 된다.
        """
        if state.degraded:
            return (window,)
        gaps = [
            TimeRange(start=minute, end=minute + timedelta(minutes=1))
            for minute in sorted(collected.failed_minutes)
        ]
        return tuple(gaps)

    @staticmethod
    def _summary_for_supervisor(report: LogAnalysisReport, state: AnalysisRunState) -> str:
        """Supervisor가 읽을 한두 문단. 리포트 전문이 아니다."""
        parts = [report.summary or "(요약 없음)"]
        if report.root_causes:
            cause = report.root_causes[0]
            parts.append(
                f"원인 후보: {cause.statement} (confidence={cause.confidence or '미기재'})"
            )
        if report.unresolved_questions:
            parts.append("미해결: " + "; ".join(report.unresolved_questions[:3]))
        if state.gaps:
            parts.append(f"확보하지 못한 근거 {len(state.gaps)}건")
        return " / ".join(parts)


def _structured_call(
    messages: list[dict],
    max_tokens: int,
    *,
    provider: str,
    model: str,
    api_key: str,
    response_format=None,
) -> str:
    """provider/model/api_key가 묶인 호출자.

    이 계층 아래(워크플로 노드, Validator)는 누구에게 묻는지 모른다 — 그 결정은
    조립 시점에 한 번만 이뤄진다.
    """
    return complete(
        messages=messages,
        provider=provider,
        model=model,
        api_key=api_key,
        max_tokens=max_tokens,
        response_format=response_format,
    )
