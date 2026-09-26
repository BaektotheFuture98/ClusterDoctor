"""근거를 놓고 원인을 묻고, 쓴 리포트를 검증하고, 지적을 받아 고친다.

수집(``collector.py``)과 리포트 작성이 갈라져 있는 이유는 Diagnosis SubAgent가
모델에게 그 둘을 **따로** 고르게 하기 때문이다. 한 덩어리로 묶여 있으면
"근거는 이미 모았으니 리포트만 다시 쓴다"가 표현되지 않고, 다시 쓸 때마다
데이터소스 조회 비용이 함께 든다.

검증과 수정은 여기 안에 있고 모델이 건드리지 못한다. 모델이 자기 출력을
채점하면 거의 통과하기 때문이다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from functools import partial

from cluster_doctor.exceptions import LlmApiError, LlmResponseError
from cluster_doctor.application.ports.artifact_store import ArtifactStore
from cluster_doctor.domain.incident.guardrails import MAX_REPORT_REVISIONS
from cluster_doctor.domain.diagnosis.evidence import Evidence
from cluster_doctor.domain.diagnosis.contracts import LogAnalysisRequest
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.domain.diagnosis.time_range import TimeRange
from cluster_doctor.adapters.outbound.deepagents.runtime.litellm_client import complete
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.prompts import (
    build_analysis_prompt,
    build_revision_prompt,
)
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.run_state import (
    AnalysisRunState,
)
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.schema import (
    DraftReport,
    parse_draft,
)
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.validator import (
    validate_report,
)

_logger = logging.getLogger(__name__)

_ANALYSIS_MAX_TOKENS = 8192


class ReportWriter:
    """초안 → 검증 → 수정. 리포트 하나가 만들어지는 전 구간."""

    def __init__(
        self,
        *,
        store: ArtifactStore,
        call_llm: Callable[..., str],
        max_revisions: int = MAX_REPORT_REVISIONS,
    ) -> None:
        self._store = store
        self._call_llm = call_llm
        self._max_revisions = max_revisions

    # ── Cross-source Analysis ────────────────────────────────────────
    def draft_report(
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
    def verify_and_revise(
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
    def unresolved_gaps(
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
    def summary_for_supervisor(report: LogAnalysisReport, state: AnalysisRunState) -> str:
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


def build_structured_call(
    *, provider: str, model: str, api_key: str
) -> Callable[..., str]:
    """provider/model/api_key를 한 번 묶어 둔 호출자를 만든다.

    이 계층 아래(워크플로 노드, Validator, ReportWriter)는 누구에게 묻는지
    모른다 — 그 결정은 조립 시점에 한 번만 이뤄진다.
    """
    return partial(_structured_call, provider=provider, model=model, api_key=api_key)


def _structured_call(
    messages: list[dict],
    max_tokens: int,
    *,
    provider: str,
    model: str,
    api_key: str,
    response_format=None,
) -> str:
    return complete(
        messages=messages,
        provider=provider,
        model=model,
        api_key=api_key,
        max_tokens=max_tokens,
        response_format=response_format,
    )
