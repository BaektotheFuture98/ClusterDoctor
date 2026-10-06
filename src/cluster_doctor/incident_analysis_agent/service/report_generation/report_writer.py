"""근거로 초안을 작성하고, 호출자가 전달한 지적으로 한 번 수정한다.

수집(``evidence_collection/collector.py``)과 리포트 작성이 갈라져 있는
이유는 Analysis SubAgent가 모델에게 그 둘을 **따로** 고르게 하기 때문이다.
한 덩어리로 묶여 있으면 "근거는 이미 모았으니 리포트만 다시 쓴다"가 표현되지
않고, 다시 쓸 때마다 데이터소스 조회 비용이 함께 든다.

검증과 수정 횟수 결정은 호출자의 책임이다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import partial

from cluster_doctor.exceptions import LlmApiError, LlmResponseError
from cluster_doctor.incident_analysis_agent.agent.runtime.llm_call_log import llm_label
from cluster_doctor.incident_analysis_agent.model.analysis_contract import LogAnalysisRequest
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import build_analysis_context
from cluster_doctor.incident_analysis_agent.service.observation.builder import ObservationBuilder
from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
    build_analysis_prompt,
    build_incident_synthesis_messages,
    build_incident_review_messages,
    build_revision_prompt,
    _INCIDENT_REVIEW_INSTRUCTIONS,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.schema import (
    DraftReport,
    parse_draft,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.litellm_client import complete

_logger = logging.getLogger(__name__)


def _incident_response_format() -> dict:
    """Constrain JSON syntax without a large nested provider schema."""
    return {"type": "json_schema", "json_schema": {
        "name": "incident_report_object",
        "schema": {"type": "object", "additionalProperties": True},
    }}


class ReportWriter:
    """구조화된 초안 작성과 단일 수정 호출. LLM 호출 하나만 의존한다."""

    def __init__(self, *, call_llm: Callable[..., str]) -> None:
        self._call_llm = call_llm

    # ── Cross-source Analysis ────────────────────────────────────────
    def draft_report(
        self,
        request: LogAnalysisRequest,
        evidence: list[Evidence],
        state: ObservationBuilder,
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
            prior_summary=self._prior_summary(request.prior_report),
            gaps=tuple(state.gaps),
            analysis_context=build_analysis_context(state.to_observations(), evidence),
        )
        try:
            with llm_label("report_draft"):
                text = self._call_llm(
                    [{"role": "user", "content": prompt}],
                    response_format=DraftReport,
                )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.error("[subagent] 원인 분석 실패: %s", exc)
            state.mark_gap(f"원인 분석 호출이 실패했다: {exc}")
            return DraftReport()
        return parse_draft(text)

    def draft_incident(self, *, incident_id: str, cluster: str,
                       window_reports: list[LogAnalysisReport],
                       observations: Observations, evidence: list[Evidence]) -> LogAnalysisReport:
        """Summarize retained incident facts without another fetch or agent loop."""
        import json
        from cluster_doctor.incident_analysis_agent.service.report_generation.schema import IncidentDraftReport
        from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
        spans = [*observations.requested,
                 *((r.analyzed_from, r.analyzed_to) for r in window_reports)]
        start, end = min(a for a, _ in spans), max(b for _, b in spans)
        # Prior diagnoses can anchor the final model to unsupported claims.
        # Retain the span metadata; independently diagnose accumulated facts.
        previous = [{
            "from": r.analyzed_from.isoformat(), "to": r.analyzed_to.isoformat(),
        } for r in window_reports]
        previous_json = json.dumps({"windows": previous}, ensure_ascii=False)
        if len(previous_json) > 12000:
            raise ValueError("구간별 범위 입력이 종합 예산을 초과했다")
        messages = build_incident_synthesis_messages(
            cluster=cluster, analyzed_from=start, analyzed_to=end,
            previous_windows_json=previous_json,
            analysis_context=build_analysis_context(observations, evidence, max_chars=40000),
        )
        if sum(len(message['content']) for message in messages) > 60000:
            raise ValueError("사건 종합 입력이 예산을 초과했다")
        with llm_label("incident_synthesis"):
            # Keep the provider grammar small; validate the complete DTO below.
            text = self._call_llm(messages, response_format=_incident_response_format())
        # A malformed synthesis must not silently become an empty successful report.
        draft = IncidentDraftReport.model_validate_json(text)
        if not draft.summary.strip():
            raise ValueError("사건 종합 응답에 요약이 없다")
        editable = draft.model_dump_json(include={
            "summary", "summary_evidence_refs", "timeline", "findings",
            "root_causes", "recommendations", "unresolved_questions",
        })
        context_budget = min(40000, 60000 - len(editable) - len(_INCIDENT_REVIEW_INSTRUCTIONS) - 500)
        if context_budget < 1000:
            raise ValueError("사건 교정 입력이 예산을 초과했다")
        review_messages = build_incident_review_messages(
            analyzed_from=start, analyzed_to=end, draft_json=editable,
            analysis_context=build_analysis_context(observations, evidence, max_chars=context_budget),
        )
        if sum(len(message['content']) for message in review_messages) > 60000:
            raise ValueError("사건 교정 입력이 예산을 초과했다")
        with llm_label("incident_review"):
            reviewed = self._call_llm(review_messages, response_format=_incident_response_format())
        draft = IncidentDraftReport.model_validate_json(reviewed)
        if not draft.summary.strip():
            raise ValueError("사건 교정 응답에 요약이 없다")
        first = window_reports[0]
        return draft.to_domain(
            incident_id=incident_id,
            window=TimeRange(first.analyzed_from, first.analyzed_to),
            evidence_refs=tuple(e.evidence_id for e in evidence),
        ).model_copy(update={"analyzed_from": start, "analyzed_to": end})

    @staticmethod
    def _prior_summary(prior_report: LogAnalysisReport | None) -> str:
        """같은 Incident의 앞선 리포트 요약.

        **전문을 싣지 않는다.** 앞선 분석의 결론 한 문단이면 이번 구간을 어떤
        물음으로 볼지 정하는 데 충분하고, 전문을 실으면 호출마다 리포트가 하나씩
        더 붙어 Context가 분석 횟수만큼 불어난다.
        """
        if prior_report is None:
            return ""
        questions = "; ".join(prior_report.unresolved_questions[:3])
        return (
            f"[{prior_report.analyzed_from:%H:%M}~{prior_report.analyzed_to:%H:%M}] "
            f"{prior_report.summary}"
            + (f" 미해결: {questions}" if questions else "")
        )

    def revise_report(
        self,
        report: LogAnalysisReport,
        issues: tuple[str, ...],
        evidence: list[Evidence],
        *, observations: Observations | None = None,
        incident_final: bool = False,
    ) -> LogAnalysisReport | None:
        prompt = build_revision_prompt(report=report, issues=issues, evidence=evidence,
            analysis_context=build_analysis_context(observations or Observations(), evidence),
            incident_final=incident_final)
        messages = ([{"role": "system", "content": _INCIDENT_REVIEW_INSTRUCTIONS}]
                    if incident_final else []) + [{"role": "user", "content": prompt}]
        if sum(len(message['content']) for message in messages) > 60000:
            _logger.warning("수정 입력이 예산을 초과해 기존 보고서를 보존한다")
            return None
        try:
            with llm_label("report_revision"):
                text = self._call_llm(
                    messages,
                    response_format=_incident_response_format() if incident_final else DraftReport,
                )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("[subagent] revision 호출 실패: %s", exc)
            return None
        from cluster_doctor.incident_analysis_agent.model.time_range import split_span

        try:
            from cluster_doctor.incident_analysis_agent.service.report_generation.schema import IncidentDraftReport
            draft_type = IncidentDraftReport if incident_final else DraftReport
            draft = draft_type.model_validate_json(text)
            if not draft.summary.strip():
                return None
        except Exception:
            _logger.warning("revision 응답을 읽지 못해 기존 보고서를 보존한다")
            return None
        return draft.to_domain(
            incident_id=report.incident_id,
            window=split_span(report.analyzed_from, report.analyzed_to)[0],
            evidence_refs=report.evidence_refs,
        ).model_copy(update={
            "analyzed_from": report.analyzed_from,
            "analyzed_to": report.analyzed_to,
        })

    # ── 응답 조립 ────────────────────────────────────────────────────
    @staticmethod
    def summary_for_supervisor(report: LogAnalysisReport, state: ObservationBuilder) -> str:
        """Supervisor가 읽을 한두 문단. 리포트 전문이 아니다."""
        from cluster_doctor.incident_analysis_agent.model.report import VerificationStatus
        if report.verification_status != VerificationStatus.PASSED:
            return f"근거 {len(report.evidence_refs)}건, 분석 해석 {report.verification_status.value}."
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
        response_format=response_format,
    )
