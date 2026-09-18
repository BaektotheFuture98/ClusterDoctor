"""모델이 채우는 리포트 스키마. 판단만 담는다.

관측값(분 단위 건수·노드 최대값·상태 이력)은 여기 없다. 코드가 모아 리포트에
직접 싣는다 — 모델이 옮겨 적게 시키면 틀린다는 것을 이 저장소는 실측으로 두 번
확인했다(``domain/model/diagnosis_report.py`` docstring).

**모든 주장이 evidence_refs를 달고 다닌다.** 그것이 Validator를 가능하게 하는
구조다. 근거 참조가 없는 주장은 "검증할 수 없다"가 아니라 "근거가 없다"로
판정된다.

길이를 제한하지 않는다. 자르면 정보를 잃고, 한국어는 글자 단위로 잘리므로
``follower_check``가 ``follower``/``_check``로 쪼개진다. ``Field(max_length=...)``
같은 제약도 쓰지 않는다 — 초과가 곧 ``ValidationError``이고, 그것이 재시도
루프가 된다. 이 저장소는 429를 최우선 제약으로 다뤄 재시도를 0으로 두는 곳이다.

같은 이유로 **모든 필드에 기본값이 있다.** required 필드 하나가 빠져도 같은
재시도 루프가 된다.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field, field_validator

from cluster_doctor.domain.model.diagnosis_report import SuspectPick as DomainSuspectPick
from cluster_doctor.domain.model.log_analysis import VerificationStatus
from cluster_doctor.domain.model.log_analysis_report import (
    LogAnalysisReport,
    ReportFinding,
    RootCause,
    TimelineEvent,
)
from cluster_doctor.domain.model.time_range import TimeRange, split_span
from cluster_doctor.infrastructure.outbound.agent.common.kst import parse_kst

_logger = logging.getLogger(__name__)

_SEVERITIES = ("Critical", "Warning", "Info")
_CONFIDENCES = ("High", "Medium", "Low")


class TimelineEntry(BaseModel):
    at: str = Field(default="", description="ISO 8601 시각. 근거의 시각을 그대로 쓴다.")
    description: str = Field(default="", description="그 시각에 무엇이 일어났는가. 한 문장.")
    evidence_refs: list[str] = Field(
        default=[], description="이 줄의 근거가 된 Evidence id. 예: E-abc-3"
    )


class Finding(BaseModel):
    severity: str = Field(default="", description="Critical / Warning / Info 중 하나.")
    title: str = Field(default="", description="무엇이 문제인가. 한 문장.")
    detail: str = Field(default="", description="관찰된 사실만. 원인 추정은 root_causes에 쓴다.")
    evidence_refs: list[str] = Field(default=[], description="근거 Evidence id.")

    @field_validator("severity", mode="before")
    @classmethod
    def _normalize(cls, value: object) -> str:
        """셋 중 하나로 맞춘다. 아니면 **빈 값**으로 둔다.

        기본값을 ``"Info"``로 두는 것은 이미 실패한 길이다 — 실측에서 25초 지연과
        노드 19대 타임아웃이 전부 Info로 나왔는데, 모델이 필드를 채우지 않아
        기본값이 그대로 실린 것이었다. 빈 값이면 렌더러가 배지를 붙이지 않아
        "분류하지 않았다"와 "Info로 분류했다"가 화면에서 구별된다.
        """
        text = str(value or "").strip().capitalize()
        if text in _SEVERITIES:
            return text
        if text:
            _logger.warning("알 수 없는 severity=%r — 분류 없음으로 둔다", value)
        return ""


class CauseEntry(BaseModel):
    statement: str = Field(default="", description="가장 유력한 원인 하나. 한두 문장.")
    confidence: str = Field(default="", description="High / Medium / Low.")
    supporting_evidence_refs: list[str] = Field(
        default=[], description="이 결론을 뒷받침하는 Evidence id."
    )
    counter_evidence_refs: list[str] = Field(
        default=[], description="이 결론과 맞지 않는 Evidence id. 없으면 빈 배열."
    )

    @field_validator("confidence", mode="before")
    @classmethod
    def _normalize(cls, value: object) -> str:
        text = str(value or "").strip().capitalize()
        return text if text in _CONFIDENCES else ""


class SuspectPick(BaseModel):
    """코드가 제시한 후보 중 모델이 고른 것.

    쿼리 원문도, took도, 노드명도 여기 없다. 그 값들은 코드가 ``candidate_id``로
    조인해 붙인다 — 모델이 옮겨 적으면 틀리고, 실제로 틀렸다.
    """

    candidate_id: str = Field(default="", description="코드가 준 id. 예: C1")
    reason: str = Field(default="", description="왜 문제로 보는가. 한두 문장.")

    @field_validator("candidate_id", mode="before")
    @classmethod
    def _clean(cls, value: object) -> str:
        return str(value or "").strip()


class WindowSuggestion(BaseModel):
    start_iso: str = ""
    end_iso: str = ""


class DraftReport(BaseModel):
    """한 analysis window에 대한 모델의 판단 전부."""

    summary: str = Field(default="", description="이 구간에서 관찰된 것의 요약. 한 문단.")
    timeline: list[TimelineEntry] = Field(
        default=[], description="사고 전개를 시간순으로. 근거가 있는 시각만."
    )
    findings: list[Finding] = Field(
        default=[], description="근거를 댈 수 있는 문제만. 이상이 없으면 빈 배열."
    )
    root_causes: list[CauseEntry] = Field(
        default=[],
        description=(
            "원인 후보. 근거가 부족하면 confidence를 Low로 두거나 비운다. "
            "확정적으로 쓰지 않는다."
        ),
    )
    unresolved_questions: list[str] = Field(
        default=[], description="이 구간의 근거만으로는 답할 수 없는 물음."
    )
    recommendations: list[str] = Field(
        default=[], description="운영자가 취할 수 있는 조치. 근거 없는 일반론은 쓰지 않는다."
    )
    suspect_picks: list[SuspectPick] = Field(
        default=[],
        description=(
            "느린 요청 후보 목록에서 문제로 보이는 것. id와 이유만 쓴다. "
            "수치와 쿼리 원문은 코드가 붙인다."
        ),
    )
    needs_more_context: bool = Field(
        default=False,
        description=(
            "이 구간 **밖의** 시간을 봐야 답할 수 있으면 true. "
            "구간 안에서 더 조사하면 되는 것은 여기 해당하지 않는다."
        ),
    )
    suggested_windows: list[WindowSuggestion] = Field(
        default=[], description="needs_more_context가 true일 때 필요한 시간 범위."
    )

    def to_domain(
        self,
        *,
        incident_id: str,
        window: TimeRange,
        evidence_refs: tuple[str, ...],
        revision_count: int = 0,
    ) -> LogAnalysisReport:
        """도메인 타입으로 옮긴다. 여기가 pydantic이 끝나는 경계다.

        **잘못된 참조를 지우지 않는다.** 없는 id를 인용한 것은 Validator가
        잡아야 할 사실이고, 여기서 조용히 걸러 내면 검증이 늘 통과한다.
        """
        return LogAnalysisReport(
            incident_id=incident_id,
            analyzed_from=window.start,
            analyzed_to=window.end,
            summary=self.summary,
            timeline=tuple(
                event
                for event in (self._timeline_event(entry) for entry in self.timeline)
                if event is not None
            ),
            findings=tuple(
                ReportFinding(
                    severity=item.severity,
                    title=item.title,
                    detail=item.detail,
                    evidence_refs=tuple(item.evidence_refs),
                )
                for item in self.findings
            ),
            root_causes=tuple(
                RootCause(
                    statement=item.statement,
                    confidence=item.confidence,
                    supporting_evidence_refs=tuple(item.supporting_evidence_refs),
                    counter_evidence_refs=tuple(item.counter_evidence_refs),
                )
                for item in self.root_causes
            ),
            unresolved_questions=tuple(self.unresolved_questions),
            recommendations=tuple(self.recommendations),
            suspect_picks=tuple(
                DomainSuspectPick(candidate_id=item.candidate_id, reason=item.reason)
                for item in self.suspect_picks
                if item.candidate_id
            ),
            evidence_refs=evidence_refs,
            verification_status=VerificationStatus.NOT_VERIFIED,
            revision_count=revision_count,
        )

    @staticmethod
    def _timeline_event(entry: TimelineEntry) -> TimelineEvent | None:
        """시각을 못 읽은 줄은 버린다.

        이것만은 버린다 — 시각 없는 타임라인 항목은 타임라인이 아니고, 임의의
        값을 채우면 리포트가 거짓을 말한다. 내용 자체는 ``summary``와
        ``findings``에 남아 있으므로 잃는 것이 크지 않다.
        """
        try:
            at = parse_kst(entry.at)
        except (ValueError, TypeError):
            _logger.warning("타임라인 시각을 읽지 못했다: %r — 그 줄을 버린다", entry.at)
            return None
        return TimelineEvent(
            at=at,
            description=entry.description,
            evidence_refs=tuple(entry.evidence_refs),
        )

    def parsed_windows(self) -> list[TimeRange]:
        """제안 구간을 도메인 타입으로. 읽지 못한 것은 버린다.

        10분을 넘는 제안은 거절하지 않고 **쪼갠다**. 상한의 근거는 한 번의
        조회가 부담하는 팬아웃 비용이지 "그 시간대를 보면 안 된다"가 아니다.
        거절하면 모델이 정당하게 요청한 15분 구간이 통째로 사라진다.
        """
        windows: list[TimeRange] = []
        for item in self.suggested_windows:
            try:
                windows.extend(
                    split_span(parse_kst(item.start_iso), parse_kst(item.end_iso))
                )
            except Exception as exc:
                _logger.info(
                    "제안 구간을 읽지 못했다 (%r ~ %r): %s",
                    item.start_iso,
                    item.end_iso,
                    exc,
                )
        return windows


def parse_draft(text: str) -> DraftReport:
    """구조화 응답을 ``DraftReport``로. 실패하면 빈 초안.

    예외를 올리지 않는 이유: 이 경로의 실패는 형식 이탈이고, 그때도 Evidence는
    온전하다. 빈 초안을 돌려주면 Validator가 "근거를 인용하지 않았다"로 잡고,
    호출자가 그 사실을 gap으로 남긴다 — 리포트는 관측값과 함께 전달된다.
    """
    try:
        return DraftReport.model_validate_json(text)
    except Exception as exc:
        _logger.warning("리포트 초안을 읽지 못했다: %s", exc)
        return DraftReport()
