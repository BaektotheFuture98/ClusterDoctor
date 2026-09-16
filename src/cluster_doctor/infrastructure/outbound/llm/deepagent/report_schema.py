"""모델이 채우는 리포트 스키마. 판단만 담는다.

관측값(분 단위 건수·노드 최대값·마스터 로그·상태 이력·느린 요청 수치)은 여기
없다. 코드가 모아 리포트에 직접 싣는다 — 모델이 옮겨 적게 시키면 틀린다는 것을
실측으로 두 번 확인했다(``domain/model/diagnosis_report.py`` docstring).

pydantic이 어댑터 계층에 있고 도메인은 dataclass인 것도 같은 경계다. ``to_domain``
한 함수가 그 경계를 지킨다 — 없애면 도메인이 pydantic과 langchain의 스키마 규약에
묶이고, provider를 바꾸는 순간 도메인이 흔들린다.

## 길이를 제한하지 않는 이유

**모델이 쓴 것은 자르지 않는다.** 자르면 정보를 잃고, 한국어는 글자 단위로
잘리므로 ``follower_check``가 ``follower`` / ``_check``로 쪼개진다. 리포트는
운영자가 읽는 것이고, 끊긴 문장은 읽을 수 없다.

한도를 둘 이유가 실측으로도 없었다. 실제 리포트에서 근본 원인 문단은 154자,
문제점 항목은 57자였다. 한국어는 조밀해서 네 문장짜리 문단이 350자 정도다.
있지도 않은 위험을 막으려고 정보를 잃는 장치를 들일 이유가 없다.

출력이 통째로 잘릴 위험도 이 경로에는 없다. 오케스트레이터의 ``ChatLiteLLM``에는
``max_tokens``가 걸려 있지 않고(8192는 langgraph의 분별·종합 호출용 상수다),
이 스키마는 판단만 담아 애초에 짧다.

``Field(max_length=...)`` 같은 **제약**도 쓰지 않는다. 초과가 곧
``ValidationError``이고, 이 스키마는 ``ToolStrategy``가 평범한 tool 하나로
바인딩하므로 검증 실패가 재시도로 돌아온다. deepagents의 ``recursion_limit``은
9,999라 프레임워크도 막지 않고, 이 저장소는 429를 최우선 제약으로 다뤄 재시도를
일부러 0으로 둔 곳이다.

길이는 **프롬프트와 필드 설명으로만 유도한다.** 지시를 어겨도 리포트는 그대로
실리고, 어겼다는 사실만 경고로 남는다.

같은 이유로 **모든 필드에 기본값이 있다.** required 필드 하나가 빠져도 같은
재시도 루프가 된다.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field, field_validator

from cluster_doctor.domain.model.diagnosis_report import (
    Finding as DomainFinding,
    Narrative,
    SuspectPick as DomainSuspectPick,
)

_logger = logging.getLogger(__name__)

_SEVERITIES = ("Critical", "Warning", "Info")


def _note_text(value: object, guide: int, label: str) -> object:
    """길이를 재서 남기기만 한다. 값은 손대지 않는다.

    경고를 남기는 이유는 프롬프트의 유도가 먹히는지 보기 위해서다. 필드가 계속
    안내보다 길게 나오면 안내가 틀렸거나 스키마가 잘못 쪼개진 것이고, 그 신호는
    로그에만 있어도 충분하다 — 리포트는 그대로 온전하다.
    """
    if isinstance(value, str) and len(value) > guide:
        _logger.warning(
            "리포트 필드 %s가 안내(%d자)보다 길다(%d자) — 그대로 싣는다",
            label, guide, len(value),
        )
    return value


def _note_items(value: object, guide_items: int, guide_chars: int, label: str) -> object:
    """리스트 길이와 각 항목 길이를 재서 남기기만 한다. 값은 손대지 않는다.

    리스트가 아닌 값이 오면 그대로 돌려보낸다. 여기서 타입까지 바로잡으려 들면
    조용히 이상한 모양을 만들고, 그 판정은 pydantic이 하는 편이 정확하다.
    """
    if not isinstance(value, list):
        return value
    if len(value) > guide_items:
        _logger.warning(
            "리포트 필드 %s의 항목이 안내(%d개)보다 많다(%d개) — 그대로 싣는다",
            label, guide_items, len(value),
        )
    for item in value:
        _note_text(item, guide_chars, f"{label}[]")
    return value


class Finding(BaseModel):
    """모델이 지목한 문제 하나."""

    severity: str = Field(
        default="",
        description="Critical / Warning / Info 중 하나. 반드시 고른다.",
    )
    title: str = Field(default="", description="무엇이 문제인가. 한 문장.")
    evidence: list[str] = Field(
        default=[],
        description="근거. 로그 원문이나 수치를 시각과 함께 짧게 인용한다. 두 개면 충분하다.",
    )

    @field_validator("severity", mode="before")
    @classmethod
    def _normalize_severity(cls, value: object) -> str:
        """셋 중 하나로 맞춘다. 아니면 **빈 값**으로 둔다.

        기본값이 ``"Info"``였을 때 실측에서 문제가 났다. 25초 쿼리 지연과 노드
        19대 타임아웃이 전부 ``Info``로 나왔는데, 모델이 그 필드를 채우지 않아
        기본값이 그대로 실린 것이었다. 빈 칸이 그럴듯한 값으로 채워지는 것은
        관측값 쪽에서 이미 한 번 당한 실패다(``slowlog=264``) — 판단 쪽에도
        같은 함정을 두지 않는다.

        빈 값이면 렌더러가 배지를 붙이지 않는다. "분류하지 않았다"와 "Info로
        분류했다"가 화면에서 구별된다.

        ``Literal``로 강제하지 않는 이유는 이 모듈 docstring과 같다 — 모델이
        "중간" 같은 말을 쓰면 그것이 재시도 루프가 된다. 배지 색 하나 때문에
        진단을 다시 돌릴 이유가 없다.
        """
        text = str(value or "").strip().capitalize()
        if text in _SEVERITIES:
            return text
        if text:
            _logger.warning("알 수 없는 severity=%r — 분류 없음으로 둔다", value)
        return ""

    @field_validator("title", mode="before")
    @classmethod
    def _note_title(cls, value: object) -> object:
        return _note_text(value, 120, "findings[].title")

    @field_validator("evidence", mode="before")
    @classmethod
    def _note_evidence(cls, value: object) -> object:
        return _note_items(value, 2, 240, "findings[].evidence")


class SuspectPick(BaseModel):
    """코드가 제시한 후보 중 모델이 고른 것.

    쿼리 원문도, took도, 노드명도 여기 없다. 그 값들은 코드가
    ``candidate_id``로 조인해 붙인다 — 모델이 옮겨 적으면 틀리고, 실제로
    틀렸다(``took=미확인``).
    """

    candidate_id: str = Field(default="", description="코드가 준 id. 예: C1")
    reason: str = Field(default="", description="왜 문제로 보는가. 한두 문장.")

    @field_validator("candidate_id", mode="before")
    @classmethod
    def _clean_id(cls, value: object) -> str:
        return str(value or "").strip()

    @field_validator("reason", mode="before")
    @classmethod
    def _note_reason(cls, value: object) -> object:
        return _note_text(value, 200, "suspect_picks[].reason")


class ReportNarrative(BaseModel):
    """진단 리포트의 판단 부분. 관측값은 담지 않는다."""

    headline: str = Field(default="", description="한 문장 결론.")
    context: list[str] = Field(
        default=[],
        description=(
            "코드가 알 수 없는 맥락만. 유입 시각·분석 구간·시각 기준은 코드가 "
            "싣는다. 예: '유입이 지속되는 중에 분석했다', '구간을 5분 당긴 이유'."
        ),
    )
    findings: list[Finding] = Field(
        default=[],
        description="근거를 댈 수 있는 문제만. 이상이 없으면 빈 배열.",
    )
    root_cause: str = Field(default="", description="가장 유력한 근본 원인 하나.")
    supporting: list[str] = Field(default=[], description="이 결론을 뒷받침하는 관찰.")
    contradicting: list[str] = Field(
        default=[], description="이 결론과 맞지 않는 관찰. 없으면 빈 배열."
    )
    unverified: list[str] = Field(
        default=[], description="봤어야 했는데 못 본 것. 없으면 빈 배열."
    )
    suspect_picks: list[SuspectPick] = Field(
        default=[], description="느린 요청 후보 중 문제로 보이는 것. id와 이유만."
    )
    recommendations: list[str] = Field(default=[], description="권장 조치.")

    @field_validator("headline", "root_cause", mode="before")
    @classmethod
    def _note_strings(cls, value: object, info) -> object:
        guides = {"headline": 160, "root_cause": 800}
        return _note_text(value, guides[info.field_name], info.field_name)

    @field_validator(
        "context", "supporting", "contradicting", "unverified", "recommendations",
        mode="before",
    )
    @classmethod
    def _note_lists(cls, value: object, info) -> object:
        guides = {
            "context": (4, 200),
            "supporting": (4, 200),
            "contradicting": (4, 200),
            "unverified": (4, 200),
            "recommendations": (5, 200),
        }
        guide_items, guide_chars = guides[info.field_name]
        return _note_items(value, guide_items, guide_chars, info.field_name)

    @field_validator("findings", "suspect_picks", mode="before")
    @classmethod
    def _note_objects(cls, value: object, info) -> object:
        guides = {"findings": 5, "suspect_picks": 3}
        guide_items = guides[info.field_name]
        if isinstance(value, list) and len(value) > guide_items:
            _logger.warning(
                "리포트 필드 %s의 항목이 안내(%d개)보다 많다(%d개) — 그대로 싣는다",
                info.field_name,
                guide_items,
                len(value),
            )
        return value

    def to_domain(self) -> Narrative:
        """도메인 dataclass로 옮긴다. 여기가 pydantic이 끝나는 경계다."""
        return Narrative(
            headline=self.headline,
            context=tuple(self.context),
            findings=tuple(
                DomainFinding(
                    severity=item.severity,
                    title=item.title,
                    evidence=tuple(item.evidence),
                )
                for item in self.findings
            ),
            root_cause=self.root_cause,
            supporting=tuple(self.supporting),
            contradicting=tuple(self.contradicting),
            unverified=tuple(self.unverified),
            suspect_picks=tuple(
                DomainSuspectPick(candidate_id=item.candidate_id, reason=item.reason)
                for item in self.suspect_picks
            ),
            recommendations=tuple(self.recommendations),
        )
