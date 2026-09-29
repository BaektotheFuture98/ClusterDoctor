"""Raw Grounding 검증이 남기는 불일치의 분류.

분류가 필요한 이유는 처방이 다르기 때문이다. 분석이 틀린 것은 다시 분석해야
하고, 표현만 틀린 것은 리포트만 고치면 된다. ``ValidationResult``
(``report_validation.py``, 결정적 검사 8개 전용)의 지적과 다르다 — 이쪽만
재분석 여부를 가른다. 그 8개 검사는 처방이 항상 "리포트를 고쳐라" 하나뿐이라
분류가 필요 없다.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from cluster_doctor.domain.analysis.time_range import TimeRange


class VerificationIssueType(StrEnum):
    # 구간 분석이 원문과 어긋난다. 같은 구간을 다시 분석한다.
    ANALYSIS_MISMATCH = "analysis_mismatch"
    # 분석은 맞고 리포트의 표현이 과하거나 틀렸다. 리포트를 수정한다.
    REPORT_MISMATCH = "report_mismatch"
    # 원문이 없는 등 대조할 수 없다.
    UNVERIFIABLE = "unverifiable"


class VerificationIssue(BaseModel):
    """``GroundingValidator``가 Claim 하나를 원문과 대조해 낸 불일치 하나.

    ``issue_type``이 ``subagent._run_validation_loop``의 분기(재분석 대
    리포트 수정)를 결정한다. ``affected_range``는 지금 어떤 생성자도 채우지
    않는 예비 필드다(항상 None) — 구간 단위로 재분석을 좁히게 되면 그때
    채운다.
    """

    model_config = ConfigDict(frozen=True)

    issue_type: VerificationIssueType
    reason: str
    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)
    affected_range: TimeRange | None = None
