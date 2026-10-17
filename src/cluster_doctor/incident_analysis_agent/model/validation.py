"""근거 대조 결과의 분류와 상세 사유.

구간 분석의 finalize_update는 분석 불일치면 재수집·재작성하고,
표현 불일치면 리포트를 수정한다. 구조 검사의 ValidationResult와 구별한다.
사건 전체 종합은 누적된 근거로 리포트 수정만 시도한다.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange


class VerificationIssueType(StrEnum):
    # 구간 분석이 원문과 어긋난다. 같은 구간을 다시 분석한다.
    ANALYSIS_MISMATCH = "analysis_mismatch"
    # 분석은 맞고 리포트의 표현이 과하거나 틀렸다. 리포트를 수정한다.
    REPORT_MISMATCH = "report_mismatch"
    # 검증할 주장 부재·호출 실패·잘못된 응답 등으로 대조를 완료하지 못했다.
    UNVERIFIABLE = "unverifiable"


class VerificationIssue(BaseModel):
    """``GroundingValidator``가 남긴 불일치 또는 검증 미완료 사유.

    ``issue_type``은 구간 분석의 ``subagent.finalize_update``에서 재분석과
    리포트 수정을 구분한다. 현재 검증기는 ``affected_range``를 채우지 않는다.
    구간 분석에서 재분석이 필요하면 승인된 분석 구간 전체를 다시 조사한다.
    """

    model_config = ConfigDict(frozen=True)

    issue_type: VerificationIssueType
    reason: str
    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)
    affected_range: TimeRange | None = None
