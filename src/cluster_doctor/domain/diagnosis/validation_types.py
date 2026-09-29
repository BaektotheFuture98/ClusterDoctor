"""Raw Grounding 검증이 남기는 불일치의 분류.

분류가 필요한 이유는 처방이 다르기 때문이다. 분석이 틀린 것은 다시 분석해야
하고, 표현만 틀린 것은 리포트만 고치면 된다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from cluster_doctor.domain.diagnosis.time_range import TimeRange


class MismatchKind(StrEnum):
    # 구간 분석이 원문과 어긋난다. 같은 구간을 다시 분석한다.
    ANALYSIS_MISMATCH = "analysis_mismatch"
    # 분석은 맞고 리포트의 표현이 과하거나 틀렸다. 리포트를 수정한다.
    REPORT_MISMATCH = "report_mismatch"
    # 원문이 없는 등 대조할 수 없다.
    UNVERIFIABLE = "unverifiable"


@dataclass
class ValidationIssue:
    kind: MismatchKind
    description: str
    affected_window: TimeRange | None
    affected_evidence_refs: list[str] = field(default_factory=list)
