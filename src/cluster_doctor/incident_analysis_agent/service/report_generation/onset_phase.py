"""근거 시각을 사건의 최초 관측 분에 견준 표시로 바꾼다.

계산은 코드가 하고, 그 표시를 어떻게 읽을지는 모델이 정한다.
"""

from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.kst import KST

BEFORE = "before"
ONSET = "onset"
AFTER = "after"


def onset_phase(at: datetime, onset: datetime) -> str:
    """``at``이 최초 관측 분보다 앞인지, 그 분 안인지, 뒤인지 돌려준다."""
    onset_minute = onset.astimezone(KST).replace(second=0, microsecond=0)
    moment = at.astimezone(KST)
    if moment < onset_minute:
        return BEFORE
    if moment < onset_minute + timedelta(minutes=1):
        return ONSET
    return AFTER
