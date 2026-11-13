"""보고서 개요에 싣는 incident 범위·마감 사유·세션 줄. HTML과 평문이 같이 쓴다."""

from __future__ import annotations

from datetime import datetime

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentClose,
    SessionContext,
    tracking_stopped,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import (
    stamp,
)


def scope_rows(
    *,
    observed_start: datetime,
    incident_end: datetime | None,
    close: IncidentClose,
    session: SessionContext | None,
) -> tuple[tuple[str, str], ...]:
    if incident_end is None:
        return ()
    rows = [
        (
            "incident 범위",
            # 마감 시각은 벽시계 now라 마이크로초까지 붙는다. 표시에는 초까지만 쓴다.
            f"{stamp(observed_start.replace(microsecond=0))} ~ "
            f"{stamp(incident_end.replace(microsecond=0))}",
        ),
        ("마감 사유", close.value),
    ]
    if session is not None:
        previous = (
            f" · 이전 incident {session.previous_incident_id}"
            if session.previous_incident_id
            else ""
        )
        rows.append(("세션", f"{session.session_id} · {session.sequence}번째{previous}"))
        # 두 번째 incident부터 개요의 "발생 및 인지 일시"는 이번 범위의 시작이다.
        # 장애가 처음 관측된 시각을 따로 보여 준다.
        if session.sequence > 1 and session.onset is not None:
            rows.append(("장애 최초 관측", stamp(session.onset.replace(microsecond=0))))
    if tracking_stopped(close, session):
        rows.append(("추적", "장시간 미복구, 자동 추적 중단"))
    return tuple(rows)
