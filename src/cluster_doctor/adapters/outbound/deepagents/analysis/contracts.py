"""Supervisor와 Log Analysis SubAgent가 주고받는 DeepAgents 내부 계약.

``LogAnalysisRequest``/``LogAnalysisResponse``는 한 번의 SubAgent 위임에서
사용하는 어댑터 전용 메시지다. 구체적인 분석 구간과 목표, Artifact 참조를
전달한다. 반면 ``application.commands.StartIncident``는 intake가 Incident
진단 유스케이스를 시작할 때 쓰는 애플리케이션 명령으로, Incident와 관측 구간,
settling 대기 시간을 담는다. 개별 DeepAgents 위임이나 승인된 분석 목표를
표현하지 않는다.

자유 형식 dict가 아니라 타입으로 두는 이유는 경계가 프로세스 안에 있기
때문이다. 프로세스 밖 경계는 틀리면 직렬화가 막아 주지만, 안쪽 경계는 dict로
두면 오타가 조용한 ``None``이 되고 그 ``None``이 리포트까지 흘러간다.

**Request는 무엇을 볼지만 담고, Response는 무엇을 봤는지만 담는다.** 원문도
중간 결과도 여기 없다. 둘 사이를 오가는 것은 참조(``state_ref``,
``report_ref``)뿐이고, 실체는 ``ArtifactStore``가 갖는다 — Supervisor의
Context에 raw 로그가 쌓이지 않게 하는 장치가 이 타입이다.

``LogAnalysisResponse``는 Main Agent의 ``structured_response``로 실제로
Context에 실리는 값이다. 구간별 계획 데이터(``suggested_windows`` 등)까지
담아 domain ``IncidentState``를 갱신하는 내부 전용 값은 이 파일에 없다 —
``subagent.py``의 private ``_WindowOutcome``이 그 역할이며, Main Agent
모델은 그 값을 보지 않는다.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from cluster_doctor.domain.analysis.report import VerificationStatus
from cluster_doctor.domain.analysis.time_range import TimeRange


class LogAnalysisRequest(BaseModel):
    """Supervisor → SubAgent.

    ``analysis_goal``이 필드인 것이 중요하다. "왜 이 구간을 보는가"가 없으면
    SubAgent는 매번 같은 폭으로 훑고, Supervisor가 Scope를 좁힌 판단이 전달되지
    않는다. 짝인 ``LogAnalysisResponse``(SubAgent → Supervisor)와 달리 이쪽은
    대응하는 상태 필드가 없다 — 위임 시작 시 ``_run`` 안에서 한 번 만들어져
    ``_AnalysisSession.request``에 그대로 보관된다.
    """

    model_config = ConfigDict(frozen=True)

    incident_id: str
    cluster: str
    analysis_window: TimeRange
    # 같은 Incident의 앞선 Evidence·Report를 꺼낼 참조. 첫 호출에서는 None.
    state_ref: str | None = None
    analysis_goal: str = ""


class AnalysisStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


class LogAnalysisResponse(BaseModel):
    """SubAgent → Supervisor. Main Agent가 ``structured_response``로 실제로 읽는 좁은 계약.

    한 TimeRange의 검증 완료 Report 참조와 상태만 노출한다. ``analysis_summary``는
    코드가 리포트에서 뽑은 한두 문단이며, Main Agent가 충분성을 판단하는 유일한
    글이다.

    ``_WindowOutcome``(``subagent.py`` 내부 전용)과 다르다 — 이쪽만 모델
    Context에 실린다. 계획용 필드(``suggested_windows``, ``unresolved_gaps``,
    ``gaps``)는 여기 없다 — 그 값은 코드가 domain ``IncidentState``를 갱신하는
    데만 쓰이고, 모델에게 실으면 위임마다 토큰만 늘고 얻는 것이 없다.
    """

    model_config = ConfigDict(frozen=True)

    status: AnalysisStatus
    report_ref: str | None
    verification_status: VerificationStatus
    failure_reason: str | None = None
    analysis_summary: str = ""
