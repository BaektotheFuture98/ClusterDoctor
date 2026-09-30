"""Main Agent와 Analysis SubAgent가 주고받는 DeepAgents 내부 계약.

``LogAnalysisRequest``/``LogAnalysisResponse``는 한 번의 SubAgent 위임에서
사용하는 어댑터 전용 메시지다. 구체적인 분석 구간과 목표, 이전 window의
리포트를 전달한다. 반면 ``incident_lifecycle``의 ``StartIncident``는 intake가
Incident 진단 유스케이스를 시작할 때 쓰는 애플리케이션 명령으로, Incident와
관측 구간, settling 대기 시간을 담는다. 개별 DeepAgents 위임이나 승인된 분석
목표를 표현하지 않는다.

자유 형식 dict가 아니라 타입으로 두는 이유는 경계가 프로세스 안에 있기
때문이다. 프로세스 밖 경계는 틀리면 직렬화가 막아 주지만, 안쪽 경계는 dict로
두면 오타가 조용한 ``None``이 되고 그 ``None``이 리포트까지 흘러간다.

``LogAnalysisResponse``는 Main Agent의 ``structured_response``로 실제로
Context에 실리는 값이다. 구간별 계획 데이터(``suggested_windows`` 등)까지
담아 ``MainAgentState``를 갱신하는 값은 ``WindowAnalysisResult``이며, Main Agent
모델은 그 값을 보지 않는다.
"""

from __future__ import annotations

from enum import StrEnum
from dataclasses import dataclass, field

from pydantic import BaseModel, ConfigDict

from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.observations import Observations


class LogAnalysisRequest(BaseModel):
    """Main Agent → SubAgent.

    ``analysis_goal``이 필드인 것이 중요하다. "왜 이 구간을 보는가"가 없으면
    SubAgent는 매번 같은 폭으로 훑고, Main Agent가 좁힌 판단이 전달되지
    않는다. 짝인 ``LogAnalysisResponse``(SubAgent → Main Agent)와 달리 이쪽은
    위임 시작 시 한 번 만들어져
    ``AnalysisAgentState.request``에 그대로 보관된다.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    incident_id: str
    cluster: str
    analysis_window: TimeRange
    # 같은 Incident의 앞선 window 리포트 객체. 참조 문자열이 아니라 객체를
    # 직접 담는 이유: MainAgentState가 리포트 객체를 직접 소유하고 있어 조회
    # 없이 그대로 넘길 수 있다.
    prior_report: LogAnalysisReport | None = None
    analysis_goal: str = ""
    evidence_sequence_start: int = 0
    prior_candidate_ids: tuple[str, ...] = ()


class AnalysisStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


class LogAnalysisResponse(BaseModel):
    """SubAgent → Main Agent. Main Agent가 ``structured_response``로 실제로 읽는 좁은 계약.

    한 TimeRange의 검증 완료 여부와 상태만 노출한다. ``analysis_summary``는
    코드가 리포트에서 뽑은 한두 문단이며, Main Agent가 충분성을 판단하는 유일한
    글이다.

    ``WindowAnalysisResult``(코드 간 전체 결과 계약)와 다르다 — 이쪽만 모델
    Context에 실린다. 계획용 필드(``suggested_windows``, ``unresolved_gaps``,
    ``gaps``)는 여기 없다 — 그 값은 코드가 ``MainAgentState``를 갱신하는 데만
    쓰이고, 모델에게 실으면 위임마다 토큰만 늘고 얻는 것이 없다.
    """

    model_config = ConfigDict(frozen=True)

    status: AnalysisStatus
    # 이 위임이 리포트를 남겼는가. 참조 문자열이 아니라 bool인 이유:
    # Main Agent 모델은 참조를 조회할 수단이 없고, 실제로 필요한 것도
    # "리포트가 있는가/없는가" 하나뿐이다.
    has_report: bool
    verification_status: VerificationStatus
    failure_reason: str | None = None
    analysis_summary: str = ""


@dataclass(frozen=True)
class WindowAnalysisResult:
    """Final immutable projection of one AnalysisAgentState."""

    window: TimeRange
    status: LogAnalysisStatus
    verification_status: VerificationStatus
    report: LogAnalysisReport | None = None
    evidence: tuple[Evidence, ...] = ()
    observations: Observations = field(default_factory=Observations)
    suggested_windows: tuple[TimeRange, ...] = ()
    unresolved_gaps: tuple[TimeRange, ...] = ()
    gaps: tuple[str, ...] = ()
    evidence_sequence: int = 0
    analysis_summary: str = ""
