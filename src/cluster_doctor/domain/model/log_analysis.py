"""Supervisor와 Log Analysis SubAgent가 주고받는 계약.

자유 형식 dict가 아니라 타입으로 두는 이유는 경계가 프로세스 안에 있기
때문이다. 프로세스 밖 경계는 틀리면 직렬화가 막아 주지만, 안쪽 경계는 dict로
두면 오타가 조용한 ``None``이 되고 그 ``None``이 리포트까지 흘러간다.

**Request는 무엇을 볼지만 담고, Response는 무엇을 봤는지만 담는다.** 원문도
중간 결과도 여기 없다. 둘 사이를 오가는 것은 참조(``state_ref``,
``report_ref``)뿐이고, 실체는 ``ArtifactStore``가 갖는다 — Supervisor의
Context에 raw 로그가 쌓이지 않게 하는 장치가 이 타입이다.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from cluster_doctor.domain.model.time_range import TimeRange


class LogAnalysisStatus(StrEnum):
    """SubAgent가 한 번의 분석을 어떻게 끝냈는가."""

    COMPLETED = "COMPLETED"
    # 현재 요청 범위 밖의 시간이 필요하다. Scope 확장은 Supervisor가 승인한다.
    NEED_MORE_CONTEXT = "NEED_MORE_CONTEXT"
    # 허용된 revision을 다 쓰고도 Evidence와 Report가 맞지 않았다.
    VALIDATION_FAILED = "VALIDATION_FAILED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class VerificationStatus(StrEnum):
    """Report Consistency Validator의 판정."""

    PASSED = "PASSED"
    # 검증은 돌았고 불일치가 남았다. 리포트는 존재한다.
    MISMATCH = "MISMATCH"
    # 검증을 돌리지 못했다. 통과와 구별해야 한다.
    NOT_VERIFIED = "NOT_VERIFIED"


class LogAnalysisRequest(BaseModel):
    """Supervisor → SubAgent.

    ``analysis_goal``이 필드인 것이 중요하다. "왜 이 구간을 보는가"가 없으면
    SubAgent는 매번 같은 폭으로 훑고, Supervisor가 Scope를 좁힌 판단이 전달되지
    않는다.
    """

    model_config = ConfigDict(frozen=True)

    incident_id: str
    cluster: str
    analysis_window: TimeRange
    # 같은 Incident의 앞선 Evidence·Report를 꺼낼 참조. 첫 호출에서는 None.
    state_ref: str | None = None
    analysis_goal: str = ""


class LogAnalysisResponse(BaseModel):
    """SubAgent → Supervisor.

    ``suggested_windows``는 **제안**이다. Supervisor가 ``analyzed_windows``와
    비교해 실제로 새로 필요한 부분만 남긴다(``window_planner``). 여기서 확정하면
    SubAgent가 Scope를 쥐게 되고, 그것은 이 설계가 나눈 책임 경계를 되돌린다.
    """

    model_config = ConfigDict(frozen=True)

    status: LogAnalysisStatus

    analyzed_window: TimeRange

    suggested_windows: tuple[TimeRange, ...] = Field(default_factory=tuple)
    unresolved_gaps: tuple[TimeRange, ...] = Field(default_factory=tuple)

    report_ref: str | None = None
    verification_status: VerificationStatus = VerificationStatus.NOT_VERIFIED

    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)

    # 확보하지 못한 보조 근거를 사람이 읽을 문장으로. ``unresolved_gaps``와
    # 나누는 이유는 쓰임이 다르기 때문이다 — 저쪽은 Supervisor가 다음 범위를
    # 계산하는 값이고, 이쪽은 notifier가 배너로 그려 운영자에게 닿는 값이다.
    # 이 저장소는 빠진 사실이 반드시 운영자에게 도달하게 한다.
    gaps: tuple[str, ...] = Field(default_factory=tuple)

    # Supervisor가 읽을 한두 문단. 리포트 전문이 아니다 — 전문은 report_ref로
    # 꺼낸다.
    analysis_summary: str = ""
