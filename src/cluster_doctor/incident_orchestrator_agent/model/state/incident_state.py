"""Incident 하나가 끝날 때까지 유지되는 업무 상태.

**Agent conversation memory가 아니다.** Main Agent의 Context는 사이클마다
버려도 되지만 이 값은 남아야 한다. 그래서 여기 담는 것은 "다음 사이클에서 다시
판단하려면 반드시 필요한 것"으로 한정한다.

담지 않는 것:
  raw log / minute map 중간 결과 / ToolMessage / SSH 원문 / 이전 대화.
그런 원문은 여기 담기지 않지만, window별 최종 Evidence·Report·Observations는
이 객체가 직접 소유한다 — 참조로 우회할 별도 저장 계층을 두지 않는다. Main
Agent Context에 실리는 것은 ``MainAgentState``뿐이고, 이 객체는 그 계층을
거치지 않는 순수 Python 실행 상태다.

``analyzed_windows``가 리스트인 것은 시간 순서가 아니라 **집합**으로 쓰기
때문이다. 중복 판정과 차집합 산수는 ``time_range.subtract_spans``가 한다.

Incident마다 이 객체 하나가 만들어져(``AnalyzeIncident.handle``) Main Agent
Tool과 Analysis SubAgent에 **같은 참조**로 공유된다. 별도 저장소를 거치지
않으므로 조회·저장 메서드가 없다 — mutation이 곧 반영이다. Incident가 끝나면
참조를 더 이상 아무도 들고 있지 않게 되어 GC가 정리한다.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.basemodel.observations import Observations
from cluster_doctor.incident_analysis_agent.model.basemodel.report import (
    LogAnalysisReport,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import TimeRange, is_covered
from cluster_doctor.incident_orchestrator_agent.model.basemodel.incident import IncidentStatus


class WindowResult(BaseModel):
    """한 window의 검증 완료 리포트.

    참조 문자열이 아니라 리포트 객체 자체를 담는다 — 별도 저장소가 없으므로
    담을 곳이 이 객체뿐이다.
    """

    model_config = ConfigDict(frozen=True)

    window: TimeRange
    report: LogAnalysisReport


class IncidentState(BaseModel):
    """Incident 전체의 진행, 예산, 분석 구간과 산출물을 보관한다.

    시작 맥락인 ``Incident``와 달리 실행 중 갱신되며, Agent 대화 상태와도
    별개다. ``MainAgentState``(LangGraph 실행 상태, 프로세스 메모리에만
    존재하며 Agent 실행이 끝나면 사라진다)와 달리 Incident 전체의 수명 동안
    살아 있다 — 리포트 전달과 예산 조회가 이 값을 읽는다.
    """

    incident_id: str

    analyzed_windows: list[TimeRange] = Field(default_factory=list)
    pending_windows: list[TimeRange] = Field(default_factory=list)
    unresolved_gaps: list[TimeRange] = Field(default_factory=list)

    # window별 검증 완료 리포트. 위임이 끝날 때마다 순서대로 쌓이고,
    # window마다 따로 보관한다(합치지 않는다).
    window_results: list[WindowResult] = Field(default_factory=list)
    # Incident 전체에서 누적된 Evidence. 최종 리포트 전달이 읽는다.
    evidence: list[Evidence] = Field(default_factory=list)
    # Incident 전체에서 누적된, 코드가 관측한 사실. window마다
    # ``merge_observations``로 접힌다.
    observations: Observations = Field(default_factory=Observations)
    # Evidence id 발급 카운터. window를 넘나들며 단조 증가해야 한다 —
    # datasource별로 새로 시작하면 같은 id가 두 번 붙는다.
    evidence_counter: int = 0

    # 분석이 확보하지 못한 것을 사람이 읽을 문장으로. 리포트 배너가 이것만
    # 읽는다 — 사이클마다 쌓이므로 여기 두지 않으면 앞선 분석의 누락이
    # 사라지고, 리포트가 갖추지 못한 완결성을 주장하게 된다.
    accumulated_gaps: list[str] = Field(default_factory=list)

    latest_analysis_status: LogAnalysisStatus | None = None
    latest_verification_status: VerificationStatus | None = None

    analysis_call_count: int = 0
    # 지금까지 분석한 **분 수**. 예산의 단위다 — 비용 동인이 호출이 아니라
    # 분이기 때문이다(비어 있지 않은 분마다 datasource별로 LLM이 돈다).
    analyzed_minutes: int = 0
    # Guardrail이 거절한 횟수. 모델이 같은 요청을 반복하면 이 값이 오른다.
    rejected_decision_count: int = 0
    total_wait_seconds: float = 0.0

    status: IncidentStatus = IncidentStatus.OPEN
    # 종료 사유. FAILED/CANCELLED에서 운영자가 읽을 유일한 설명이다.
    closing_reason: str = ""

    def remaining_of(self, window: TimeRange) -> list[TimeRange]:
        """이 구간에서 아직 보지 않은 부분만."""
        from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import (
            subtract_spans,
        )

        return subtract_spans(window, self.analyzed_windows)

    def record_analyzed(self, window: TimeRange) -> None:
        """분석이 끝난 구간을 반영하고 대기 목록에서 지운다.

        ``pending_windows``에서 **덮인 것을 전부** 지운다. 완전히 같은 것만
        지우면, 13:50~14:05를 13:50~14:00으로 좁혀 분석했을 때 원래 제안이
        영원히 남아 종료 조건을 막는다.
        """
        self.analyzed_windows.append(window)
        self.pending_windows = [
            pending
            for pending in self.pending_windows
            if not is_covered(pending, self.analyzed_windows)
        ]
        self.unresolved_gaps = [
            gap for gap in self.unresolved_gaps if not is_covered(gap, self.analyzed_windows)
        ]

    def next_evidence_id(self) -> str:
        """다음 Evidence에 붙일 id를 발급한다.

        번호를 이 객체가 발급하는 이유는 한 Incident 안에서 번호가 이어져야
        하기 때문이다 — window/datasource마다 새로 세면 같은 id가 여러 번
        붙는다.
        """
        self.evidence_counter += 1
        return f"E-{self.incident_id}-{self.evidence_counter}"
