"""Analysis SubAgent 내부 그래프의 state_schema."""

from __future__ import annotations

from deepagents import DeepAgentState


class AnalysisAgentState(DeepAgentState):
    """Analysis SubAgent 자신의 내부 DeepAgent 그래프 state_schema.

    ``MainAgentState``(``incident_orchestrator_agent/model/state``)와 별개다 —
    이 그래프는 위임 하나의 내부 루프(``collect_evidence`` →
    ``write_report`` → ``report_insufficient``)만 돌고, 도구는 이 state를
    읽지 않고 ``AnalysisSession``을 클로저로만 참조하므로 커스텀 필드가
    필요 없다. ``DeepAgentState``만 상속하는 이유는 deepagents 표준 에이전트
    루프가 요구하는 ``messages`` 채널을 갖추기 위해서다.
    """
