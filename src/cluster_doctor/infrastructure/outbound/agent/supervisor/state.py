"""Main DeepAgent와 Diagnosis SubAgent 사이를 흐르는 state 계약.

``task`` 도구는 타입을 넘기지 못한다. 스키마가 ``{description: str,
subagent_type: str}``로 고정되어 있고, description은 모델이 쓴 자유 텍스트다.
구조화된 값이 SubAgent에 닿는 통로는 graph state 하나뿐이다 —
``deepagents.middleware.subagents``가 부모 state에서 ``messages``/``todos``/
``structured_response``만 빼고 그대로 넘겨준다.

**그래서 분석 구간은 description이 아니라 여기서 읽는다.** 모델이 쓴 문장에서
시각을 파싱하면 Guardrail이 우회된다 — 승인받지 않은 구간을 문장에 적는 것을
막을 방법이 없기 때문이다. ``propose_analysis`` 도구가 Guardrail을 통과시킨
구간만 ``ADMITTED_WINDOW``에 쓰고, SubAgent는 그것만 본다.

SubAgent는 끝나면서 ``ADMITTED_WINDOW``를 ``None``으로 되돌린다. 승인 하나에
위임 하나라는 뜻이고, 이것이 예산 회계가 어긋나지 않는 이유다.
"""

from __future__ import annotations

from typing import Annotated, Any, NotRequired

from deepagents import DeepAgentState

# state 키 이름. 문자열을 각자 박으면 한쪽만 바뀌어도 아무도 모른다.
INCIDENT_ID = "incident_id"
CLUSTER = "cluster"
ADMITTED_WINDOW = "admitted_window"
ADMITTED_GOAL = "admitted_goal"
LAST_RESPONSE = "last_response"

# SubAgent 이름. ``task(subagent_type=...)``이 이 값을 받는다.
DIAGNOSIS_SUBAGENT = "diagnosis"


def _last_write_wins(_current: Any, incoming: Any) -> Any:
    """뒤에 쓴 값이 이긴다.

    ``None``도 값이다 — SubAgent가 승인을 소모했다고 알리는 방법이 그것이라,
    기본 reducer처럼 ``None``을 무시하면 승인이 영원히 남아 위임이 반복된다.
    """
    return incoming


class IncidentAgentState(DeepAgentState):
    """Main DeepAgent의 state.

    ``DeepAgentState``를 상속하는 이유는 ``task`` 도구가 ``messages``를
    요구하기 때문이다(``CompiledSubAgent``는 반드시 ``messages``를 돌려줘야
    한다).
    """

    incident_id: NotRequired[str]
    cluster: NotRequired[str]

    # 승인된 구간. ``{"start": "<iso>", "end": "<iso>"}`` 또는 ``None``.
    # TimeRange를 그대로 넣지 않는 이유: frozen dataclass라 langgraph의
    # state 직렬화 경로에서 되살아나지 않고, >10분이면 __post_init__이
    # 예외를 던져 도구 인자 검증 안에서 터진다.
    admitted_window: NotRequired[Annotated[dict[str, str] | None, _last_write_wins]]
    admitted_goal: NotRequired[Annotated[str, _last_write_wins]]

    # 직전 분석 결과 요약. 참조와 요약 문자열만 담는다 — 원문 로그는 절대
    # 담지 않는다. 담으면 ArtifactStore 간접참조가 통째로 무의미해진다.
    last_response: NotRequired[Annotated[dict[str, Any] | None, _last_write_wins]]
