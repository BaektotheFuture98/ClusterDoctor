"""StateGraph 조립.

    split_by_minute
         │
         ├── Send ──> analyze_minute (구간 1)
         ├── Send ──> analyze_minute (구간 2)   팬아웃, 최대 10
         └── Send ──> analyze_minute (구간 N)
                          │
                          ↓  findings 누적 (state의 operator.add)
                     synthesize
                          ↓
                        report
"""

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from cluster_doctor.infrastructure.outbound.llm.langgraph.nodes import (
    LlmCaller,
    make_analyze_minute,
    make_synthesize,
    split_by_minute,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.state import GraphState

_ANALYZE_MINUTE = "analyze_minute"
_SPLIT = "split_by_minute"
_SYNTHESIZE = "synthesize"


def _fan_out(state: GraphState) -> list:
    """구간마다 analyze_minute 인스턴스를 하나씩 띄운다.

    ``Send``는 노드에 상태 전체가 아니라 지정한 값만 넘긴다. 각 인스턴스가
    자기 구간의 로그만 보게 되므로, 노드 안에서 "내가 몇 번째인가"를 따질
    필요가 없다.

    버킷이 없으면(로그가 전혀 없는 시간 범위) synthesize로 바로 보낸다.
    빈 리스트를 돌려주면 스케줄될 노드가 없어 그래프가 report 없이 끝나고,
    어댑터가 KeyError를 맞는다.
    """
    if not state["buckets"]:
        return [_SYNTHESIZE]
    return [Send(_ANALYZE_MINUTE, bucket) for bucket in state["buckets"]]


def build_graph(call_llm: LlmCaller, call_llm_minute: LlmCaller | None = None):
    """컴파일된 그래프를 돌려준다.

    ``call_llm``은 provider/model/api_key가 이미 묶인 호출자다. 그래프는
    누구에게 묻는지 모른다 — 그 결정은 조립 시점(``analyzer.py``)에 끝난다.

    ``call_llm_minute``이 주어지면 분 단위 분석에 structured output(JSON)을
    사용한다. 없으면 ``call_llm``으로 텍스트 파싱 방식으로 동작한다.
    """
    minute_caller = call_llm_minute if call_llm_minute is not None else call_llm
    structured = call_llm_minute is not None
    builder = StateGraph(GraphState)
    builder.add_node(_SPLIT, split_by_minute)
    builder.add_node(_ANALYZE_MINUTE, make_analyze_minute(minute_caller, structured=structured))
    builder.add_node(_SYNTHESIZE, make_synthesize(call_llm))

    builder.add_edge(START, _SPLIT)
    builder.add_conditional_edges(
        _SPLIT, _fan_out, [_ANALYZE_MINUTE, _SYNTHESIZE]
    )
    builder.add_edge(_ANALYZE_MINUTE, _SYNTHESIZE)
    builder.add_edge(_SYNTHESIZE, END)

    return builder.compile()
