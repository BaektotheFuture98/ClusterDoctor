"""Hierarchical Map-Reduce 로그 선별 그래프.

    ├── Send ──> map_minute (분 1)
    ├── Send ──> map_minute (분 2)      팬아웃 (버킷은 호출자가 미리 구성)
    └── Send ──> map_minute (분 N)
                     │
                     ↓  minute_results 누적 (operator.add)
                  reduce
                     ↓
               Meaningful Evidence[]

datasource마다 그래프를 새로 짜지 않는다. 절차는 같고 판단 기준만 다르므로
``AnalysisSpec``을 주입한다.
"""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.nodes import (
    EvidenceIdFactory,
    StructuredLlmCaller,
    make_map_minute,
    make_reduce_to_evidence,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.spec import AnalysisSpec
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    AnalysisResult,
    MinuteBucket,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.state import (
    MinuteAnalysisState,
)

_MAP = "map_minute"
_REDUCE = "reduce"

# 그래프 작업 동시 실행 수. 실제 LLM 요청은 공통 transport의 15개 제한을 공유한다.
MAX_CONCURRENCY = 15


def run_analysis(
    spec: AnalysisSpec,
    buckets: list[MinuteBucket],
    call_llm: StructuredLlmCaller,
    *,
    new_evidence_id: EvidenceIdFactory,
) -> AnalysisResult:
    """한 datasource의 분 단위 선별을 끝까지 돌린다.

    그래프를 실행마다 새로 컴파일한다. reduce 노드가 Incident별 id 발급기를
    포획해야 하는데, 컴파일된 그래프에 노드를 갈아 끼우는 공개 API가 없다.
    컴파일 비용은 그래프가 노드 둘짜리라 무시할 수 있다 — 실행당 LLM 호출
    수십 번에 비하면 없는 것과 같다.
    """
    if not buckets:
        return AnalysisResult(evidence=[])

    builder = StateGraph(MinuteAnalysisState)
    builder.add_node(_MAP, make_map_minute(spec, call_llm))
    builder.add_node(
        _REDUCE,
        make_reduce_to_evidence(spec, call_llm, new_evidence_id=new_evidence_id),
    )

    def dispatch(state: MinuteAnalysisState) -> list:
        if not state["buckets"]:
            return [_REDUCE]
        return [Send(_MAP, bucket) for bucket in state["buckets"]]

    builder.add_conditional_edges(START, dispatch, [_MAP, _REDUCE])
    builder.add_edge(_MAP, _REDUCE)
    builder.add_edge(_REDUCE, END)

    final = builder.compile().invoke(
        {
            "buckets": buckets,
            "minute_results": [],
            "evidence": [],
            "reduce_degraded": False,
        },
        config={"max_concurrency": MAX_CONCURRENCY},
    )

    results = final["minute_results"]
    return AnalysisResult(
        evidence=final["evidence"],
        analyzed_minutes=len(results),
        failed_minutes=sum(1 for item in results if item.failed),
        failed_minutes_at=tuple(sorted(item.minute for item in results if item.failed)),
        reduce_degraded=bool(final["reduce_degraded"]),
    )
