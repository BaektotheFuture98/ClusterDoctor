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

from dataclasses import dataclass

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from cluster_doctor.domain.diagnosis.evidence import Evidence
from cluster_doctor.agent.diagnosis.workflows.minute_analysis.nodes import (
    EvidenceIdFactory,
    RawRefFactory,
    StructuredLlmCaller,
    make_map_minute,
    make_reduce_to_evidence,
)
from cluster_doctor.agent.diagnosis.workflows.minute_analysis.spec import AnalysisSpec
from cluster_doctor.agent.diagnosis.workflows.minute_analysis.state import (
    AnalysisState,
    MinuteBucket,
)

_MAP = "map_minute"
_REDUCE = "reduce"

# 분별 호출의 동시 실행 수. 올리면 429가 빨라진다.
MAX_CONCURRENCY = 5


@dataclass(frozen=True)
class AnalysisResult:
    """한 datasource workflow의 산출물.

    Evidence만 돌려주지 않는 이유: 몇 분이 실패했는지를 호출자가 알아야 한다.
    실패한 분이 있는 리포트와 없는 리포트는 다른 것이고, 그 차이가 드러나지
    않으면 "그 시각에는 아무 일도 없었다"로 읽힌다.
    """

    evidence: list[Evidence]
    analyzed_minutes: int = 0
    failed_minutes: int = 0
    reduce_degraded: bool = False
    # 선별이 실패한 분의 시각. 호출자가 이것을 unresolved gap으로 올린다 —
    # 개수만으로는 "어느 시각을 못 봤는가"를 말할 수 없다.
    failed_minutes_at: tuple = ()

    @property
    def fully_failed(self) -> bool:
        return self.analyzed_minutes > 0 and self.failed_minutes == self.analyzed_minutes


def run_analysis(
    spec: AnalysisSpec,
    buckets: list[MinuteBucket],
    call_llm: StructuredLlmCaller,
    *,
    new_evidence_id: EvidenceIdFactory,
    put_raw: RawRefFactory,
) -> AnalysisResult:
    """한 datasource의 분 단위 선별을 끝까지 돌린다.

    그래프를 실행마다 새로 컴파일한다. reduce 노드가 Incident별 id 발급기를
    포획해야 하는데, 컴파일된 그래프에 노드를 갈아 끼우는 공개 API가 없다.
    컴파일 비용은 그래프가 노드 둘짜리라 무시할 수 있다 — 실행당 LLM 호출
    수십 번에 비하면 없는 것과 같다.
    """
    if not buckets:
        return AnalysisResult(evidence=[])

    builder = StateGraph(AnalysisState)
    builder.add_node(_MAP, make_map_minute(spec, call_llm))
    builder.add_node(
        _REDUCE,
        make_reduce_to_evidence(spec, call_llm, new_evidence_id=new_evidence_id, put_raw=put_raw),
    )

    def dispatch(state: AnalysisState) -> list:
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
