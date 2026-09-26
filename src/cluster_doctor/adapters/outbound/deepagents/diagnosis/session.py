"""Diagnosis SubAgent의 공유 상태 타입.

``DiagnosisSeams``는 도구 셋과 응답 조립이 함께 쓰는 파이프라인 조각을
한 곳에 모은 불변 컨테이너다. ``_DiagnosisSession``은 위임 하나가 실제로 어디까지
갔는지 추적하는 가변 상태다. 둘을 한 모듈에 두는 이유는 도구가 두 타입을
동시에 참조하고, 분리하면 순환 참조가 생기기 때문이다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from cluster_doctor.application.ports.artifact_store import ArtifactStore
from cluster_doctor.application.ports.cluster_repository import (
    ClusterRepository,
    NodeResolver,
)
from cluster_doctor.application.ports.node_log_fetcher import NodeLogFetcher
from cluster_doctor.domain.diagnosis.log_entries import LogEntry, NodeLogEntry
from cluster_doctor.domain.diagnosis.evidence import Evidence
from cluster_doctor.domain.diagnosis.time_range import TimeRange
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport
from cluster_doctor.domain.diagnosis.contracts import LogAnalysisRequest
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.collector import CollectedEvidence
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.report_writer import ReportWriter
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.run_state import AnalysisRunState
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.schema import DraftReport
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.datasource.node_metric import (
    DEFAULT_THRESHOLDS,
    NodeMetricThresholds,
)


@dataclass(frozen=True)
class DiagnosisSeams:
    """도구 셋이 실제로 쓰는 파이프라인 조각 전부.

    이 SubAgent가 모델에게 주는 것은 "수집"과 "리포트 작성"으로 **나뉜**
    단계다. 어느 쪽도 포트 하나로 표현되지 않는다 — 수집기는 datasource
    의존성 여덟 개를 받아야 하고, 리포트 작성은 그 의존성을 쓰지 않는다.
    그래서 구체 타입을 고르는 일은 조립 지점에 남기고, 여기서는 받은 조각을
    그대로 쓴다.

    frozen인 이유는 위임 중에 바뀔 값이 하나도 없기 때문이다. 위임마다 달라지는
    것은 ``_DiagnosisSession``이고, 그쪽은 가변이다.
    """

    store: ArtifactStore
    fetch_logs: Callable[[TimeRange], list[LogEntry]]
    fetch_node_logs: Callable[..., list[NodeLogEntry]]
    cluster: ClusterRepository
    node_resolver: NodeResolver
    node_log_fetcher: NodeLogFetcher
    call_llm: Callable[..., str]
    report_writer: ReportWriter
    metric_thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS


class _DiagnosisSession:
    """위임 하나가 실제로 어디까지 갔는가.

    도구들이 공유하는 유일한 가변 상태다. 위임마다 새로 만든다 — Incident
    하나에 위임이 여럿이고, 앞 위임의 근거가 다음 위임에 남으면
    ``collect_evidence``의 멱등성이 위임 경계를 넘어 잘못 작동한다.
    """

    def __init__(self, window: TimeRange, request: LogAnalysisRequest) -> None:
        self.window = window
        self.request = request
        self.run_state = AnalysisRunState(window)
        self.collected: CollectedEvidence | None = None
        self.evidence: list[Evidence] = []
        self.report: LogAnalysisReport | None = None
        self.report_ref: str | None = None
        self.draft: DraftReport | None = None
        self.report_attempts = 0
        self.insufficient_reason = ""
        self.suggested: list[TimeRange] = []

    @property
    def collected_once(self) -> bool:
        return self.collected is not None
