"""Analysis SubAgent 도구 셋이 공유하는 불변 의존성 묶음.

``AnalysisSeams``는 도구 셋과 응답 조립이 함께 쓰는 파이프라인 조각을 한 곳에
모은 컨테이너다. 위임마다 달라지는 실행 상태(``AnalysisSession``,
``model/state/analysis_session.py``)와 분리된 이유는 저 상태는 위임마다
새로 만들지만 이 의존성들은 프로세스 전체에서 재사용되기 때문이다.

frozen인 이유는 위임 중에 바뀔 값이 하나도 없기 때문이다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import (
    DEFAULT_THRESHOLDS,
    NodeMetricThresholds,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.cluster_health import (
    ClusterRepository,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.node_resolver import (
    NodeResolver,
)
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import NodeLogFetcher
from cluster_doctor.incident_analysis_agent.model.basemodel.log_entries import LogEntry, NodeLogEntry
from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
)


@dataclass(frozen=True)
class AnalysisSeams:
    """도구 셋이 실제로 쓰는 파이프라인 조각 전부.

    이 SubAgent가 모델에게 주는 것은 "수집"과 "리포트 작성"으로 **나뉜**
    단계다. 어느 쪽도 포트 하나로 표현되지 않는다 — 수집기는 datasource
    의존성 여럿을 받아야 하고, 리포트 작성은 LLM 하나만 있으면 된다. 각
    소비자(``EvidenceCollector``, ``node_investigation``, ``ReportWriter``,
    ``GroundingValidator``)는 여기서 자기가 필요한 필드만 꺼내 쓴다.
    """

    fetch_logs: Callable[[TimeRange], list[LogEntry]]
    fetch_node_logs: Callable[..., list[NodeLogEntry]]
    cluster: ClusterRepository
    node_resolver: NodeResolver
    node_log_fetcher: NodeLogFetcher
    call_llm: Callable[..., str]
    report_writer: ReportWriter
    metric_thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS
