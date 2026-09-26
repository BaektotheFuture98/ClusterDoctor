"""마스터 노드 로그 datasource.

클러스터 **차원**의 사건이 여기에만 남는다 — 샤드 재배치, 노드 이탈, 리더 선출,
allocation 실패. 조회 조건이 레벨이 아니라 로거로 좁혀져 있는 이유가 그것이다:
ES는 그 사건들을 INFO로 남긴다.

여기서 나온 Evidence가 Node Investigation의 입구다. 문제 노드 후보가 나오는
곳이 이 소스뿐이므로, 노드 이름이 실린 줄을 남기는 것이 특히 중요하다.
"""

from __future__ import annotations

from cluster_doctor.domain.diagnosis.evidence import EvidenceSource
from cluster_doctor.domain.diagnosis.log_entries import NodeLogEntry
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.log_format import format_log_line
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.minute_analysis.spec import AnalysisSpec
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.minute_analysis.state import RawRecord

# 조회 조건. 레벨만으로는 안 된다 — 실측(packetbeat.loki_logs)에서 INFO 10건 중
# 진단에 필요한 것은 AllocationService 1건이었고 나머지 9건은 ML 유지보수·만료
# 데이터 삭제·매핑 변경 잡음이었다. 그래서 레벨(WARN/ERROR)과 클러스터 사건
# 로거를 OR로 묶는다. 커버리지는 오르고 볼륨은 내려간다.
MASTER_ROLE = "master"
MASTER_LOG_LEVELS = ("WARN", "ERROR")
MASTER_EVENT_LOGGERS = (
    "o.e.c.s.MasterService",                   # 클러스터 상태 변경, node-left/join
    "o.e.c.c.Coordinator",                     # 리더 선출, 마스터 이탈
    "o.e.c.c.NodeLeftExecutor",
    "o.e.c.c.NodeJoinExecutor",
    "o.e.c.r.a.AllocationService",             # 샤드 할당 (INFO로 기록된다)
    "o.e.c.r.a.DiskThresholdMonitor",          # 디스크 워터마크
    "o.e.c.r.a.d.DiskThresholdDecider",
    "o.e.m.j.JvmGcMonitorService",             # GC overhead
    "o.e.i.b.HierarchyCircuitBreakerService",  # circuit breaker
    "o.e.c.InternalClusterInfoService",
)
# 로거를 좁혔으므로 300은 과하다. 사고 중 비용 천장을 낮게 유지한다.
MASTER_LOG_MAX_LINES = 300

SPEC = AnalysisSpec(
    source=EvidenceSource.MASTER_LOG,
    label="master log (마스터 노드 ES 로그)",
    what_matters=(
        "- 노드 이탈/합류 (node-left, node-join, follower check 실패).\n"
        "- 리더 선출, 마스터 전환.\n"
        "- 샤드 할당 실패, 재배치 시작/종료, unassigned 발생.\n"
        "- GC overhead, circuit breaker 발동, 디스크 워터마크 초과.\n"
        "- 노드 이름이 실린 줄은 특히 중요하다. 이후 그 노드를 직접 조사할지\n"
        "  판단하는 근거가 된다."
    ),
    what_is_noise=(
        "- ML 유지보수, 만료 데이터 삭제, 인덱스 라이프사이클의 정기 동작.\n"
        "- 매핑·설정 변경 같은 일상 운영 기록.\n"
        "- 같은 사건이 노드별로 여러 줄 남은 경우 (대표 한 줄만 남긴다).\n"
        "  실측에서 24줄 중 20줄이 같은 follower_check 타임아웃이었고 대상\n"
        "  노드 이름만 달랐다."
    ),
)


def to_records(entries: list[NodeLogEntry]) -> list[RawRecord]:
    """ClickHouse에서 온 마스터 로그를 선별 레코드로."""
    ordered = sorted(entries, key=lambda entry: entry.timestamp)
    return [
        RawRecord(
            record_id=index,
            event_time=entry.timestamp,
            line=format_log_line(entry),
            node_name=entry.node or None,
            severity=(entry.level or entry.detected_level or "").strip() or None,
        )
        for index, entry in enumerate(ordered, start=1)
    ]
