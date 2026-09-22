"""datasource workflow를 조율해 한 window의 Evidence를 모은다.

    fetch_logs (ClickHouse)          fetch_node_logs (ClickHouse)
        ├── slowlog   ─ Triage ─┐        └── master log ─ Triage ─┐
        ├── query log ─ Triage ─┤                 (실패 시 SSH 폴백)│
        └── node metric ─ 규칙 ─┤                                  │
                                ↓                                  ↓
                          Evidence[] ←───────────────────── Node Investigation
                                                            (후보가 있을 때만)

**어느 단계도 예외를 밖으로 내보내지 않는다.** 소스 하나가 실패해도 나머지
소스의 근거는 유효하고, 그 사실은 ``gaps``로 남아 리포트의 배너가 된다.
분석 자체가 성립하지 않은 경우(모든 소스 실패)만 ``degraded``를 세운다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from cluster_doctor.storage.artifact_store import ArtifactStore
from cluster_doctor.agent.integrations.elasticsearch.ports import (
    ClusterRepository,
    NodeResolver,
)
from cluster_doctor.agent.integrations.ssh.port import NodeLogFetcher
from cluster_doctor.application.service.guardrails import (
    MAX_EVIDENCE_TOTAL,
    clamp_evidence,
    truncate_raw,
)
from cluster_doctor.contracts.evidence import Evidence, EvidenceSource
from cluster_doctor.agent.integrations.clickhouse.models import (
    LogEntry,
    NodeLogEntry,
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.contracts.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.common.kst import KST
from cluster_doctor.infrastructure.outbound.agent.diagnosis import node_investigation
from cluster_doctor.infrastructure.outbound.agent.diagnosis.run_state import (
    AnalysisRunState,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.datasource import (
    master_log,
    node_log,
    node_metric,
    query_log,
    slowlog,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.datasource.node_metric import (
    DEFAULT_THRESHOLDS,
    NodeMetricThresholds,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.triage.graph import (
    TriageResult,
    run_triage,
)

_logger = logging.getLogger(__name__)


@dataclass
class CollectedEvidence:
    evidence: list[Evidence] = field(default_factory=list)
    master_evidence: list[Evidence] = field(default_factory=list)
    investigated_nodes: list[str] = field(default_factory=list)
    # 선별이 실패해 근거가 비어 있는 분. 호출자가 unresolved gap으로 올린다.
    failed_minutes: set = field(default_factory=set)


class EvidenceCollector:
    """한 analysis window에 대해 모든 datasource를 돌린다."""

    def __init__(
        self,
        *,
        incident_id: str,
        store: ArtifactStore,
        fetch_logs: Callable[[TimeRange], list[LogEntry]],
        fetch_node_logs: Callable[..., list[NodeLogEntry]],
        cluster: ClusterRepository,
        node_resolver: NodeResolver,
        node_log_fetcher: NodeLogFetcher,
        call_llm: Callable[..., str],
        metric_thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS,
    ) -> None:
        self._incident_id = incident_id
        self._store = store
        self._fetch_logs = fetch_logs
        self._fetch_node_logs = fetch_node_logs
        self._cluster = cluster
        self._node_resolver = node_resolver
        self._node_log_fetcher = node_log_fetcher
        self._call_llm = call_llm
        self._metric_thresholds = metric_thresholds
        # 선별이 실패한 분. ``collect``마다 비운다 — 같은 collector를 두 window에
        # 재사용하면 앞 window의 실패가 뒤 window의 gap으로 새어 나간다.
        self._failed_minutes: set = set()

    # ── 저장소 어댑터 ─────────────────────────────────────────────────
    def _new_evidence_id(self) -> str:
        return self._store.next_evidence_id(self._incident_id)

    def _put_raw(self, text: str) -> str:
        return self._store.put_raw(self._incident_id, text)

    # ── 수집 ─────────────────────────────────────────────────────────
    def collect(self, window: TimeRange, state: AnalysisRunState) -> CollectedEvidence:
        collected = CollectedEvidence()
        self._failed_minutes = set()

        collected.evidence.extend(self._collect_cluster_health(state))
        entries = self._fetch_window_logs(window, state)
        if entries:
            state.record_log_observations(entries)
            collected.evidence.extend(self._triage_slowlog(entries, state))
            collected.evidence.extend(self._triage_query_log(entries, state))
            collected.evidence.extend(self._node_metric_evidence(entries))

        master = self._collect_master(window, state)
        collected.master_evidence = master
        collected.evidence.extend(master)

        investigation = self._investigate_nodes(master, window, state)
        collected.evidence.extend(investigation.evidence)
        collected.investigated_nodes = [
            node.node_name or node.node_id for node in investigation.investigated
        ]

        collected.failed_minutes = self._failed_minutes
        collected.evidence = clamp_evidence(
            sorted(collected.evidence, key=lambda item: item.event_time),
            MAX_EVIDENCE_TOTAL,
            what="전체",
        )
        for item in collected.evidence:
            self._store.put_evidence(self._incident_id, item)

        _logger.info(
            "[collector] %s ~ %s → Evidence %d건 (노드 조사 %d대)",
            window.start.strftime("%H:%M"),
            window.end.strftime("%H:%M"),
            len(collected.evidence),
            len(collected.investigated_nodes),
        )
        return collected

    # ── 개별 소스 ────────────────────────────────────────────────────
    def _fetch_window_logs(
        self, window: TimeRange, state: AnalysisRunState
    ) -> list[LogEntry]:
        try:
            return self._fetch_logs(window)
        except Exception as exc:
            _logger.exception("[collector] 로그 조회 실패")
            # 주 소스가 통째로 실패한 것이다. 마스터 로그만으로도 리포트는
            # 만들 수 있으므로 중단하지는 않되, 분석이 성립하지 않았다고 본다.
            state.degraded = True
            state.mark_gap(f"slowlog/쿼리 로그/노드 메트릭 조회 실패: {exc}")
            return []

    def _triage_slowlog(
        self, entries: list[LogEntry], state: AnalysisRunState
    ) -> list[Evidence]:
        items = [entry for entry in entries if isinstance(entry, SlowlogEntry)]
        if not items:
            return []
        result = self._run_triage(slowlog.SPEC, slowlog.to_records(items), state)
        return result.evidence

    def _triage_query_log(
        self, entries: list[LogEntry], state: AnalysisRunState
    ) -> list[Evidence]:
        items = [entry for entry in entries if isinstance(entry, QueryLogEntry)]
        if not items:
            return []
        result = self._run_triage(query_log.SPEC, query_log.to_records(items), state)
        return result.evidence

    def _node_metric_evidence(self, entries: list[LogEntry]) -> list[Evidence]:
        items = [entry for entry in entries if isinstance(entry, NodeMetricEntry)]
        if not items:
            return []
        return node_metric.to_evidence(
            items,
            new_evidence_id=self._new_evidence_id,
            put_raw=self._put_raw,
            thresholds=self._metric_thresholds,
        )

    def _run_triage(self, spec, records, state: AnalysisRunState) -> TriageResult:
        """Triage 하나를 돌리고 실패를 gap으로 남긴다."""
        try:
            result = run_triage(
                spec,
                records,
                self._call_llm,
                new_evidence_id=self._new_evidence_id,
                put_raw=self._put_raw,
            )
        except Exception as exc:
            _logger.exception("[collector] %s triage 오류", spec.label)
            state.mark_gap(f"{spec.label} 선별이 오류로 중단됐다: {exc}")
            return TriageResult(evidence=[])

        self._failed_minutes.update(result.failed_minutes_at)
        if result.fully_failed:
            state.mark_gap(
                f"{spec.label}의 모든 분({result.analyzed_minutes}개) 선별이 실패했다."
            )
        elif result.failed_minutes:
            state.mark_gap(
                f"{spec.label} 중 {result.failed_minutes}개 분의 선별이 실패했다"
                f"(전체 {result.analyzed_minutes}개 분)."
            )
        if result.reduce_degraded:
            state.mark_gap(
                f"{spec.label}의 종합 선별이 실패해 분별 결과를 그대로 남겼다 — "
                "반복·정상 이벤트가 걸러지지 않았다."
            )
        return result

    def _collect_cluster_health(self, state: AnalysisRunState) -> list[Evidence]:
        """클러스터 상태를 관측값으로 남기고, green이 아니면 근거로도 만든다.

        이 값은 **실시간**이다. 과거 사고를 분석하면 분석을 돌린 시점의 상태이지
        사고 당시의 상태가 아니다. 그래서 Evidence의 ``message``에 조회 시점임을
        적는다 — 뒤 단계가 이것을 사고 당시의 상태로 읽으면 타임라인이 거짓이
        된다.
        """
        try:
            payload = self._cluster.health()
        except Exception as exc:
            state.mark_gap(f"클러스터 상태 조회 실패: {exc}")
            return []

        now = datetime.now(KST)
        try:
            state.record_health(payload, now=now)
        except Exception:
            # payload가 dict가 아니거나 숫자 칸에 문자열이 오면 int()가 터진다.
            # 상태 이력 한 줄 때문에 분석을 잃지 않는다.
            _logger.exception("[collector] 클러스터 상태 기록 실패")
            return []

        status = str(payload.get("status", "")).lower()
        if status not in ("yellow", "red"):
            return []

        message = (
            f"클러스터 상태가 {status.upper()}다 (분석 시점 {now:%Y-%m-%d %H:%M:%S} 조회. "
            f"분석 구간 당시의 값이 아니다). "
            f"unassigned_shards={payload.get('unassigned_shards', 0)} "
            f"active_shards={payload.get('active_shards', 0)} "
            f"nodes={payload.get('number_of_nodes', 0)}"
        )
        try:
            explained = self._cluster.explain_allocation()
        except Exception as exc:
            # 미할당 샤드가 없으면 ES가 400을 돌려준다. 실패가 아니다.
            _logger.info("[collector] allocation explain 없음/실패: %s", exc)
        else:
            message += f" | allocation explain: {truncate_raw(str(explained), 2000)}"

        return [
            Evidence(
                evidence_id=self._new_evidence_id(),
                event_time=now,
                source=EvidenceSource.CLUSTER_STATE,
                event_type="cluster_not_green",
                severity="Critical" if status == "red" else "Warning",
                message=message,
                raw_ref=self._put_raw(message),
                selection_reason="코드가 조회한 클러스터 상태. 모델을 거치지 않았다.",
            )
        ]

    def _collect_master(
        self, window: TimeRange, state: AnalysisRunState
    ) -> list[Evidence]:
        """마스터 로그를 모아 Triage한다. ClickHouse를 먼저, 실패하면 SSH.

        0건에서는 SSH로 내려가지 않는다. 로거를 좁혀 뒀으므로 건강한 창에서
        0건은 정상이고, 그때마다 내려가면 분석 호출마다 ES 왕복 + 새 SSH 접속을
        치른다. 대가는 적재 지연으로 0건인 경우를 메우지 못하는 것인데, 0건만
        보고는 "사건 없음"과 "적재 안 됨"을 구별할 수 없으므로 접속 비용이 더
        크다고 본다.
        """
        records = []
        try:
            entries = self._fetch_node_logs(
                window.start,
                window.end,
                node_role=master_log.MASTER_ROLE,
                levels=master_log.MASTER_LOG_LEVELS,
                loggers=master_log.MASTER_EVENT_LOGGERS,
                limit=master_log.MASTER_LOG_MAX_LINES,
            )
        except Exception as exc:
            _logger.warning("[collector] 마스터 로그 조회 실패, SSH로 폴백: %s", exc)
            text = self._master_via_ssh(window, state)
            if not text:
                return []
            state.record_master_text(text)
            records = node_log.to_records(
                truncate_raw(text), fallback_time=window.start
            )
        else:
            state.record_master_logs(entries)
            records = master_log.to_records(entries)

        if not records:
            return []
        return self._run_triage(master_log.SPEC, records, state).evidence

    def _master_via_ssh(self, window: TimeRange, state: AnalysisRunState) -> str:
        try:
            resolved = self._node_resolver.resolve("_master")
        except Exception as exc:
            state.mark_gap(f"마스터 노드 조회 실패: {exc}")
            return ""
        if resolved is None or not resolved.is_reachable():
            state.mark_gap("마스터 노드 접속 정보를 얻지 못해 마스터 로그를 보지 못했다")
            return ""
        try:
            return self._node_log_fetcher.fetch(
                resolved.host,
                resolved.log_path or "",
                resolved.cluster_name,
                start_dt=window.start,
                end_dt=window.end,
            )
        except Exception as exc:
            state.mark_gap(f"마스터 로그 SSH 수집 실패: {exc}")
            return ""

    def _investigate_nodes(
        self, master_evidence: list[Evidence], window: TimeRange, state: AnalysisRunState
    ) -> node_investigation.NodeInvestigationResult:
        candidates = node_investigation.find_problem_nodes(
            master_evidence, self._call_llm
        )
        result = node_investigation.investigate_nodes(
            candidates,
            window,
            resolver=self._node_resolver,
            fetcher=self._node_log_fetcher,
            call_llm=self._call_llm,
            new_evidence_id=self._new_evidence_id,
            put_raw=self._put_raw,
        )
        for gap in result.gaps:
            state.mark_gap(gap)
        return result
