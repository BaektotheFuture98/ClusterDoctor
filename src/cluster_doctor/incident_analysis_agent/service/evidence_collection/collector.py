"""datasource workflow를 조율해 한 window의 Evidence를 모은다.

    fetch_logs (ClickHouse)          fetch_node_logs (ClickHouse)
        ├── slowlog   ─ 선별 ─┐        └── master log ─ 선별 ─┐
        ├── query log ─ 선별 ─┤                                  │
        └── node metric ─ 규칙 ─┤                                  │
                                ↓                                  ↓
                          Evidence[] ←───────────────────── Node Investigation
                                                            (후보가 있을 때만)

**어느 단계도 예외를 밖으로 내보내지 않는다.** 소스 하나가 실패해도 나머지
소스의 근거는 유효하고, 그 사실은 ``gaps``로 남아 리포트의 배너가 된다.
분 단위 세 소스의 모든 구간 조회가 실패한 경우 ``degraded``를 세운다.
클러스터 상태·마스터 로그·문제 노드 조사의 실패는 별도 gap으로 기록한다.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from cluster_doctor.incident_analysis_agent.datasource.clickhouse import (
    master_log,
    node_metric,
    query_log,
    slowlog,
)
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
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import (
    NodeLogFetcher,
)
from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import (
    NodeLogEntry,
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.incident_analysis_agent.model.log_fetch import LogFetchResult
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.observations import SourceWindowStatus
from cluster_doctor.incident_analysis_agent.service.evidence_collection.limits import (
    MAX_EVIDENCE_TOTAL,
    clamp_evidence,
    truncate_raw,
)
from cluster_doctor.incident_analysis_agent.service.node_investigation import (
    node_investigation,
)
from cluster_doctor.incident_analysis_agent.service.observation.builder import (
    ObservationBuilder,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.graph import (
    run_analysis,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    AnalysisResult,
    MinuteBucket,
    group_into_buckets,
)

_logger = logging.getLogger(__name__)


def _shift_ids(records: list, start: int) -> list:
    """record_id를 start부터 시작하도록 재번호 매긴다.

    분마다 to_records()를 호출하면 각 분이 1부터 시작한다. reduce 단계가
    모든 분의 레코드를 하나의 dict로 합치므로 분 경계를 넘어 유일해야 한다.
    """
    from dataclasses import replace

    return [replace(r, record_id=start + i) for i, r in enumerate(records)]


@dataclass
class CollectedEvidence:
    """한 구간의 수집 실행이 반환하는 근거 묶음과 조사 진행 정보.

    단일 Evidence와 달리 조사 노드·실패한 분을 함께 전달한다.
    """

    evidence: list[Evidence] = field(default_factory=list)
    investigated_nodes: list[str] = field(default_factory=list)
    # 선별이 실패해 근거가 비어 있는 분. 호출자가 unresolved gap으로 올린다.
    failed_minutes: set = field(default_factory=set)


class EvidenceCollector:
    """한 analysis window에 대해 모든 datasource를 돌린다."""

    def __init__(
        self,
        *,
        new_evidence_id: Callable[[], str],
        fetch_logs: Callable[[TimeRange], LogFetchResult],
        fetch_node_logs: Callable[..., list[NodeLogEntry]],
        cluster: ClusterRepository,
        node_resolver: NodeResolver,
        node_log_fetcher: NodeLogFetcher,
        call_llm: Callable[..., str],
        metric_thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS,
    ) -> None:
        self._new_evidence_id = new_evidence_id
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

    # ── 수집 ─────────────────────────────────────────────────────────
    def collect(
        self, window: TimeRange, state: ObservationBuilder
    ) -> CollectedEvidence:
        collected = CollectedEvidence()
        self._failed_minutes = set()

        collected.evidence.extend(self._collect_cluster_health(state))
        slowlog_buckets, query_log_buckets, metric_entries = self._fetch_and_bucket(
            window, state
        )
        collected.evidence.extend(
            self._run_analysis(slowlog.SPEC, slowlog_buckets, state).evidence
        )
        collected.evidence.extend(
            self._run_analysis(query_log.SPEC, query_log_buckets, state).evidence
        )
        collected.evidence.extend(self._node_metric_evidence(metric_entries))

        master = self._collect_master(window, state)
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

        _logger.info(
            "[collector] %s ~ %s → Evidence %d건 (노드 조사 %d대)",
            window.start.strftime("%H:%M"),
            window.end.strftime("%H:%M"),
            len(collected.evidence),
            len(collected.investigated_nodes),
        )
        return collected

    # ── 개별 소스 ────────────────────────────────────────────────────
    def _fetch_and_bucket(
        self, window: TimeRange, state: ObservationBuilder
    ) -> tuple[list[MinuteBucket], list[MinuteBucket], list[NodeMetricEntry]]:
        """분마다 세 소스를 병렬 조회하고 성공 레코드를 datasource별로 변환한다.

        실패는 소스·구간별 gap으로 기록한다. 쿼리 요청은 관측값에 보존하며,
        선별 입력은 분 단위 버킷으로 변환한다.

        반환값: (slowlog_buckets, query_log_buckets, metric_entries)
        """
        from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
            split_by_minute,
        )

        slowlog_buckets: list[MinuteBucket] = []
        query_log_buckets: list[MinuteBucket] = []
        metric_entries: list[NodeMetricEntry] = []
        slow_next_id = 1
        query_next_id = 1
        any_success = False

        for minute_range in split_by_minute(window):
            label = minute_range.start.strftime("%H:%M")
            try:
                result = self._fetch_logs(minute_range)
            except Exception as exc:
                _logger.warning("[collector] %s 로그 조회 실패: %s", label, exc)
                state.mark_gap(
                    f"slowlog/쿼리 로그/노드 메트릭 조회 실패 ({label}분): {exc}"
                )
                for source in ("slowlog", "es_query_log", "node_metric"):
                    state.record_source_status(SourceWindowStatus(source, minute_range.start,
                        minute_range.end, "failed", None, datetime.now(KST), str(exc)))
                continue

            # 정상 조회가 0건인 경우도 성공이다. 데이터 부재와 조회 실패를 구분한다.
            any_success |= len(result.failures) < 3
            for failure in result.failures:
                state.mark_gap(
                    f"{failure.source} 조회 실패 "
                    f"({failure.window.start.astimezone(KST).isoformat()} ~ "
                    f"{failure.window.end.astimezone(KST).isoformat()}): {failure.error}"
                )
            entries = list(result.entries)
            failures = {failure.source: failure for failure in result.failures}
            for source in ("slowlog", "es_query_log", "node_metric"):
                source_entries = [entry for entry in entries if entry.source == source]
                failure = failures.get(source)
                limited = any(entry.provenance and entry.provenance.excerpt for entry in source_entries)
                state.record_source_status(SourceWindowStatus(source, minute_range.start,
                    minute_range.end, "failed" if failure else ("limited" if limited else "ok"),
                    None if failure else len(source_entries), datetime.now(KST),
                    failure.error if failure else ""))
            state.record_log_observations(entries)
            minute = minute_range.start.replace(second=0, microsecond=0)

            slow_items = [e for e in entries if isinstance(e, SlowlogEntry)]
            if slow_items:
                records = _shift_ids(slowlog.to_records(slow_items), slow_next_id)
                slow_next_id += len(records)
                slowlog_buckets.append(MinuteBucket(minute=minute, records=records))

            query_items = [e for e in entries if isinstance(e, QueryLogEntry)]
            if query_items:
                records = _shift_ids(query_log.to_records(query_items), query_next_id)
                query_next_id += len(records)
                query_log_buckets.append(MinuteBucket(minute=minute, records=records))

            metric_entries.extend(e for e in entries if isinstance(e, NodeMetricEntry))

        if not any_success:
            state.degraded = True

        return slowlog_buckets, query_log_buckets, metric_entries

    def _node_metric_evidence(self, entries: list[NodeMetricEntry]) -> list[Evidence]:
        if not entries:
            return []
        return node_metric.to_evidence(
            entries,
            new_evidence_id=self._new_evidence_id,
            thresholds=self._metric_thresholds,
        )

    def _run_analysis(self, spec, buckets, state: ObservationBuilder) -> AnalysisResult:
        """분 단위 선별 하나를 돌리고 실패를 gap으로 남긴다."""
        try:
            result = run_analysis(
                spec,
                buckets,
                self._call_llm,
                new_evidence_id=self._new_evidence_id,
            )
        except Exception as exc:
            _logger.exception("[collector] %s 선별 오류", spec.label)
            state.mark_gap(f"{spec.label} 선별이 오류로 중단됐다: {exc}")
            return AnalysisResult(evidence=[])

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

    def _collect_cluster_health(self, state: ObservationBuilder) -> list[Evidence]:
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
        raw_payload = {"health": payload}
        endpoint = "/_cluster/health"
        try:
            explained = self._cluster.explain_allocation()
        except Exception as exc:
            # 미할당 샤드가 없으면 ES가 400을 돌려준다. 실패가 아니다.
            _logger.info("[collector] allocation explain 없음/실패: %s", exc)
        else:
            message += f" | allocation explain: {truncate_raw(str(explained), 2000)}"
            raw_payload["allocation_explain"] = explained
            endpoint += " + /_cluster/allocation/explain"

        raw = json.dumps(raw_payload, ensure_ascii=False, default=str, indent=2)

        return [
            Evidence(
                evidence_id=self._new_evidence_id(),
                event_time=now,
                source=EvidenceSource.CLUSTER_STATE,
                event_type="cluster_not_green",
                severity="Critical" if status == "red" else "Warning",
                message=message,
                raw=truncate_raw(raw),
                raw_kind="record",
                raw_truncated=len(truncate_raw(raw)) != len(raw),
                provenance=EvidenceProvenance(
                    method="elasticsearch_api", collected_at=now, endpoint=endpoint
                ),
                selection_reason="코드가 조회한 클러스터 상태. 모델을 거치지 않았다.",
            )
        ]

    def _collect_master(
        self, window: TimeRange, state: ObservationBuilder
    ) -> list[Evidence]:
        """ClickHouse 마스터 로그를 선별한다. 조회 실패는 gap으로 남긴다."""
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
            state.mark_gap(f"마스터 로그 조회 실패 (ClickHouse): {exc}")
            return []

        state.record_master_logs(entries)
        records = master_log.to_records(entries)

        if not records:
            return []
        return self._run_analysis(
            master_log.SPEC, group_into_buckets(records), state
        ).evidence

    def _investigate_nodes(
        self,
        master_evidence: list[Evidence],
        window: TimeRange,
        state: ObservationBuilder,
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
        )
        for status in result.source_statuses:
            state.record_source_status(status)
        for gap in result.gaps:
            state.mark_gap(gap)
        return result
