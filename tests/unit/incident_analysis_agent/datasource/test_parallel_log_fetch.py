from datetime import UTC, datetime, timedelta
from threading import Barrier
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.datasource.clickhouse import (
    node_metric,
    query_log,
    slowlog,
)
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    ClickHouseLogAdapter,
)
from cluster_doctor.incident_analysis_agent.model.log_entries import SlowlogEntry
from cluster_doctor.incident_analysis_agent.model.log_fetch import (
    LogFetchResult,
    LogSourceFailure,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.evidence_collection.collector import (
    EvidenceCollector,
)
from cluster_doctor.incident_analysis_agent.service.observation.builder import (
    ObservationBuilder,
)

T0 = datetime(2026, 10, 1, tzinfo=UTC)
WINDOW = TimeRange(T0, T0 + timedelta(minutes=1))


def test_sources_enter_query_concurrently_and_partial_results_survive(monkeypatch):
    gate = Barrier(3)
    called = []

    def fetch(source):
        def run(client, table, window):
            called.append((source, table, window))
            gate.wait(timeout=2)
            if source == "slowlog":
                raise OSError("slowlog unavailable")
            return [SimpleNamespace(timestamp=T0, source=source)]

        return run

    for module, source in (
        (slowlog, "slowlog"),
        (query_log, "es_query_log"),
        (node_metric, "node_metric"),
    ):
        monkeypatch.setattr(module, "fetch", fetch(source))
    adapter = ClickHouseLogAdapter(object(), "slow", "query", "metric", "node")
    result = adapter.fetch_logs(WINDOW)
    assert {e.source for e in result.entries} == {"es_query_log", "node_metric"}
    assert len(called) == 3
    assert all(window == WINDOW for _, _, window in called)
    assert len(result.failures) == 1
    assert result.failures[0].source == "slowlog"
    assert result.failures[0].error == "slowlog unavailable"


def test_collector_keeps_successful_source_and_reports_exact_failure():
    fetched = LogFetchResult(
        entries=(SlowlogEntry(timestamp=T0, took="1s", query='{"query":{}}'),),
        failures=(
            LogSourceFailure(source="es_query_log", window=WINDOW, error="unavailable"),
        ),
    )
    collector = EvidenceCollector(
        new_evidence_id=lambda: "E",
        fetch_logs=lambda _: fetched,
        fetch_node_logs=lambda *a, **kw: [],
        cluster=None,
        node_resolver=None,
        node_log_fetcher=None,
        call_llm=lambda *a, **kw: "",
    )
    state = ObservationBuilder(WINDOW)
    slow, queries, metrics = collector._fetch_and_bucket(WINDOW, state)
    assert len(slow) == 1 and queries == [] and metrics == []
    assert state.to_observations().timeline[0].counts["slowlog"] == 1
    assert not state.degraded
    assert any("es_query_log" in gap and "unavailable" in gap for gap in state.gaps)
    assert not any("노드 메트릭 조회 실패" in gap for gap in state.gaps)


@pytest.mark.parametrize(
    "failed_sources", [(), ("slowlog",), ("slowlog", "es_query_log", "node_metric")]
)
def test_empty_success_is_distinct_from_all_sources_failing(failed_sources):
    result = LogFetchResult(
        failures=tuple(
            LogSourceFailure(source, WINDOW, "unavailable") for source in failed_sources
        )
    )
    collector = EvidenceCollector(
        new_evidence_id=lambda: "E",
        fetch_logs=lambda _: result,
        fetch_node_logs=lambda *a, **kw: [],
        cluster=None,
        node_resolver=None,
        node_log_fetcher=None,
        call_llm=lambda *a, **kw: "",
    )
    state = ObservationBuilder(WINDOW)
    assert collector._fetch_and_bucket(WINDOW, state) == ([], [], [])
    assert state.degraded == (len(failed_sources) == 3)
    assert len(state.gaps) == len(failed_sources)


def test_each_source_is_fetched_once_per_minute_and_results_sorted(monkeypatch):
    calls = []

    def fetch(client, table, window):
        calls.append((table, window))
        return [SimpleNamespace(timestamp=window.start)]

    for module in (slowlog, query_log, node_metric):
        monkeypatch.setattr(module, "fetch", fetch)
    window = TimeRange(T0, T0 + timedelta(minutes=2))
    result = ClickHouseLogAdapter(
        object(), "slow", "query", "metric", "node"
    ).fetch_logs(window)
    assert len(calls) == len(set(calls)) == 6
    assert {table for table, _ in calls} == {"slow", "query", "metric"}
    assert [entry.timestamp for entry in result.entries] == [
        T0 + timedelta(minutes=1)
    ] * 3 + [T0] * 3
    assert not result.failures
