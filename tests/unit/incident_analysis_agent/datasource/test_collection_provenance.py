import json
from datetime import UTC, datetime, timedelta
from itertools import count
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.datasource.clickhouse import (
    master_log,
    node_metric,
    query_log,
    slowlog,
)
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    ClickHouseLogAdapter,
)
from cluster_doctor.incident_analysis_agent.model.evidence import ProblemNodeCandidate
from cluster_doctor.incident_analysis_agent.model.log_fetch import LogFetchResult
from cluster_doctor.incident_analysis_agent.model.resolved_node import ResolvedNode
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.evidence_collection.collector import (
    EvidenceCollector,
)
from cluster_doctor.incident_analysis_agent.service.node_investigation.node_investigation import (
    investigate_nodes,
)
from cluster_doctor.incident_analysis_agent.service.observation.builder import (
    ObservationBuilder,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.schema import (
    MapOutput,
)

T0 = datetime(2026, 10, 1, tzinfo=UTC)
WINDOW = TimeRange(start=T0, end=T0 + timedelta(minutes=1))


class Client:
    def __init__(self, row):
        self.row = row

    def query(self, sql, parameters):
        if isinstance(self.row, dict):
            return SimpleNamespace(
                column_names=tuple(self.row), result_rows=[tuple(self.row.values())]
            )
        return SimpleNamespace(result_rows=[self.row])


@pytest.mark.parametrize(
    "module,row",
    [
        (
            slowlog,
            [
                T0,
                "index",
                "data-03",
                "12s",
                "100",
                3,
                "opaque",
                '{"query": "original"}',
            ],
        ),
        (
            query_log,
            {
                "reg_date": T0,
                "host": "data-03",
                "run_time": 12,
                "success": "N",
                "s_date": 20260901,
                "e_date": 20260930,
                "date_range": 30,
                "keyword": [],
                "url": "/search",
                "cmd": "original cmd",
                "service": "service",
                "env": "prod",
                "project": "project",
                "cluster": "cluster",
                "company": "company",
                "user": "user",
                "search_count": 0,
                "etc": "",
            },
        ),
        (
            node_metric,
            [T0, "data-03", "10.0.1.23", 90, 80, 70, 95, 10, 150, 1, 0, 0, 0],
        ),
    ],
)
def test_clickhouse_fetch_retains_table_window_and_record(module, row):
    entry = module.fetch(Client(row), "actual_db.actual_table", WINDOW)[0]
    p = entry.provenance
    assert p.method == "clickhouse" and p.table == "actual_db.actual_table"
    assert p.query_from == T0 and p.query_to == WINDOW.end
    assert p.collected_at is not None
    if module is node_metric:
        evidence = module.to_evidence([entry], new_evidence_id=lambda: "E-1")[0]
        assert evidence.provenance == p
        assert evidence.node_name == "data-03"
        assert "search_rejected=1" in evidence.message
    else:
        record = module.to_records([entry])[0]
        assert record.provenance == p
        assert "original" in record.line


def test_clickhouse_master_keeps_original_file_and_line():
    row = [
        T0,
        "master-01",
        "master",
        "WARN",
        "warn",
        "logger",
        "/original/es.log",
        "10.0.1.1",
        "original <line>",
    ]
    adapter = ClickHouseLogAdapter(
        Client(row), "slow", "query", "metric", "actual_db.node_logs"
    )
    record = master_log.to_records(adapter.fetch_node_logs(T0, WINDOW.end))[0]
    assert "original <line>" in record.line
    assert record.provenance.file_path == "/original/es.log"
    assert record.provenance.host == "10.0.1.1"
    assert record.provenance.table == "actual_db.node_logs"


def select_first(messages, response_format):
    return json.dumps(
        {"selected": [{"record_id": 1}]}
        if response_format is MapOutput
        else {"keep": [{"record_id": 1}]}
    )


class Fetcher:
    def __init__(self):
        self.calls = []

    def fetch(self, host, path, cluster, **kwargs):
        self.calls.append((host, path, cluster))
        return "[2026-10-01T09:00:02,123][WARN ][logger] original error"


def resolved():
    return ResolvedNode(
        node_id="node-id",
        node_name="master-01",
        host="10.0.1.1",
        log_path="/actual/es",
        cluster_name="prod",
    )


def test_master_query_failure_records_gap_without_resolving_or_using_ssh():
    fetcher = Fetcher()
    resolved_ids = []

    def unavailable(*args, **kwargs):
        raise OSError("ClickHouse unavailable")

    collector = EvidenceCollector(
        new_evidence_id=lambda: "E-1",
        fetch_logs=lambda tr: LogFetchResult(),
        fetch_node_logs=unavailable,
        cluster=None,
        node_resolver=SimpleNamespace(
            resolve=lambda node: resolved_ids.append(node) or resolved()
        ),
        node_log_fetcher=fetcher,
        call_llm=select_first,
    )
    state = ObservationBuilder(WINDOW)
    assert collector._collect_master(WINDOW, state) == []
    assert resolved_ids == [] and fetcher.calls == []
    assert any("마스터 로그 조회 실패" in gap for gap in state.gaps)


def test_data_node_ssh_investigation_keeps_collection_location():
    ids = count(1)
    result = investigate_nodes(
        [
            ProblemNodeCandidate(
                node_id="node-id", reason="error", evidence_refs=("E-master",)
            )
        ],
        WINDOW,
        resolver=SimpleNamespace(resolve=lambda _: resolved()),
        fetcher=Fetcher(),
        call_llm=select_first,
        new_evidence_id=lambda: f"E-{next(ids)}",
    )
    evidence = result.evidence[0]
    assert evidence.provenance.method == "ssh"
    assert evidence.provenance.file_path == "/actual/es/prod.log"
    assert evidence.node_id == "node-id"


def test_cluster_state_keeps_api_response_and_provenance():
    payload = {"status": "red", "unassigned_shards": 1}
    cluster = SimpleNamespace(
        health=lambda: payload, explain_allocation=lambda: {"reason": "original"}
    )
    collector = EvidenceCollector(
        new_evidence_id=lambda: "E-1",
        fetch_logs=lambda tr: LogFetchResult(),
        fetch_node_logs=lambda *a: [],
        cluster=cluster,
        node_resolver=None,
        node_log_fetcher=None,
        call_llm=select_first,
    )
    evidence = collector._collect_cluster_health(ObservationBuilder(WINDOW))[0]
    assert evidence.provenance.method == "elasticsearch_api"
    assert "/_cluster/health" in evidence.provenance.endpoint
    assert f"unassigned_shards={payload['unassigned_shards']}" in evidence.message
