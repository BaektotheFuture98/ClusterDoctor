"""Composition Root.

구현체를 고르는 곳은 여기 하나다. application과 domain은 포트만 알고, 어느
구현이 물려 있는지 모른다 — in-memory 저장소를 Redis로 바꾸는 날 바뀌는 파일도
여기 하나다.
"""

from functools import lru_cache
from urllib.parse import urlparse

import clickhouse_connect
from elasticsearch import Elasticsearch

from cluster_doctor.config.settings import Settings, get_settings
from cluster_doctor.adapters.inbound.kafka.consumer import KafkaConsumerAdapter
from cluster_doctor.application.use_cases.diagnose_incident import DiagnoseIncident
from cluster_doctor.application.use_cases.manual_diagnosis import RunManualDiagnosis
from cluster_doctor.application.use_cases.slowlog_intake import SlowlogIntake
from cluster_doctor.application.ports.slowlog_handler import SlowlogHandler
from cluster_doctor.adapters.outbound.deepagents import (
    DeepAgentsConfig,
    build_deepagents_incident_analyzer,
)
from cluster_doctor.adapters.outbound.clickhouse.reader import (
    ClickHouseLogAdapter,
)
from cluster_doctor.adapters.outbound.elasticsearch.cluster_adapter import (
    ElasticsearchClusterAdapter,
)
from cluster_doctor.adapters.outbound.elasticsearch.node_resolver import (
    ElasticsearchNodeResolver,
)
from cluster_doctor.adapters.outbound.reporting.html_file_notifier import (
    HtmlFileReportPublisher,
)
from cluster_doctor.adapters.outbound.ssh.fetcher import SshNodeLogFetcher
from cluster_doctor.adapters.outbound.persistence.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.adapters.outbound.persistence.in_memory_incident_state_store import (
    InMemoryIncidentStateRepository,
)

_DEFAULT_CLICKHOUSE_PORT = 8123
_DEFAULT_DATABASE = "default"
_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS = 30
_ES_REQUEST_TIMEOUT_SECONDS = 10


def _parse_clickhouse_url(jdbc_url: str) -> tuple[str, int, str]:
    parsed = urlparse(jdbc_url.replace("jdbc:", "", 1))
    host = parsed.hostname or "localhost"
    port = parsed.port or _DEFAULT_CLICKHOUSE_PORT
    db = (parsed.path or "").lstrip("/") or _DEFAULT_DATABASE
    return host, port, db


@lru_cache
def _get_clickhouse_client():
    s = get_settings()
    host, port, db = _parse_clickhouse_url(s.clickhouse_url)
    return clickhouse_connect.get_client(
        host=host, port=port, database=db,
        username=s.clickhouse_user, password=s.clickhouse_password,
        send_receive_timeout=_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS,
    )


@lru_cache
def _get_es_client() -> Elasticsearch:
    s = get_settings()
    hosts = [
        {"host": h.strip(), "port": s.es_port, "scheme": "http"}
        for h in s.es_host.split(",")
        if h.strip()
    ]
    return Elasticsearch(
        hosts=hosts,
        basic_auth=(s.es_user, s.es_password) if s.es_user else None,
        request_timeout=_ES_REQUEST_TIMEOUT_SECONDS,
    )


@lru_cache
def _get_cluster_repository() -> ElasticsearchClusterAdapter:
    return ElasticsearchClusterAdapter(_get_es_client())


@lru_cache
def _get_node_resolver() -> ElasticsearchNodeResolver:
    return ElasticsearchNodeResolver(_get_es_client())


@lru_cache
def _get_log_repository() -> ClickHouseLogAdapter:
    s = get_settings()
    return ClickHouseLogAdapter(
        client=_get_clickhouse_client(),
        slowlog_table=s.clickhouse_slowlog_table,
        log_table=s.clickhouse_log_table,
        node_metric_table=s.clickhouse_node_metric_table,
        node_log_table=s.clickhouse_node_log_table,
    )


@lru_cache
def _get_state_repository() -> InMemoryIncidentStateRepository:
    return InMemoryIncidentStateRepository()


@lru_cache
def _get_artifact_store() -> InMemoryArtifactStore:
    return InMemoryArtifactStore()


def _build_diagnose_incident(s: Settings) -> DiagnoseIncident:
    if s is None:
        s = get_settings()

    log_repository = _get_log_repository()
    state_repo = _get_state_repository()
    store = _get_artifact_store()

    def _cleanup_incident(incident_id: str) -> None:
        state_repo.discard(incident_id)
        store.discard(incident_id)

    incident_analyzer = build_deepagents_incident_analyzer(
        config=DeepAgentsConfig(
            provider=s.llm_provider,
            model=s.llm_model,
            api_key=s.llm_api_key,
            heap_warn_percent=s.node_heap_warn_percent,
            queue_warn=s.node_queue_warn,
        ),
        state_repository=state_repo,
        artifact_store=store,
        log_repository=log_repository,
        cluster_repository=_get_cluster_repository(),
        node_resolver=_get_node_resolver(),
        node_log_fetcher=SshNodeLogFetcher(
            ssh_user=s.ssh_user,
            ssh_password=s.ssh_password,
            ssh_port=s.ssh_port,
        ),
    )

    return DiagnoseIncident(
        incident_analyzer=incident_analyzer,
        state_repository=state_repo,
        artifact_store=store,
        # 리포트는 HTML 파일로 남긴다. 저장에 실패하면 어댑터가 전문을 로그로
        # 떨어뜨린다.
        report_publisher=HtmlFileReportPublisher(output_dir=s.report_dir),
        on_incident_complete=_cleanup_incident,
    )

def build_slowlog_intake(s: Settings | None = None) -> SlowlogIntake:
    if s is None:
        s = get_settings()
    return SlowlogIntake(
        diagnose_incident=_build_diagnose_incident(s),
        cluster=s.cluster_name,
        micro_batch_seconds=s.micro_batch_seconds,
        max_pending=10_000,
    )

def build_manual_diagnosis(s: Settings | None = None) -> RunManualDiagnosis:
    if s is None:
        s = get_settings()
    return RunManualDiagnosis(
        diagnose_incident=_build_diagnose_incident(s), cluster=s.cluster_name
    )

def build_kafka_consumer(
    intake: SlowlogHandler, s: Settings | None = None
) -> KafkaConsumerAdapter:
    if s is None:
        s = get_settings()
    return KafkaConsumerAdapter(
        intake=intake,
        bootstrap_servers=s.kafka_bootstrap_servers,
        topic=s.kafka_topic,
        group_id=s.kafka_group_id,
    )


def close_clickhouse_client() -> None:
    if _get_clickhouse_client.cache_info().currsize == 0:
        return
    _get_clickhouse_client().close()
