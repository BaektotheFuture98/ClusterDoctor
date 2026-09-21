"""Composition Root.

구현체를 고르는 곳은 여기 하나다. application과 domain은 포트만 알고, 어느
구현이 물려 있는지 모른다 — in-memory 저장소를 Redis로 바꾸는 날 바뀌는 파일도
여기 하나다.
"""

import queue as stdlib_queue
from functools import lru_cache
from urllib.parse import urlparse

import clickhouse_connect
from elasticsearch import Elasticsearch

from cluster_doctor.application.service.incident_orchestrator import IncidentOrchestrator
from cluster_doctor.application.service.slowlog_trigger_service import SlowlogTriggerService
from cluster_doctor.infrastructure.config.settings import Settings, get_settings
from cluster_doctor.infrastructure.inbound.kafka.consumer import KafkaConsumerAdapter
from cluster_doctor.infrastructure.outbound.agent.common.litellm_client import (
    require_supported_provider,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.agent import (
    DiagnosisAgentAdapter,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.agent import (
    SupervisorAgentAdapter,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.datasource.node_metric import (
    NodeMetricThresholds,
)
from cluster_doctor.infrastructure.outbound.clickhouse.clickhouse_log_adapter import (
    ClickHouseLogAdapter,
)
from cluster_doctor.infrastructure.outbound.elasticsearch.es_cluster_adapter import (
    ElasticsearchClusterAdapter,
)
from cluster_doctor.infrastructure.outbound.elasticsearch.es_node_resolver import (
    ElasticsearchNodeResolver,
)
from cluster_doctor.infrastructure.outbound.notifier.html_file_notifier import HtmlFileNotifier
from cluster_doctor.infrastructure.outbound.ssh.node_log_fetcher import SshNodeLogFetcher
from cluster_doctor.infrastructure.outbound.memory.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.infrastructure.outbound.memory.in_memory_incident_state_repository import (
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


def build_trigger_service(s: Settings | None = None) -> SlowlogTriggerService:
    if s is None:
        s = get_settings()

    # provider를 조립 시점에 검증한다. 잘못된 값을 첫 호출까지 끌고 가면
    # Incident 한 건을 통째로 날린 뒤에야 오타를 알게 된다.
    provider = require_supported_provider(s.llm_provider)

    # pending 큐를 먼저 만들고 drain 클로저와 orchestrator가 같은 객체를 공유한다.
    pending: stdlib_queue.Queue = stdlib_queue.Queue()

    def drain_pending():
        items = []
        while True:
            try:
                items.append(pending.get_nowait())
            except stdlib_queue.Empty:
                break
        return items

    log_repository = _get_log_repository()
    store = _get_artifact_store()

    # provider별 키·모델을 직접 읽지 않는다. llm_api_key/llm_model이
    # LLM_PROVIDER에 따라 고른 값을 돌려주므로 여기서 분기할 일이 없다.
    log_analysis_agent = DiagnosisAgentAdapter(
        provider=provider,
        model=s.llm_model,
        api_key=s.llm_api_key,
        fetch_logs=log_repository.fetch_logs,
        fetch_node_logs=log_repository.fetch_node_logs,
        cluster=_get_cluster_repository(),
        node_resolver=_get_node_resolver(),
        node_log_fetcher=SshNodeLogFetcher(
            ssh_user=s.ssh_user,
            ssh_password=s.ssh_password,
            ssh_port=s.ssh_port,
        ),
        store=store,
        metric_thresholds=NodeMetricThresholds(
            heap_warn_percent=s.node_heap_warn_percent,
            queue_warn=s.node_queue_warn,
        ),
    )

    orchestrator = IncidentOrchestrator(
        supervisor=SupervisorAgentAdapter(
            provider=provider, model=s.llm_model, api_key=s.llm_api_key
        ),
        log_analysis_agent=log_analysis_agent,
        state_repository=_get_state_repository(),
        artifact_store=store,
        # 리포트는 HTML 파일로 남긴다. 저장에 실패하면 어댑터가 전문을 로그로
        # 떨어뜨린다.
        notifier=HtmlFileNotifier(output_dir=s.report_dir),
        drain_pending=drain_pending,
    )

    return SlowlogTriggerService(
        orchestrator=orchestrator,
        pending=pending,
        cluster=s.cluster_name,
        micro_batch_seconds=s.micro_batch_seconds,
    )


def build_kafka_consumer(
    service: SlowlogTriggerService, s: Settings | None = None
) -> KafkaConsumerAdapter:
    if s is None:
        s = get_settings()
    return KafkaConsumerAdapter(
        service=service,
        bootstrap_servers=s.kafka_bootstrap_servers,
        topic=s.kafka_topic,
        group_id=s.kafka_group_id,
    )


def close_clickhouse_client() -> None:
    if _get_clickhouse_client.cache_info().currsize == 0:
        return
    _get_clickhouse_client().close()
