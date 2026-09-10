import queue as stdlib_queue
from functools import lru_cache
from urllib.parse import urlparse

import clickhouse_connect
from elasticsearch import Elasticsearch

from cluster_doctor.application.service.slowlog_trigger_service import SlowlogTriggerService
from cluster_doctor.infrastructure.outbound.clickhouse.clickhouse_log_adapter import ClickHouseLogAdapter
from cluster_doctor.infrastructure.outbound.elasticsearch.es_cluster_adapter import ElasticsearchClusterAdapter
from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import DeepAgentAnalyzer
from cluster_doctor.infrastructure.outbound.notifier.stdout_notifier import StdoutNotifier
from cluster_doctor.infrastructure.outbound.ssh.node_log_fetcher import NodeLogFetcher
from cluster_doctor.infrastructure.inbound.kafka.consumer import KafkaConsumerAdapter
from cluster_doctor.infrastructure.config.settings import Settings, get_settings

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
def _get_log_repository() -> ClickHouseLogAdapter:
    s = get_settings()
    return ClickHouseLogAdapter(
        client=_get_clickhouse_client(),
        slowlog_table=s.clickhouse_slowlog_table,
        log_table=s.clickhouse_log_table,
        node_metric_table=s.clickhouse_node_metric_table,
    )


def build_trigger_service(s: Settings | None = None) -> SlowlogTriggerService:
    if s is None:
        s = get_settings()

    # pending 큐를 먼저 만들고 drain 클로저와 analyzer가 같은 객체를 공유한다.
    pending: stdlib_queue.Queue = stdlib_queue.Queue()

    def drain_pending():
        items = []
        while True:
            try:
                items.append(pending.get_nowait())
            except stdlib_queue.Empty:
                break
        return items

    # provider별 키·모델을 직접 읽지 않는다. llm_api_key/llm_model이
    # LLM_PROVIDER에 따라 고른 값을 돌려주므로 여기서 분기할 일이 없다.
    analyzer = DeepAgentAnalyzer(
        provider=s.llm_provider,
        api_key=s.llm_api_key,
        default_model=s.llm_model,
        cluster=_get_cluster_repository(),
        fetch_logs=_get_log_repository().fetch_logs,
        drain_pending=drain_pending,
        node_log_fetcher=NodeLogFetcher(
            ssh_user=s.ssh_user,
            ssh_password=s.ssh_password,
            ssh_port=s.ssh_port,
        ),
    )

    return SlowlogTriggerService(
        llm_analyzer=analyzer,
        notifier=StdoutNotifier(),
        pending=pending,
        micro_batch_seconds=s.micro_batch_seconds,
    )


def build_kafka_consumer(service: SlowlogTriggerService, s: Settings | None = None) -> KafkaConsumerAdapter:
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
