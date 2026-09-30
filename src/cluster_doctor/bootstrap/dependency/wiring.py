"""Composition Root.

구현체를 고르는 곳은 여기 하나다. 각 Agent/서비스는 자기 포트만 알고, 어느
구현이 물려 있는지 모른다 — datasource 구현을 바꾸는 날 바뀌는 파일도 여기
하나다.
"""

from functools import lru_cache
from urllib.parse import urlparse

import clickhouse_connect
from elasticsearch import Elasticsearch

from cluster_doctor.bootstrap.configuration.settings import Settings, get_settings
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    ClickHouseLogAdapter,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.cluster_health import (
    ElasticsearchClusterAdapter,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.node_resolver import (
    ElasticsearchNodeResolver,
)
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import SshNodeLogFetcher
from cluster_doctor.incident_orchestrator_agent.agent.adapter import (
    DeepAgentsConfig,
    build_deepagents_incident_analyzer,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    AnalyzeIncident,
)
from cluster_doctor.incident_orchestrator_agent.service.manual_analysis.manual_analysis import (
    RunManualAnalysis,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    HtmlFileReportPublisher,
)
from cluster_doctor.kafka_consumer.consumer.kafka.consumer import KafkaConsumerAdapter
from cluster_doctor.kafka_consumer.trigger_settling.service.intake import SlowlogIntake

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
def get_clickhouse_client():
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
        client=get_clickhouse_client(),
        slowlog_table=s.clickhouse_slowlog_table,
        log_table=s.clickhouse_log_table,
        node_metric_table=s.clickhouse_node_metric_table,
        node_log_table=s.clickhouse_node_log_table,
    )


def _build_analyze_incident(s: Settings) -> AnalyzeIncident:
    if s is None:
        s = get_settings()

    log_repository = _get_log_repository()

    incident_analyzer = build_deepagents_incident_analyzer(
        config=DeepAgentsConfig(
            provider=s.llm_provider,
            model=s.llm_model,
            api_key=s.llm_api_key,
            heap_warn_percent=s.node_heap_warn_percent,
            queue_warn=s.node_queue_warn,
        ),
        log_repository=log_repository,
        cluster_repository=_get_cluster_repository(),
        node_resolver=_get_node_resolver(),
        node_log_fetcher=SshNodeLogFetcher(
            ssh_user=s.ssh_user,
            ssh_password=s.ssh_password,
            ssh_port=s.ssh_port,
        ),
    )

    return AnalyzeIncident(
        incident_analyzer=incident_analyzer,
        # 리포트는 HTML 파일로 남긴다. 저장에 실패하면 어댑터가 전문을 로그로
        # 떨어뜨린다.
        report_publisher=HtmlFileReportPublisher(output_dir=s.report_dir),
    )

def build_slowlog_intake(s: Settings | None = None) -> SlowlogIntake:
    if s is None:
        s = get_settings()
    return SlowlogIntake(
        analyze_incident=_build_analyze_incident(s),
        cluster=s.cluster_name,
        max_pending=10_000,
    )

def build_manual_analysis(s: Settings | None = None) -> RunManualAnalysis:
    if s is None:
        s = get_settings()
    return RunManualAnalysis(
        analyze_incident=_build_analyze_incident(s), cluster=s.cluster_name
    )

def build_kafka_consumer(
    intake: SlowlogIntake, s: Settings | None = None
) -> KafkaConsumerAdapter:
    if s is None:
        s = get_settings()
    return KafkaConsumerAdapter(
        intake=intake,
        bootstrap_servers=s.kafka_bootstrap_servers,
        topic=s.kafka_topic,
        group_id=s.kafka_group_id,
    )
