"""Composition Root.

구현체를 고르는 곳은 여기 하나다. 각 Agent/서비스는 자기 포트만 알고, 어느
구현이 물려 있는지 모른다 — datasource 구현을 바꾸는 날 바뀌는 파일도 여기
하나다.
"""

from contextlib import ExitStack
import logging

from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.file.report_publisher import (
    ReportPublisher,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.sftp.sftp_report_publisher import (
    SftpReportPublisher,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.sftp.sftp_uploader import (
    SftpTarget,
    SftpUploader,
)

_logger = logging.getLogger(__name__)
from urllib.parse import urlparse

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from elasticsearch import Elasticsearch

from cluster_doctor.bootstrap.configuration.settings import Settings
from cluster_doctor.bootstrap.lifecycle.app_lifecycle import RuntimeResources
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    ClickHouseLogAdapter,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.cluster_health import (
    ElasticsearchClusterAdapter,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.node_resolver import (
    ElasticsearchNodeResolver,
)
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import (
    SshNodeLogFetcher,
)
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
from cluster_doctor.kafka_consumer.trigger_settling.service.problem_log_processor import ProblemLogProcessor

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


def _build_clickhouse_client(settings: Settings) -> Client:
    host, port, db = _parse_clickhouse_url(settings.clickhouse_url)
    return clickhouse_connect.get_client(
        host=host, port=port, database=db,
        username=settings.clickhouse_user, password=settings.clickhouse_password,
        send_receive_timeout=_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS,
        # 공유 HTTP client의 병렬 조회가 동일한 ClickHouse 세션을 사용하지 않도록 한다.
        autogenerate_session_id=False,
        # Domain windows are timezone-aware; UTC results must retain their offset.
        tz_mode="aware",
    )


def _build_es_client(settings: Settings) -> Elasticsearch:
    hosts = [
        {"host": h.strip(), "port": settings.es_port, "scheme": "http"}
        for h in settings.es_host.split(",")
        if h.strip()
    ]
    return Elasticsearch(
        hosts=hosts,
        basic_auth=(settings.es_user, settings.es_password) if settings.es_user else None,
        request_timeout=_ES_REQUEST_TIMEOUT_SECONDS,
    )


def build_runtime_resources(settings: Settings) -> RuntimeResources:
    """Create execution-owned clients, rolling back a partially built runtime."""
    with ExitStack() as stack:
        clickhouse_client = _build_clickhouse_client(settings)
        stack.callback(clickhouse_client.close)
        es_client = _build_es_client(settings)
        stack.callback(es_client.close)
        return RuntimeResources(clickhouse_client, es_client, stack.pop_all())


def _build_cluster_repository(client: Elasticsearch) -> ElasticsearchClusterAdapter:
    return ElasticsearchClusterAdapter(client)


def _build_node_resolver(client: Elasticsearch) -> ElasticsearchNodeResolver:
    return ElasticsearchNodeResolver(client)


def _build_log_repository(settings: Settings, client: Client) -> ClickHouseLogAdapter:
    return ClickHouseLogAdapter(
        client=client,
        slowlog_table=settings.clickhouse_slowlog_table,
        log_table=settings.clickhouse_log_table,
        node_metric_table=settings.clickhouse_node_metric_table,
        node_log_table=settings.clickhouse_node_log_table,
    )


def build_sftp_uploader(settings: Settings, *, attempts: int = 2) -> SftpUploader:
    """``REPORT_SFTP_*`` 설정으로 업로더를 만든다. 서버 호스트 키를 검증하지 않으면 경고한다."""
    if not settings.report_sftp_known_hosts.strip():
        _logger.warning(
            "REPORT_SFTP_KNOWN_HOSTS가 비어 있어 서버 호스트 키를 검증하지 않는다"
        )
    return SftpUploader(
        SftpTarget(
            host=settings.report_sftp_host.strip(),
            port=settings.report_sftp_port,
            user=settings.report_sftp_user.strip(),
            remote_dir=settings.report_sftp_remote_dir.strip(),
            password=settings.report_sftp_password.get_secret_value(),
            key_file=settings.report_sftp_key_file.strip(),
            known_hosts=settings.report_sftp_known_hosts.strip(),
            timeout_seconds=settings.report_sftp_timeout_seconds,
        ),
        attempts=attempts,
    )


def _build_report_publisher(settings: Settings) -> ReportPublisher:
    """리포트는 로컬 HTML 파일로 남기고, 설정이 켜져 있으면 SFTP로도 올린다."""
    local = HtmlFileReportPublisher(output_dir=settings.report_dir)
    if not settings.report_sftp_enabled:
        return local
    return SftpReportPublisher(local, build_sftp_uploader(settings))


def _build_analyze_incident(
    settings: Settings, runtime_resources: RuntimeResources
) -> AnalyzeIncident:
    log_repository = _build_log_repository(settings, runtime_resources.clickhouse_client)

    incident_analyzer = build_deepagents_incident_analyzer(
        config=DeepAgentsConfig(
            provider=settings.llm_provider,
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            heap_warn_percent=settings.node_heap_warn_percent,
            queue_warn=settings.node_queue_warn,
        ),
        log_repository=log_repository,
        cluster_repository=_build_cluster_repository(runtime_resources.es_client),
        node_resolver=_build_node_resolver(runtime_resources.es_client),
        node_log_fetcher=SshNodeLogFetcher(
            ssh_user=settings.ssh_user,
            ssh_password=settings.ssh_password,
            ssh_port=settings.ssh_port,
        ),
    )

    return AnalyzeIncident(
        incident_analyzer=incident_analyzer,
        # 리포트는 HTML 파일로 남긴다. 저장에 실패하면 어댑터가 전문을 로그로
        # 떨어뜨린다.
        report_publisher=_build_report_publisher(settings),
    )


def build_problem_log_processor(
    settings: Settings, runtime_resources: RuntimeResources
) -> ProblemLogProcessor:
    return ProblemLogProcessor(
        analyze_incident=_build_analyze_incident(settings, runtime_resources),
        cluster=settings.cluster_name,
        max_pending=10_000,
    )


def build_manual_analysis(
    settings: Settings, runtime_resources: RuntimeResources
) -> RunManualAnalysis:
    return RunManualAnalysis(
        analyze_incident=_build_analyze_incident(settings, runtime_resources),
        cluster=settings.cluster_name,
    )


def build_kafka_consumer(
    problem_log_processor: ProblemLogProcessor, settings: Settings
) -> KafkaConsumerAdapter:
    return KafkaConsumerAdapter(
        problem_log_processor=problem_log_processor,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
        group_id=settings.kafka_group_id,
        failure_timeout_seconds=settings.kafka_failure_timeout_seconds,
    )
