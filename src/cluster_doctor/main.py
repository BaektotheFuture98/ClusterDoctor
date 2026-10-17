import asyncio
import logging
import os
from contextlib import ExitStack
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import urlparse

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from elasticsearch import Elasticsearch

from cluster_doctor.bootstrap.configuration.settings import (
    LoggingSettings,
    Settings,
    get_settings,
)
from cluster_doctor.bootstrap.lifecycle.app_lifecycle import RuntimeResources
from cluster_doctor.incident_analysis_agent.agent.subagent import AnalysisAgent
from cluster_doctor.incident_analysis_agent.agent.dependencies import AnalysisSeams
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    ClickHouseLogAdapter,
)
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import (
    NodeMetricThresholds,
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
from cluster_doctor.incident_analysis_agent.agent.runtime.litellm_client import (
    require_supported_provider,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
    build_structured_call,
)
from cluster_doctor.incident_orchestrator_agent.agent.adapter import MainAgent
from cluster_doctor.incident_orchestrator_agent.agent.analysis_subagent import (
    AnalysisSubagentAdapter,
)
from cluster_doctor.incident_orchestrator_agent.agent.runtime.chat_model import (
    build_chat_model,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    AnalyzeIncident,
)
from cluster_doctor.kafka_consumer.consumer.kafka.consumer import KafkaConsumerAdapter
from cluster_doctor.kafka_consumer.trigger_settling.service.problem_log_processor import (
    ProblemLogProcessor,
)
from cluster_doctor.exceptions import KafkaUnavailableError
from cluster_doctor.log_context import IncidentIdLogFilter

from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    HtmlFileReportPublisher,
)
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

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s [%(incident_id)s] %(message)s"
_HANDLER_MARKER = "_cluster_doctor_owned_handler"


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    already_configured = any(
        getattr(handler, _HANDLER_MARKER, False) for handler in root_logger.handlers
    )
    if already_configured:
        return

    logging_settings = LoggingSettings()
    log_dir = Path(logging_settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(_LOG_FORMAT)
    incident_id_filter = IncidentIdLogFilter()

    file_handler = RotatingFileHandler(
        log_dir / "app.log",
        maxBytes=logging_settings.log_max_bytes,
        backupCount=logging_settings.log_backup_count,
        encoding="utf-8",
    )
    stream_handler = logging.StreamHandler()
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        handler.addFilter(incident_id_filter)
        setattr(handler, _HANDLER_MARKER, True)
        root_logger.addHandler(handler)


_logger = logging.getLogger(__name__)

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
        # 도메인의 조회 창이 timezone-aware이므로 결과도 UTC 오프셋을 유지해야 한다.
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
    """실행 단위가 소유하는 클라이언트를 만들고, 일부만 만들어진 runtime은 롤백한다."""
    with ExitStack() as stack:
        clickhouse_client = _build_clickhouse_client(settings)
        stack.callback(clickhouse_client.close)
        es_client = _build_es_client(settings)
        stack.callback(es_client.close)
        return RuntimeResources(clickhouse_client, es_client, stack.pop_all())


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


def build_report_publisher(settings: Settings) -> ReportPublisher:
    """리포트는 로컬 HTML 파일로 남기고, 설정이 켜져 있으면 SFTP로도 올린다."""
    local = HtmlFileReportPublisher(output_dir=settings.report_dir)
    if not settings.report_sftp_enabled:
        return local
    return SftpReportPublisher(local, build_sftp_uploader(settings))


def compose_analysis_service(settings: Settings, resources: RuntimeResources) -> AnalyzeIncident:
    """자동·수동 실행이 공유하는 분석 의존성을 명시적으로 조립한다."""
    log_repository = ClickHouseLogAdapter(
        client=resources.clickhouse_client,
        slowlog_table=settings.clickhouse_slowlog_table,
        log_table=settings.clickhouse_log_table,
        node_metric_table=settings.clickhouse_node_metric_table,
        node_log_table=settings.clickhouse_node_log_table,
    )
    provider = require_supported_provider(settings.llm_provider)
    call_llm = build_structured_call(
        provider=provider, model=settings.llm_model, api_key=settings.llm_api_key
    )
    seams = AnalysisSeams(
        fetch_logs=log_repository.fetch_logs,
        fetch_node_logs=log_repository.fetch_node_logs,
        cluster=ElasticsearchClusterAdapter(resources.es_client),
        node_resolver=ElasticsearchNodeResolver(resources.es_client),
        node_log_fetcher=SshNodeLogFetcher(
            ssh_user=settings.ssh_user,
            ssh_password=settings.ssh_password,
            ssh_port=settings.ssh_port,
        ),
        call_llm=call_llm,
        report_writer=ReportWriter(call_llm=call_llm),
        metric_thresholds=NodeMetricThresholds(
            heap_warn_percent=settings.node_heap_warn_percent,
            queue_warn=settings.node_queue_warn,
        ),
        analysis_concurrency=settings.analysis_concurrency,
    )
    model = build_chat_model(
        provider=provider, model=settings.llm_model, api_key=settings.llm_api_key
    )
    analysis_agent = AnalysisAgent(model=model, seams=seams)
    analysis_subagent = AnalysisSubagentAdapter(analysis_agent=analysis_agent)
    incident_analyzer = MainAgent(
        model=model, seams=seams, analysis_subagent=analysis_subagent
    )
    return AnalyzeIncident(
        incident_analyzer=incident_analyzer,
        report_publisher=build_report_publisher(settings),
    )


async def main() -> None:
    configure_logging()
    settings = get_settings()
    with build_runtime_resources(settings) as runtime_resources:
        problem_log_processor = ProblemLogProcessor(
            analyze_incident=compose_analysis_service(settings, runtime_resources),
            cluster=settings.cluster_name,
            max_pending=10_000,
        )
        try:
            kafka_consumer = KafkaConsumerAdapter(
                problem_log_processor=problem_log_processor,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                topic=settings.kafka_topic,
                group_id=settings.kafka_group_id,
                failure_timeout_seconds=settings.kafka_failure_timeout_seconds,
            )
            try:
                await kafka_consumer.run()
            except KafkaUnavailableError as exc:
                logging.getLogger(__name__).critical("%s; terminating process with exit code 1", exc)
                logging.shutdown()
                # consumer 정리는 이미 시도했다. 정상 종료는 분석 스레드를 기다리므로
                # 이 치명적 장애 정책을 지킬 수 없다. 대기·진행 중 분석을 버리고
                # 프로세스 전체를 즉시 끝낸다.
                os._exit(1)
        finally:
            await problem_log_processor.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Received keyboard interrupt, shutting down...")
        pass
