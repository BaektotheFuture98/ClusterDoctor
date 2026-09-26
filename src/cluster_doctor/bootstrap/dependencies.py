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
from cluster_doctor.adapters.outbound.deepagents.runtime.litellm_client import (
    require_supported_provider,
)
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.report_writer import (
    ReportWriter,
    build_structured_call,
)
from cluster_doctor.adapters.outbound.deepagents.diagnosis.session import (
    DiagnosisSeams,
)
from cluster_doctor.adapters.outbound.deepagents.adapter import (
    DeepAgentIncidentAnalyzer,
)
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.datasource.node_metric import (
    NodeMetricThresholds,
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

    # provider를 조립 시점에 검증한다. 잘못된 값을 첫 호출까지 끌고 가면
    # Incident 한 건을 통째로 날린 뒤에야 오타를 알게 된다.
    provider = require_supported_provider(s.llm_provider)

    log_repository = _get_log_repository()
    state_repo = _get_state_repository()
    store = _get_artifact_store()

    def _cleanup_incident(incident_id: str) -> None:
        state_repo.discard(incident_id)
        store.discard(incident_id)

    # provider별 키·모델을 직접 읽지 않는다. llm_api_key/llm_model이
    # LLM_PROVIDER에 따라 고른 값을 돌려주므로 여기서 분기할 일이 없다.
    #
    # 누구에게 묻는가는 여기서 한 번만 정한다. 아래 계층(수집기, 워크플로 노드,
    # ReportWriter)은 호출자 하나만 받고 provider를 모른다.
    call_llm = build_structured_call(
        provider=provider, model=s.llm_model, api_key=s.llm_api_key
    )

    # 진단 SubAgent가 도구로 내놓는 조각들. 구체 타입을 고르는 일은 조립부의
    # 몫이므로 여기서 이름을 부른다 — SubAgent는 받은 조각을 쓰기만 한다.
    seams = DiagnosisSeams(
        store=store,
        fetch_logs=log_repository.fetch_logs,
        fetch_node_logs=log_repository.fetch_node_logs,
        cluster=_get_cluster_repository(),
        node_resolver=_get_node_resolver(),
        node_log_fetcher=SshNodeLogFetcher(
            ssh_user=s.ssh_user,
            ssh_password=s.ssh_password,
            ssh_port=s.ssh_port,
        ),
        call_llm=call_llm,
        report_writer=ReportWriter(store=store, call_llm=call_llm),
        metric_thresholds=NodeMetricThresholds(
            heap_warn_percent=s.node_heap_warn_percent,
            queue_warn=s.node_queue_warn,
        ),
    )

    # Main DeepAgent. 진단 SubAgent는 이 어댑터가 Incident마다 등록한다 —
    # 도구와 Guardrail이 그 Incident의 State를 쥐어야 하기 때문이다.
    incident_analyzer = DeepAgentIncidentAnalyzer(
        provider=provider,
        model=s.llm_model,
        api_key=s.llm_api_key,
        seams=seams,
        state_repository=state_repo,
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


def close_clickhouse_client() -> None:
    if _get_clickhouse_client.cache_info().currsize == 0:
        return
    _get_clickhouse_client().close()
