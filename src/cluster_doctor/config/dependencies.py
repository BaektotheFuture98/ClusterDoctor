from functools import lru_cache
from urllib.parse import urlparse

import clickhouse_connect

from cluster_doctor.adapter.outbound.clickhouse.clickhouse_log_adapter import ClickHouseLogAdapter
from cluster_doctor.adapter.outbound.llm.langgraph.analyzer import LangGraphAnalyzer
from cluster_doctor.adapter.outbound.llm.litellm_adapter import LiteLlmAdapter
from cluster_doctor.config.settings import get_settings
from cluster_doctor.domain.port.inbound.diagnosis_use_case import DiagnosisUseCase
from cluster_doctor.domain.port.outbound.llm_analyzer import LlmAnalyzer
from cluster_doctor.domain.service.diagnosis_service import DiagnosisService

_DEFAULT_CLICKHOUSE_PORT = 8123
_DEFAULT_DATABASE        = "default"
# Without an explicit timeout, clickhouse-connect's HTTP client can block a
# worker thread indefinitely if ClickHouse hangs. 30s comfortably covers a
# healthy one-minute-segment, single-source, LIMIT-bounded query (see
# _MAX_ROWS_PER_SEGMENT_PER_SOURCE in the adapter) while still failing fast
# enough that a stuck backend cannot pin a worker forever.
_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS = 30


def _parse_clickhouse_url(jdbc_url: str) -> tuple[str, int, str]:
    parsed = urlparse(jdbc_url.replace("jdbc:", "", 1))
    host   = parsed.hostname or "localhost"
    port   = parsed.port or _DEFAULT_CLICKHOUSE_PORT
    db     = (parsed.path or "").lstrip("/") or _DEFAULT_DATABASE
    return host, port, db


@lru_cache
def _get_clickhouse_client():
    s              = get_settings()
    host, port, db = _parse_clickhouse_url(s.clickhouse_url)
    return clickhouse_connect.get_client(
        host=host, port=port, database=db,
        username=s.clickhouse_user, password=s.clickhouse_password,
        send_receive_timeout=_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS,
    )


@lru_cache
def _get_log_repository() -> ClickHouseLogAdapter:
    s = get_settings()
    return ClickHouseLogAdapter(
        client=_get_clickhouse_client(),
        slowlog_table=s.clickhouse_slowlog_table,
        log_table=s.clickhouse_log_table,
        node_metric_table=s.clickhouse_node_metric_table,
    )


_PROVIDER_CREDENTIALS: dict[str, tuple[str, str]] = {
    # provider -> (api_key 필드, 기본 모델 필드)
    "gemini": ("gemini_api_key", "gemini_model"),
    "nvidia": ("nvidia_api_key", "nvidia_model"),
}

# analysis_mode -> 어댑터 클래스. 둘 다 LlmAnalyzer 구현체라 생성자
# 시그니처가 같으므로 조립부는 어느 쪽인지 신경 쓰지 않는다.
_ANALYZERS: dict[str, type[LlmAnalyzer]] = {
    "single": LiteLlmAdapter,
    "graph": LangGraphAnalyzer,
}


def _build_llm_analyzer(settings) -> LlmAnalyzer:
    key_field, model_field = _PROVIDER_CREDENTIALS[settings.llm_provider]
    analyzer_cls = _ANALYZERS[settings.analysis_mode]
    return analyzer_cls(
        provider=settings.llm_provider,
        api_key=getattr(settings, key_field),
        default_model=getattr(settings, model_field),
    )


@lru_cache
def _get_llm_analyzer() -> LlmAnalyzer:
    return _build_llm_analyzer(get_settings())


def get_diagnosis_use_case() -> DiagnosisUseCase:
    return DiagnosisService(
        log_repository=_get_log_repository(),
        llm_analyzer=_get_llm_analyzer(),
    )


def close_clickhouse_client() -> None:
    """Close the cached ClickHouse client on shutdown, if one exists.

    ``_get_clickhouse_client`` is only ever populated once a request has
    actually needed it (the test suite overrides the use-case dependency and
    never touches this cache), so this checks the cache before touching it
    rather than calling the factory -- which would construct a client just
    to close it.
    """
    if _get_clickhouse_client.cache_info().currsize == 0:
        return
    _get_clickhouse_client().close()
