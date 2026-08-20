from functools import lru_cache
from urllib.parse import urlparse

import clickhouse_connect

from cluster_doctor.adapter.outbound.clickhouse.clickhouse_log_adapter import ClickHouseLogAdapter
from cluster_doctor.adapter.outbound.llm.gemini_llm_adapter import GeminiLlmAdapter
from cluster_doctor.config.settings import get_settings
from cluster_doctor.domain.port.inbound.diagnosis_use_case import DiagnosisUseCase
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


@lru_cache
def _get_llm_analyzer() -> GeminiLlmAdapter:
    s = get_settings()
    return GeminiLlmAdapter(
        api_key=s.gemini_api_key,
        base_url=s.gemini_base_url,
        default_model=s.gemini_model,
    )


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
