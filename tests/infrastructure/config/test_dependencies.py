from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.domain.model.log_entry import SlowlogEntry
from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import DeepAgentAnalyzer
from cluster_doctor.infrastructure.config import dependencies
from cluster_doctor.infrastructure.config import settings as settings_module
from cluster_doctor.infrastructure.config.dependencies import (
    _parse_clickhouse_url,
    close_clickhouse_client,
)
from cluster_doctor.infrastructure.config.settings import Settings

REQUIRED_ENV = {
    "GEMINI_API_KEY": "test-key",
    "CLICKHOUSE_URL": "jdbc:clickhouse://localhost:8123/default",
}

BASE_ENV = {
    "clickhouse_url": "jdbc:clickhouse://localhost:8123/default",
}


def _settings(**overrides) -> Settings:
    with patch.dict("os.environ", {}, clear=True):
        return Settings(_env_file=None, **{**BASE_ENV, **overrides})


def test_parses_host_port_db():
    assert _parse_clickhouse_url("jdbc:clickhouse://ch.example.com:8123/mydb") == (
        "ch.example.com", 8123, "mydb"
    )


def test_defaults_port_and_db_when_absent():
    assert _parse_clickhouse_url("jdbc:clickhouse://ch.example.com") == (
        "ch.example.com", 8123, "default"
    )


def test_handles_url_without_jdbc_prefix():
    assert _parse_clickhouse_url("clickhouse://localhost:9000/logs") == (
        "localhost", 9000, "logs"
    )


def test_close_clickhouse_client_is_a_noop_when_none_was_created():
    dependencies._get_clickhouse_client.cache_clear()
    close_clickhouse_client()
    assert dependencies._get_clickhouse_client.cache_info().currsize == 0


def test_close_clickhouse_client_closes_the_cached_client(monkeypatch):
    for k, v in REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    settings_module.get_settings.cache_clear()
    dependencies._get_clickhouse_client.cache_clear()
    fake_client = MagicMock()
    monkeypatch.setattr(dependencies.clickhouse_connect, "get_client", lambda **_kw: fake_client)

    dependencies._get_clickhouse_client()

    close_clickhouse_client()

    fake_client.close.assert_called_once()

    dependencies._get_clickhouse_client.cache_clear()
    settings_module.get_settings.cache_clear()


def test_build_trigger_service_wires_deepagent_and_shares_queue(monkeypatch):
    """analyzer 조립과 pending 큐 공유를 함께 검증한다.

    ``_get_es_client`` / ``_get_log_repository``를 팩토리 단위로 교체한다.
    ``clickhouse_connect.get_client``는 생성 시점에 실제로 접속을 시도하고
    (도달 불가 호스트에서 20초 후 실패), ``Elasticsearch(hosts=[])``는
    ``ValueError``를 던진다. 게다가 ``get_settings()``는 ``.env`` *파일*을 읽어
    개발자 환경에 따라 결과가 달라진다. 팩토리를 교체하면 이 세 위험이 모두
    사라지고 lru_cache도 오염되지 않는다.

    pending 큐 공유는 이 조립 함수에서 가장 깨지기 쉬운 계약이다 — 서비스가
    넣은 항목을 analyzer의 drain 클로저가 꺼낼 수 있어야 한다.
    """
    monkeypatch.setenv("GEMINI_API_KEY", "g-key")
    monkeypatch.setenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default")
    settings_module.get_settings.cache_clear()
    monkeypatch.setattr(dependencies, "_get_es_client", lambda: MagicMock())
    monkeypatch.setattr(dependencies, "_get_log_repository", lambda: MagicMock())
    # _get_cluster_repository는 patch 대상이 아니라 실제로 실행된다(포트 조립을
    # 검증하려면 그래야 한다). lru_cache가 이 테스트의 가짜 ES 클라이언트를
    # 물고 남으면 이후 호출자가 그것을 돌려받는다.
    dependencies._get_cluster_repository.cache_clear()

    try:
        service = dependencies.build_trigger_service()

        analyzer = service._llm_analyzer
        assert isinstance(analyzer, DeepAgentAnalyzer)
        assert analyzer._api_key == "g-key"

        # analyzer는 raw Elasticsearch 클라이언트가 아니라 ClusterRepository
        # 포트를 받아야 한다. 포트와 어댑터가 정의만 되어 있고 조립되지 않으면
        # ES 호출이 포트를 우회하고 어댑터는 죽은 코드로 남는다.
        assert isinstance(analyzer._cluster, ClusterRepository)

        entry = SlowlogEntry(timestamp=datetime.now(timezone.utc))
        service._pending.put(entry)
        assert analyzer._drain_pending() == [entry]
    finally:
        dependencies._get_cluster_repository.cache_clear()
        settings_module.get_settings.cache_clear()
