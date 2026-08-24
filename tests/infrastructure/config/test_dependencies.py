from unittest.mock import MagicMock, patch

from cluster_doctor.infrastructure.outbound.llm.langgraph.analyzer import LangGraphAnalyzer
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


def test_always_builds_langgraph_analyzer(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "g-key")
    monkeypatch.setenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default")
    settings_module.get_settings.cache_clear()
    dependencies._get_llm_analyzer.cache_clear()

    analyzer = dependencies._get_llm_analyzer("gemini")

    assert isinstance(analyzer, LangGraphAnalyzer)
    assert analyzer._provider == "gemini"
    assert analyzer._api_key == "g-key"

    dependencies._get_llm_analyzer.cache_clear()
    settings_module.get_settings.cache_clear()


def test_does_not_mix_credentials_across_providers(monkeypatch):
    monkeypatch.setenv("NVIDIA_API_KEY", "n-key")
    monkeypatch.setenv("GEMINI_API_KEY", "g-key")
    monkeypatch.setenv("LLM_PROVIDER", "nvidia")
    monkeypatch.setenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default")
    settings_module.get_settings.cache_clear()
    dependencies._get_llm_analyzer.cache_clear()

    analyzer = dependencies._get_llm_analyzer("nvidia")

    assert analyzer._api_key == "n-key"

    dependencies._get_llm_analyzer.cache_clear()
    settings_module.get_settings.cache_clear()
