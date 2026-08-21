from unittest.mock import MagicMock, patch

from cluster_doctor.adapter.outbound.llm.langgraph.analyzer import LangGraphAnalyzer
from cluster_doctor.adapter.outbound.llm.litellm_adapter import LiteLlmAdapter
from cluster_doctor.config import dependencies
from cluster_doctor.config import settings as settings_module
from cluster_doctor.config.dependencies import (
    _build_llm_analyzer,
    _parse_clickhouse_url,
    close_clickhouse_client,
)
from cluster_doctor.config.settings import Settings

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
    # Must not attempt to construct a client just to close it.
    close_clickhouse_client()
    assert dependencies._get_clickhouse_client.cache_info().currsize == 0


def test_close_clickhouse_client_closes_the_cached_client(monkeypatch):
    for k, v in REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    settings_module.get_settings.cache_clear()
    dependencies._get_clickhouse_client.cache_clear()
    fake_client = MagicMock()
    monkeypatch.setattr(dependencies.clickhouse_connect, "get_client", lambda **_kw: fake_client)

    dependencies._get_clickhouse_client()  # populate the cache

    close_clickhouse_client()

    fake_client.close.assert_called_once()

    dependencies._get_clickhouse_client.cache_clear()
    settings_module.get_settings.cache_clear()


def test_builds_gemini_adapter_with_gemini_credentials():
    s = _settings(
        llm_provider="gemini", gemini_api_key="g-key", gemini_model="gemini-2.5-pro"
    )
    adapter = _build_llm_analyzer(s)
    assert isinstance(adapter, LiteLlmAdapter)
    assert adapter._provider == "gemini"
    assert adapter._api_key == "g-key"
    assert adapter._default_model == "gemini-2.5-pro"


def test_builds_nvidia_adapter_with_nvidia_credentials():
    s = _settings(
        llm_provider="nvidia",
        nvidia_api_key="n-key",
        nvidia_model="meta/llama-3.3-70b-instruct",
    )
    adapter = _build_llm_analyzer(s)
    assert adapter._provider == "nvidia"
    assert adapter._api_key == "n-key"
    assert adapter._default_model == "meta/llama-3.3-70b-instruct"


def test_does_not_mix_credentials_across_providers():
    """NVIDIA를 선택했는데 Gemini 키가 어댑터로 흘러가면 안 된다."""
    s = _settings(
        llm_provider="nvidia", nvidia_api_key="n-key", gemini_api_key="g-key"
    )
    adapter = _build_llm_analyzer(s)
    assert adapter._api_key == "n-key"


def test_defaults_to_single_call_mode():
    s = _settings(gemini_api_key="g-key")
    assert s.analysis_mode == "single"
    assert isinstance(_build_llm_analyzer(s), LiteLlmAdapter)


def test_graph_mode_builds_the_langgraph_analyzer():
    s = _settings(gemini_api_key="g-key", analysis_mode="graph")
    assert isinstance(_build_llm_analyzer(s), LangGraphAnalyzer)


def test_analysis_mode_is_orthogonal_to_provider():
    """graph 모드도 선택된 provider의 자격증명을 그대로 쓴다.

    두 축을 하나로 합치려는 시도(예: LLM_PROVIDER=langgraph)를 막는 단정이다.
    """
    s = _settings(
        llm_provider="nvidia",
        nvidia_api_key="n-key",
        nvidia_model="meta/llama-3.3-70b-instruct",
        analysis_mode="graph",
    )
    adapter = _build_llm_analyzer(s)
    assert isinstance(adapter, LangGraphAnalyzer)
    assert adapter._provider == "nvidia"
    assert adapter._api_key == "n-key"
    assert adapter._default_model == "meta/llama-3.3-70b-instruct"
