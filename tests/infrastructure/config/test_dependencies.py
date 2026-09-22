"""Composition Root. 어떤 구현이 어디에 물리는가.

리팩터링으로 조립 모양이 바뀌었다. 예전에는 ``IncidentOrchestrator``가
Supervisor와 진단 Agent를 **둘 다** 직접 들고 있었다. 지금 Runner가 아는 것은
``IncidentAgent`` 하나이고, 진단 Agent는 그 뒤에 숨는다.

    SlowlogTriggerService
        └ IncidentRunner
            └ DeepAgentIncidentAdapter   (Main DeepAgent)
                └ DiagnosisSeams         (진단 SubAgent가 도구로 내놓는 조각들)

이 파일이 지키는 것은 그 사슬이 실제로 이어져 있는가와, ``LLM_PROVIDER``가
**양쪽 끝까지** 도달하는가다. 한쪽만 배선되면 판단은 A 모델이, 분석은 B 모델이
하게 되고 그 상태는 어디에도 드러나지 않는다.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from cluster_doctor.agent.integrations.elasticsearch.ports import ClusterRepository
from cluster_doctor.incident.runner import IncidentRunner
from cluster_doctor.agent.integrations.clickhouse.models import SlowlogEntry
from cluster_doctor.infrastructure.config import dependencies
from cluster_doctor.infrastructure.config import settings as settings_module
from cluster_doctor.infrastructure.config.dependencies import (
    _parse_clickhouse_url,
    close_clickhouse_client,
)
from cluster_doctor.infrastructure.config.settings import Settings
from cluster_doctor.agent.diagnosis.subagent import (
    DiagnosisSeams,
)
from cluster_doctor.agent.supervisor.agent import (
    DeepAgentIncidentAdapter,
)

REQUIRED_ENV = {
    "GEMINI_API_KEY": "test-key",
    # 기본 provider가 nvidia_nim이라 이 키가 없으면 Settings 자체가 서지 않는다.
    "NVIDIA_API_KEY": "nv-test-key",
    "CLICKHOUSE_URL": "jdbc:clickhouse://localhost:8123/default",
    "ES_HOST": "es.example.com",
}

BASE_ENV = {
    "clickhouse_url": "jdbc:clickhouse://localhost:8123/default",
    "es_host": "es.example.com",
    "nvidia_api_key": "nv-key",
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


def _build(settings, monkeypatch):
    """외부 접속을 만드는 팩토리만 교체하고 나머지는 실제로 조립한다.

    ``clickhouse_connect.get_client``는 생성 시점에 실제로 접속을 시도하고
    (도달 불가 호스트에서 20초 후 실패), ``Elasticsearch(hosts=[])``는
    ``ValueError``를 던진다. 팩토리를 교체하면 두 위험이 사라지고 lru_cache도
    오염되지 않는다.
    """
    monkeypatch.setattr(dependencies, "_get_es_client", lambda: MagicMock())
    monkeypatch.setattr(dependencies, "_get_log_repository", lambda: MagicMock())
    dependencies._get_cluster_repository.cache_clear()
    dependencies._get_node_resolver.cache_clear()
    dependencies._get_state_repository.cache_clear()
    dependencies._get_artifact_store.cache_clear()
    return dependencies.build_trigger_service(settings)


def _cleanup():
    dependencies._get_cluster_repository.cache_clear()
    dependencies._get_node_resolver.cache_clear()
    dependencies._get_state_repository.cache_clear()
    dependencies._get_artifact_store.cache_clear()


def test_selected_provider_reaches_both_agents(monkeypatch):
    """provider 선택이 두 Agent까지 도달해야 .env 설정이 의미를 갖는다.

    이 조립 함수가 s.gemini_api_key/s.gemini_model을 직접 읽으면 .env에 NVIDIA
    설정을 넣어도 Gemini로 가고, extra="ignore" 때문에 경고조차 없다 —
    "문서화된 설정이 조용히 무시된다"는 실측 사고와 같은 모양이다.

    Main DeepAgent와 진단 Agent 둘 다 본다. 둘은 서로 다른 LLM 경로를 쓴다 —
    한쪽은 ``ChatLiteLLM``(tool loop), 다른 쪽은 ``litellm_client.complete``
    (구조화 출력 한 번). 경로가 갈라진 만큼 한쪽만 배선되기도 쉽다.
    """
    try:
        service = _build(
            _settings(
                llm_provider="nvidia_nim",
                nvidia_api_key="nv-key",
                nvidia_model="google/gemma-4-31b-it",
                gemini_api_key="g-key",
            ),
            monkeypatch,
        )
        incident_agent = service._runner._agent

        # 진단 Agent: provider/model/key가 부분 적용된 호출자에 묶여 있다.
        bound = incident_agent._seams.call_llm
        assert bound.keywords["provider"] == "nvidia_nim"
        assert bound.keywords["model"] == "google/gemma-4-31b-it"
        # 고르지 않은 provider의 키가 실려서는 안 된다. 둘 다 설정돼 있을 때
        # 엉뚱한 쪽을 집으면 401이 날 뿐 원인이 드러나지 않는다.
        assert bound.keywords["api_key"] == "nv-key"

        # Main DeepAgent: litellm 문자열 규약 위의 chat model.
        assert incident_agent._model.model == "nvidia_nim/google/gemma-4-31b-it"
        assert incident_agent._model.api_key == "nv-key"
    finally:
        _cleanup()


def test_build_trigger_service_wires_the_graph_and_shares_queue(monkeypatch):
    """조립 사슬과 pending 큐 공유를 함께 검증한다.

    ``Settings``는 ``build_trigger_service``에 명시적으로 넘긴다. 인자를 비우면
    ``get_settings()``가 ``.env`` *파일*을 읽어 개발자 환경에 따라 결과가
    달라진다 — 실제로 ``.env``에 ``LLM_PROVIDER``를 넣자 이 테스트가 깨졌고,
    단정 실패 메시지에 개발자의 진짜 API 키가 그대로 출력됐다. 환경변수를
    monkeypatch하는 것만으로는 ``.env``를 덮지 못한다.

    pending 큐 공유는 이 조립 함수에서 가장 깨지기 쉬운 계약이다 — 서비스가
    넣은 항목을 Runner의 drain 클로저가 꺼낼 수 있어야 한다. 그 클로저가
    유입 정착을 판정하는 유일한 입구다.
    """
    try:
        service = _build(_settings(micro_batch_seconds=2.5), monkeypatch)
        runner = service._runner

        assert isinstance(runner, IncidentRunner)
        # Runner가 아는 것은 포트 하나뿐이다. 진단 Agent는 그 뒤에 있다.
        assert isinstance(runner._agent, DeepAgentIncidentAdapter)
        assert isinstance(runner._agent._seams, DiagnosisSeams)

        # SubAgent는 raw Elasticsearch 클라이언트가 아니라 포트를 받아야 한다.
        # 포트와 어댑터가 정의만 되어 있고 조립되지 않으면 ES 호출이 포트를
        # 우회하고 어댑터는 죽은 코드로 남는다.
        assert isinstance(runner._agent._seams.cluster, ClusterRepository)
        assert hasattr(runner._agent._seams.node_resolver, "resolve")

        # 상태와 산출물은 포트를 거쳐 오간다. application 코드가 dict에 직접
        # 접근하면 Redis 구현으로 바꿀 수 없다.
        assert runner._store is runner._agent._seams.store
        # 리포트를 쓰는 쪽도 같은 저장소를 본다. 갈라지면 앞선 리포트 요약이
        # 영영 비어 있고, 그 사실은 어디에도 드러나지 않는다.
        assert runner._store is runner._agent._seams.report_writer._store
        # State 저장소는 Runner와 Agent가 **같은 것**을 봐야 한다. Agent가
        # 예산을 차감한 State를 Runner가 다시 읽는 것이 종료 판단의 전제다.
        assert runner._states is runner._agent._states

        entry = SlowlogEntry(timestamp=datetime.now(timezone.utc))
        service._pending.put(entry)
        assert runner._drain_pending() == [entry]

        # MICRO_BATCH_SECONDS를 승격한 목적 자체가 "문서화된 설정값이 실제로는
        # 무시된다"는 사고를 막는 것이었다.
        assert service._micro_batch_seconds == 2.5
    finally:
        _cleanup()
