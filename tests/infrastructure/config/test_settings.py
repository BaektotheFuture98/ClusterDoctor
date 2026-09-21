import traceback
from pathlib import Path

import pytest
from dotenv import dotenv_values
from pydantic import BaseModel, ValidationError

from cluster_doctor.infrastructure.config import settings as settings_module
from cluster_doctor.infrastructure.config.settings import ConfigurationError, Settings, get_settings

# 기본 provider가 nvidia_nim이므로 NVIDIA_API_KEY가 없으면 어떤 Settings도
# 세워지지 않는다. 여기 빠져 있으면 provider와 무관한 테스트까지 "필수 키
# 없음"으로 떨어져, 무엇을 검증하려던 테스트였는지 실패 메시지가 말해 주지
# 못한다.
REQUIRED = {
    "GEMINI_API_KEY":  "test-key",
    "NVIDIA_API_KEY":  "nv-test-key",
    "CLICKHOUSE_URL":  "jdbc:clickhouse://localhost:8123/default",
    "ES_HOST":         "es.example.com",
}

CANARY = "CanaryPassword_7fQ2"


def test_defaults_applied(monkeypatch):
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("CLICKHOUSE_PASSWORD", raising=False)
    monkeypatch.delenv("CLICKHOUSE_USER", raising=False)
    monkeypatch.delenv("CLICKHOUSE_SLOWLOG_TABLE", raising=False)
    monkeypatch.delenv("CLICKHOUSE_LOG_TABLE", raising=False)
    monkeypatch.delenv("CLICKHOUSE_NODE_METRIC_TABLE", raising=False)
    s = Settings(_env_file=None)
    assert s.gemini_model                 == "gemini-3.5-flash-lite"
    assert s.clickhouse_user              == "default"
    assert s.clickhouse_password          == ""
    assert s.clickhouse_slowlog_table     == "slowlog_v2"
    assert s.clickhouse_log_table         == "log"
    assert s.clickhouse_node_metric_table == "es_node_metric"



def test_env_overrides_defaults(monkeypatch):
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-pro")
    monkeypatch.setenv("CLICKHOUSE_SLOWLOG_TABLE", "slowlog_v3")
    s = Settings(_env_file=None)
    assert s.gemini_model             == "gemini-2.5-pro"
    assert s.clickhouse_slowlog_table == "slowlog_v3"


def test_required_fields_loaded(monkeypatch):
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    s = Settings(_env_file=None)
    assert s.gemini_api_key == "test-key"
    assert s.clickhouse_url == "jdbc:clickhouse://localhost:8123/default"


def test_provider_defaults_to_nvidia_nim(monkeypatch):
    """provider를 고르지 않았을 때 무엇이 쓰이는지 못 박는다.

    기본값이 바뀌면 .env를 그대로 둔 운영자의 호출 대상이 통째로 바뀐다.
    키와 모델이 그 기본값을 따라가는지까지 함께 본다 — 한쪽만 따라가면
    A provider에 B의 키를 보내고 401만 남는다.
    """
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    s = Settings(_env_file=None)
    assert s.llm_provider == "nvidia_nim"
    assert s.llm_api_key == s.nvidia_api_key
    assert s.llm_model == s.nvidia_model


def test_selecting_nvidia_switches_key_and_model(monkeypatch):
    """provider를 바꾸면 키와 모델이 함께 그쪽으로 간다.

    호출부가 provider를 분기하지 않게 하려고 프로퍼티로 묶었다. 조립부가
    s.gemini_api_key를 직접 읽으면 .env에 NVIDIA 설정을 넣어도 Gemini로 간다.
    """
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("LLM_PROVIDER", "nvidia_nim")
    monkeypatch.setenv("NVIDIA_API_KEY", "nv-key")
    monkeypatch.setenv("NVIDIA_MODEL", "google/gemma-4-31b-it")
    s = Settings(_env_file=None)
    assert s.llm_api_key == "nv-key"
    assert s.llm_model == "google/gemma-4-31b-it"


def test_nvidia_selected_without_its_key_is_rejected(monkeypatch):
    """쓰는 키가 비어 있으면 기동을 막는다.

    provider와 무관하게 한쪽 키만 요구하면 정작 쓰는 키가 비어도 통과해,
    첫 진단 요청을 통째로 날린 뒤에야 알게 된다.
    """
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("LLM_PROVIDER", "nvidia_nim")
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)
    assert "nvidia_api_key" in str(excinfo.value)


def test_gemini_key_is_not_required_when_nvidia_is_selected(monkeypatch):
    """쓰지 않는 provider의 키를 강제하지 않는다."""
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("LLM_PROVIDER", "nvidia_nim")
    monkeypatch.setenv("NVIDIA_API_KEY", "nv-key")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    Settings(_env_file=None)


def test_unknown_provider_is_rejected(monkeypatch):
    """오타를 첫 호출까지 끌고 가지 않는다."""
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)
    assert "llm_provider" in str(excinfo.value)


def test_provider_key_error_does_not_echo_the_other_providers_key(monkeypatch):
    """검증 실패 메시지에 다른 provider의 키가 실려서는 안 된다.

    필드 단위 검증기를 유지하는 이유다. model_validator(mode="after")는
    ValidationError에 모델 전체 dict를 실어 나른다.
    """
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("LLM_PROVIDER", "nvidia_nim")
    monkeypatch.setenv("GEMINI_API_KEY", CANARY)
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)
    assert CANARY not in str(excinfo.value)


def test_empty_cluster_name_is_rejected(monkeypatch):
    # 기본값이 있으므로 비우려면 명시해야 한다. 그래도 막는 이유는 빈 이름이
    # 리포트와 로그에서 어느 클러스터를 본 것인지 지우기 때문이고, 기동이
    # 실패하는 편이 Incident 한 건을 태운 뒤 알게 되는 것보다 낫다.
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("CLUSTER_NAME", "   ")
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)
    assert "cluster_name" in str(excinfo.value)


def test_cluster_name_has_a_usable_default(monkeypatch):
    # 검증기를 넣으면서 기본값까지 막지 않았는지 고정한다.
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CLUSTER_NAME", raising=False)
    assert Settings(_env_file=None).cluster_name == "elasticsearch"


def test_missing_gemini_key_is_rejected(monkeypatch):
    # gemini를 고른 경우에만 gemini 키가 필수다. provider를 명시하지 않으면
    # 기본값(nvidia_nim)이 적용돼 이 검증기가 아예 돌지 않는다.
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("CLICKHOUSE_URL", REQUIRED["CLICKHOUSE_URL"])
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)
    assert "gemini_api_key" in str(excinfo.value)


def test_key_error_does_not_echo_other_secrets(monkeypatch):
    """검증 실패 메시지에 다른 비밀값이 실려서는 안 된다."""
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("CLICKHOUSE_URL", REQUIRED["CLICKHOUSE_URL"])
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", CANARY)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)
    assert CANARY not in str(excinfo.value)


class _SettingsProbe(BaseModel):
    """``Settings``와 같은 실패 모양: 필수 필드가 없고, 비밀값 필드는 채워져 있다.

    실제 ``Settings``를 쓰지 않는 이유는 ``get_settings()``가 ``.env``를
    읽기 때문이다. 개발자의 ``.env`` 내용에 따라 결과가 달라지면 테스트가
    아니라 환경 점검이 된다.
    """

    gemini_api_key: str
    clickhouse_password: str = ""


def _raise_settings_validation_error(*_args, **_kwargs):
    _SettingsProbe(clickhouse_password=CANARY)
    raise AssertionError("probe was expected to raise ValidationError")


@pytest.fixture
def broken_settings(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setattr(settings_module, "Settings", _raise_settings_validation_error)
    yield
    get_settings.cache_clear()


def test_get_settings_converts_validation_failure_to_configuration_error(broken_settings):
    """가드는 부팅 경로가 아니라 get_settings 안에 있어야 한다.

    가드가 main.py의 부팅 경로에만 있으면, Settings()나 get_settings()를 직접
    부르는 스크립트·테스트 헬퍼는 raw ValidationError를 그대로 받고 거기에
    실린 API 키가 출력된다(실측으로 발생).
    """
    with pytest.raises(ConfigurationError) as excinfo:
        get_settings()

    message = str(excinfo.value)
    assert "gemini_api_key" in message, "운영자는 어느 설정이 문제인지 알아야 한다"
    assert CANARY not in message
    assert "input_value" not in message


def test_get_settings_failure_does_not_leak_the_secret_through_the_traceback(broken_settings):
    """uvicorn이 stderr에 쓰는 것은 체인을 포함한 전체 트레이스백이다."""
    with pytest.raises(ConfigurationError) as excinfo:
        get_settings()

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__ is True
    formatted = "".join(traceback.format_exception(excinfo.value))
    assert CANARY not in formatted


def test_get_settings_does_not_cache_a_failed_configuration(broken_settings):
    """lru_cache는 예외를 기억하지 않는다 - 고치면 재기동 없이 반영된다."""
    with pytest.raises(ConfigurationError):
        get_settings()
    with pytest.raises(ConfigurationError):
        get_settings()
    assert get_settings.cache_info().currsize == 0


def test_micro_batch_seconds_defaults_to_ten(monkeypatch):
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    assert Settings(_env_file=None).micro_batch_seconds == 10.0


def test_env_example_documents_only_settings_the_code_reads():
    # 이 프로젝트에서 실제로 났던 사고다. .env.example이 FLUSH_INTERVAL_SECONDS
    # 등 네 개를 문서화했지만 Settings에는 없었고, extra="ignore" 때문에 조용히
    # 버려졌다. 운영자가 값을 넣어도 하드코딩된 기본값으로 돌았고 경고도 없었다.
    # 문서와 코드가 다시 갈라지면 여기서 걸린다.
    env_example = Path(__file__).resolve().parents[3] / ".env.example"
    # dotenv_values는 없는 경로에 대해 예외 없이 빈 dict을 돌려준다. 경로
    # 계산이 깨지면 undeclared가 공집합이 되어, 아무것도 검사하지 않은 채
    # 이 가드가 통과한다 — 이 테스트가 막으려던 바로 그 조용한 실패다.
    assert env_example.exists(), f".env.example를 찾지 못했다: {env_example}"
    declared = {name.upper() for name in Settings.model_fields}
    documented = {key.upper() for key in dotenv_values(env_example)}

    # litellm이 import 시점에 os.environ에서 직접 읽는 값이라 Settings 필드가
    # 없는 것이 정상이다.
    undeclared = documented - declared - {"LITELLM_LOCAL_MODEL_COST_MAP"}
    assert undeclared == set(), f".env.example에만 있고 코드가 읽지 않는 설정: {sorted(undeclared)}"


def test_micro_batch_seconds_is_configurable(monkeypatch):
    # .env에 적어둔 값이 실제로 동작에 반영돼야 한다. 실측으로
    # FLUSH_INTERVAL_SECONDS가 문서에만 있고 코드에는 없어, 설정해도
    # 하드코딩된 10초로 동작하면서 아무 경고가 없었다.
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("MICRO_BATCH_SECONDS", "30")
    assert Settings(_env_file=None).micro_batch_seconds == 30.0


def test_missing_es_host_is_rejected_at_startup(monkeypatch):
    # agent의 첫 단계가 cluster_health()라 ES는 이제 필수다. 비어 있으면
    # Elasticsearch(hosts=[])가 조립 시점에 ValueError를 던지는데, 그것은
    # 어떤 설정이 문제인지 알려주지 않는다.
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("ES_HOST", "")
    settings_module.get_settings.cache_clear()
    try:
        with pytest.raises(ConfigurationError, match="es_host"):
            settings_module.get_settings()
    finally:
        settings_module.get_settings.cache_clear()
