import traceback

import pytest
from pydantic import BaseModel, ValidationError

from cluster_doctor.infrastructure.config import settings as settings_module
from cluster_doctor.infrastructure.config.settings import ConfigurationError, Settings, get_settings

REQUIRED = {
    "GEMINI_API_KEY":  "test-key",
    "CLICKHOUSE_URL":  "jdbc:clickhouse://localhost:8123/default",
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
    assert s.gemini_model                 == "gemini-2.5-flash"
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


def test_missing_gemini_key_is_rejected(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", REQUIRED["CLICKHOUSE_URL"])
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(ValidationError) as excinfo:
        Settings(_env_file=None)
    assert "gemini_api_key" in str(excinfo.value)


def test_key_error_does_not_echo_other_secrets(monkeypatch):
    """검증 실패 메시지에 다른 비밀값이 실려서는 안 된다."""
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

    이전에는 main.py의 lifespan에만 있어서, Settings()나 get_settings()를
    직접 부르는 스크립트·테스트 헬퍼는 그대로 raw ValidationError를 받았고
    거기에 실린 API 키가 출력됐다(실제로 발생).
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
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    monkeypatch.setenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default")
    assert Settings(_env_file=None).micro_batch_seconds == 10.0


def test_micro_batch_seconds_is_configurable(monkeypatch):
    # .env에 적어둔 값이 실제로 동작에 반영돼야 한다. 예전에는
    # FLUSH_INTERVAL_SECONDS가 문서에만 있고 코드에는 없어, 설정해도
    # 하드코딩된 10초로 동작하면서 아무 경고가 없었다.
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    monkeypatch.setenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default")
    monkeypatch.setenv("MICRO_BATCH_SECONDS", "30")
    assert Settings(_env_file=None).micro_batch_seconds == 30.0
