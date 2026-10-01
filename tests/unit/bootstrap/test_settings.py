import pytest
from pydantic import ValidationError

from cluster_doctor.bootstrap.configuration.settings import Settings


@pytest.mark.parametrize("report_dir,expected", [(None, "reports"), ("", "reports"), ("custom/reports", "custom/reports"), ("/var/lib/clusterdoctor/reports", "/var/lib/clusterdoctor/reports")])
def test_report_directory_from_dotenv(tmp_path, monkeypatch, report_dir, expected):
    monkeypatch.delenv("REPORT_DIR", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("" if report_dir is None else f"REPORT_DIR={report_dir}\n", encoding="utf-8")
    settings = Settings(
        _env_file=env_file,
        llm_provider="nvidia_nim",
        nvidia_api_key="test-key",
        clickhouse_url="jdbc:clickhouse://localhost:8123/default",
        es_host="localhost",
    )
    assert settings.report_dir == expected


@pytest.mark.parametrize("timeout", [0, -1])
def test_kafka_failure_timeout_must_be_positive(timeout):
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None, kafka_failure_timeout_seconds=timeout,
            llm_provider="nvidia_nim", nvidia_api_key="test-key",
            clickhouse_url="jdbc:clickhouse://localhost:8123/default", es_host="localhost",
        )


def test_kafka_failure_timeout_from_dotenv(tmp_path, monkeypatch):
    monkeypatch.delenv("KAFKA_FAILURE_TIMEOUT_SECONDS", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("KAFKA_FAILURE_TIMEOUT_SECONDS=60\n", encoding="utf-8")
    settings = Settings(
        _env_file=env_file, llm_provider="nvidia_nim", nvidia_api_key="test-key",
        clickhouse_url="jdbc:clickhouse://localhost:8123/default", es_host="localhost",
    )
    assert settings.kafka_failure_timeout_seconds == 60
