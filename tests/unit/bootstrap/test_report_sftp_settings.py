import logging

import pytest
from pydantic import ValidationError

from cluster_doctor.bootstrap.configuration.settings import Settings
from cluster_doctor.bootstrap.dependency.wiring import _build_report_publisher, build_sftp_uploader
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.sftp.sftp_report_publisher import (
    SftpReportPublisher,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    HtmlFileReportPublisher,
)

_BASE = dict(
    clickhouse_url="jdbc:clickhouse://localhost:8123/db",
    es_host="es.example",
    nvidia_api_key="key",
    llm_provider="nvidia_nim",
)


def _settings(**overrides):
    return Settings(_env_file=None, **{**_BASE, **overrides})


def _sftp(**overrides):
    values = dict(
        report_sftp_host="reports.example",
        report_sftp_user="svc",
        report_sftp_password="s3cret-pw",
        report_sftp_remote_dir="/srv/reports",
    )
    values.update(overrides)
    return _settings(**values)


def test_sftp_is_disabled_by_default():
    settings = _settings()

    assert settings.report_sftp_enabled is False
    assert settings.report_sftp_port == 22
    assert settings.report_sftp_timeout_seconds == 10.0


def test_complete_sftp_settings_enable_the_upload():
    assert _sftp().report_sftp_enabled is True


def test_password_is_masked_when_settings_are_printed():
    assert "s3cret-pw" not in repr(_sftp())


def test_key_file_can_replace_the_password():
    settings = _sftp(report_sftp_password="", report_sftp_key_file="/keys/id_ed25519")

    assert settings.report_sftp_enabled is True


@pytest.mark.parametrize(
    "overrides, missing",
    [
        (dict(report_sftp_user=""), "report_sftp_user"),
        (dict(report_sftp_remote_dir=""), "report_sftp_remote_dir"),
        (dict(report_sftp_password="", report_sftp_key_file=""), "report_sftp_password"),
    ],
)
def test_enabled_sftp_requires_user_remote_dir_and_a_credential(overrides, missing):
    with pytest.raises(ValidationError) as error:
        _sftp(**overrides)

    # get_settings()가 하듯 입력 값을 빼고 본다. 모델 검증 오류의 기본 문구에는 입력 전체가 실린다.
    errors = str(error.value.errors(include_input=False, include_url=False))
    assert missing in errors
    assert "s3cret-pw" not in errors


@pytest.mark.parametrize("overrides", [dict(report_sftp_port=0), dict(report_sftp_port=70000), dict(report_sftp_timeout_seconds=0)])
def test_invalid_port_or_timeout_is_rejected(overrides):
    with pytest.raises(ValidationError):
        _sftp(**overrides)


@pytest.mark.parametrize("name", ["REPORT_SFTP_PORT", "REPORT_SFTP_TIMEOUT_SECONDS"])
def test_empty_numeric_setting_uses_the_default(monkeypatch, tmp_path, name):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(name, "")

    settings = _settings()

    assert settings.report_sftp_port == 22
    assert settings.report_sftp_timeout_seconds == 10.0


def test_disabled_sftp_builds_only_the_local_publisher(tmp_path):
    publisher = _build_report_publisher(_settings(report_dir=str(tmp_path)))

    assert isinstance(publisher, HtmlFileReportPublisher)


def test_enabled_sftp_wraps_the_local_publisher(tmp_path):
    publisher = _build_report_publisher(_sftp(report_dir=str(tmp_path)))

    assert isinstance(publisher, SftpReportPublisher)


def test_missing_known_hosts_is_called_out_once_at_startup(tmp_path, caplog):
    caplog.set_level(logging.WARNING)

    _build_report_publisher(_sftp(report_dir=str(tmp_path)))

    assert "REPORT_SFTP_KNOWN_HOSTS" in caplog.text


def test_uploader_is_built_with_the_configured_connection_details():
    uploader = build_sftp_uploader(
        _sftp(
            report_sftp_port=2222,
            report_sftp_key_file="/keys/id_ed25519",
            report_sftp_known_hosts="/etc/ssh/known_hosts",
            report_sftp_timeout_seconds=7.0,
        ),
        attempts=1,
    )

    assert uploader._target.host == "reports.example"
    assert uploader._target.port == 2222
    assert uploader._target.user == "svc"
    assert uploader._target.remote_dir == "/srv/reports"
    assert uploader._target.key_file == "/keys/id_ed25519"
    assert uploader._target.known_hosts == "/etc/ssh/known_hosts"
    assert uploader._target.timeout_seconds == 7.0
    assert uploader._attempts == 1


def test_known_hosts_setting_silences_the_startup_warning(tmp_path, caplog):
    caplog.set_level(logging.WARNING)

    _build_report_publisher(_sftp(report_dir=str(tmp_path), report_sftp_known_hosts="/etc/ssh/known_hosts"))

    assert "REPORT_SFTP_KNOWN_HOSTS" not in caplog.text
