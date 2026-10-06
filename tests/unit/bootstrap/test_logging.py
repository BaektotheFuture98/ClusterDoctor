import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "dotenv_dir,env_dir,expected_dir",
    [
        (None, None, "logs"),
        ("custom/from-dotenv", None, "custom/from-dotenv"),
        ("custom/from-dotenv", "custom/from-environment", "custom/from-environment"),
        ("", None, "logs"),
        ("custom/from-dotenv", "", "logs"),
        ("absolute", None, "absolute"),
    ],
)
def test_log_directory_configuration(tmp_path, dotenv_dir, env_dir, expected_dir):
    if dotenv_dir == "absolute":
        dotenv_dir = str(tmp_path / "absolute")
    if dotenv_dir is not None:
        (tmp_path / ".env").write_text(f"LOG_DIR={dotenv_dir}\n", encoding="utf-8")
    env = os.environ.copy()
    env.pop("LOG_DIR", None)
    if env_dir is not None:
        env["LOG_DIR"] = env_dir
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[3] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from cluster_doctor.main import configure_logging; "
            "import logging; configure_logging(); "
            "logging.getLogger('logging-test').warning('test-log-marker')",
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    path = tmp_path / expected_dir / "app.log"
    assert path.is_file()
    assert path.read_text(encoding="utf-8").count("test-log-marker") == 1
    assert result.stderr.count("test-log-marker") == 1
    if expected_dir != "logs":
        assert not (tmp_path / "logs").exists()


def _run_logging(tmp_path, env_overrides, body):
    env = os.environ.copy()
    for key in ("LOG_DIR", "LOG_MAX_BYTES", "LOG_BACKUP_COUNT"):
        env.pop(key, None)
    env.update(env_overrides)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[3] / "src")
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "from cluster_doctor.main import configure_logging; "
            "import logging; configure_logging(); " + body,
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )


def test_log_file_rotates_and_keeps_only_the_configured_backups(tmp_path):
    result = _run_logging(
        tmp_path,
        {"LOG_MAX_BYTES": "1000", "LOG_BACKUP_COUNT": "2"},
        "[logging.getLogger('rotation-test').warning('x' * 200) for _ in range(40)]",
    )

    assert result.returncode == 0, result.stderr
    logs = tmp_path / "logs"
    assert (logs / "app.log").is_file()
    assert (logs / "app.log.1").is_file()
    assert (logs / "app.log.2").is_file()
    assert not (logs / "app.log.3").exists()
    assert all(path.stat().st_size <= 1200 for path in logs.iterdir())


def test_log_rotation_defaults_to_ten_megabytes_and_five_backups(tmp_path, monkeypatch):
    from cluster_doctor.bootstrap.configuration.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    for key in ("LOG_MAX_BYTES", "LOG_BACKUP_COUNT"):
        monkeypatch.delenv(key, raising=False)

    settings = LoggingSettings()

    assert settings.log_max_bytes == 10 * 1024 * 1024
    assert settings.log_backup_count == 5


@pytest.mark.parametrize("name", ["LOG_MAX_BYTES", "LOG_BACKUP_COUNT"])
def test_empty_rotation_setting_uses_the_default(tmp_path, monkeypatch, name):
    from cluster_doctor.bootstrap.configuration.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(name, "")

    settings = LoggingSettings()

    assert settings.log_max_bytes == 10 * 1024 * 1024
    assert settings.log_backup_count == 5


@pytest.mark.parametrize(
    "name, value", [("LOG_MAX_BYTES", "0"), ("LOG_MAX_BYTES", "-1"), ("LOG_BACKUP_COUNT", "0")]
)
def test_non_positive_rotation_setting_is_rejected(tmp_path, monkeypatch, name, value):
    from pydantic import ValidationError

    from cluster_doctor.bootstrap.configuration.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        LoggingSettings()
