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
        (tmp_path / ".env").write_text(f"CLUSTER_DOCTOR_LOG_DIR={dotenv_dir}\n", encoding="utf-8")
    env = os.environ.copy()
    env.pop("CLUSTER_DOCTOR_LOG_DIR", None)
    if env_dir is not None:
        env["CLUSTER_DOCTOR_LOG_DIR"] = env_dir
    env["PYTHONIOENCODING"] = "utf-8"
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
        encoding="utf-8",
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
    for key in ("CLUSTER_DOCTOR_LOG_DIR", "LOG_MAX_BYTES", "LOG_BACKUP_COUNT"):
        env.pop(key, None)
    env.update(env_overrides)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[3] / "src")
    env["PYTHONIOENCODING"] = "utf-8"
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
        encoding="utf-8",
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
    from cluster_doctor.bootstrap.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    for key in ("LOG_MAX_BYTES", "LOG_BACKUP_COUNT"):
        monkeypatch.delenv(key, raising=False)

    settings = LoggingSettings()

    assert settings.log_max_bytes == 10 * 1024 * 1024
    assert settings.log_backup_count == 5


@pytest.mark.parametrize("name", ["LOG_MAX_BYTES", "LOG_BACKUP_COUNT"])
def test_empty_rotation_setting_uses_the_default(tmp_path, monkeypatch, name):
    from cluster_doctor.bootstrap.settings import LoggingSettings

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

    from cluster_doctor.bootstrap.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        LoggingSettings()


def _run_entrypoint_probe(tmp_path, body):
    env = os.environ.copy()
    root = Path(__file__).resolve().parents[3]
    env["PYTHONPATH"] = os.pathsep.join((str(root / "src"), str(root / "scripts")))
    env["CLUSTER_DOCTOR_LOG_DIR"] = str(tmp_path / "logs")
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [sys.executable, "-c", body], cwd=tmp_path, env=env,
        text=True, encoding="utf-8", capture_output=True, timeout=30,
    )


@pytest.mark.parametrize("entrypoint", ["main", "sftp", "manual-dry-run"])
def test_imports_and_manual_dry_run_do_not_configure_logging(tmp_path, entrypoint):
    body = {
        "main": "import cluster_doctor.main",
        "sftp": "import upload_report_sftp",
        "manual-dry-run": (
            "import run_analysis, sys; "
            "sys.argv = ['run_analysis.py', '--at', '2026-09-16T04:22:00', '--dry-run']; "
            "assert run_analysis.main() == 0"
        ),
    }[entrypoint]
    result = _run_entrypoint_probe(
        tmp_path,
        "import logging; before = tuple(logging.getLogger().handlers); " + body + "; "
        "assert tuple(logging.getLogger().handlers) == before, 'import/dry-run changed root handlers'; "
        "assert not any(getattr(h, '_cluster_doctor_owned_handler', False) "
        "for h in logging.getLogger().handlers), 'import/dry-run installed handlers'",
    )
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "logs").exists()


@pytest.mark.parametrize("entrypoint", ["auto", "manual"])
def test_real_entrypoint_starts_logging_before_settings_and_is_idempotent(tmp_path, entrypoint):
    import textwrap

    body = textwrap.dedent('''
        import asyncio
        import logging
        import cluster_doctor.main as app
        import run_analysis as manual

        def stop_before_clients():
            logging.getLogger('entrypoint-test').warning('entrypoint-log-marker')
            raise RuntimeError('stop-before-clients')

        app.get_settings = manual.get_settings = stop_before_clients
        for _ in range(2):
            try:
                RUN_ENTRYPOINT
            except RuntimeError as exc:
                assert str(exc) == 'stop-before-clients'
        owned = [h for h in logging.getLogger().handlers
                 if getattr(h, '_cluster_doctor_owned_handler', False)]
        assert len(owned) == 2
        logging.shutdown()
    ''').replace(
        "RUN_ENTRYPOINT",
        "asyncio.run(app.main())" if entrypoint == "auto" else "manual.run([])",
    )
    result = _run_entrypoint_probe(tmp_path, body)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "logs/app.log").read_text(encoding="utf-8").count("entrypoint-log-marker") == 2
    assert result.stderr.count("entrypoint-log-marker") == 2


def test_unprefixed_log_dir_environment_variable_is_ignored(tmp_path, monkeypatch):
    from cluster_doctor.bootstrap.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CLUSTER_DOCTOR_LOG_DIR", raising=False)
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "kafka-logs"))

    assert LoggingSettings().log_dir == "logs"


def test_unprefixed_log_dir_environment_variable_does_not_override_dotenv(tmp_path, monkeypatch):
    from cluster_doctor.bootstrap.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("CLUSTER_DOCTOR_LOG_DIR=from-dotenv\n", encoding="utf-8")
    monkeypatch.delenv("CLUSTER_DOCTOR_LOG_DIR", raising=False)
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "kafka-logs"))

    assert LoggingSettings().log_dir == "from-dotenv"


def test_unprefixed_log_dir_dotenv_key_is_ignored(tmp_path, monkeypatch):
    from cluster_doctor.bootstrap.settings import LoggingSettings

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("LOG_DIR=from-dotenv\n", encoding="utf-8")
    monkeypatch.delenv("CLUSTER_DOCTOR_LOG_DIR", raising=False)
    monkeypatch.delenv("LOG_DIR", raising=False)

    assert LoggingSettings().log_dir == "logs"


def test_configure_logging_records_absolute_log_file_path(tmp_path):
    result = _run_logging(tmp_path, {}, "")

    assert result.returncode == 0, result.stderr
    log_path = tmp_path / "logs" / "app.log"
    expected = f"로그 파일: {log_path.resolve()}"
    assert expected in log_path.read_text(encoding="utf-8")
    assert expected in result.stderr
