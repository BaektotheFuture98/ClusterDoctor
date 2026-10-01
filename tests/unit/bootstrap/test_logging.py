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
