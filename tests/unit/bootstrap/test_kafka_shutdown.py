import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest


def test_kafka_failure_exits_process_even_with_running_analysis_thread(tmp_path):
    script = textwrap.dedent("""
        import asyncio
        import threading
        from contextlib import nullcontext
        from types import SimpleNamespace
        from cluster_doctor import main as app
        from cluster_doctor.exceptions import KafkaUnavailableError

        class FakeProblemLogProcessor:
            async def close(self):
                await asyncio.Event().wait()

        class Consumer:
            async def run(self):
                threading.Thread(target=threading.Event().wait, daemon=False).start()
                raise KafkaUnavailableError("Kafka connection failed continuously")

        app.get_settings = lambda: SimpleNamespace(cluster_name="c", kafka_bootstrap_servers="k", kafka_topic="t", kafka_group_id="g", kafka_failure_timeout_seconds=1)
        app.build_runtime_resources = lambda settings: nullcontext()
        app.compose_analysis_service = lambda settings, resources: object()
        app.ProblemLogProcessor = lambda **kwargs: FakeProblemLogProcessor()
        app.KafkaConsumerAdapter = lambda **kwargs: Consumer()
        asyncio.run(app.main())
    """)
    env = os.environ.copy()
    env["LOG_DIR"] = str(tmp_path / "logs")
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[3] / "src")
    try:
        result = subprocess.run(
            [sys.executable, "-c", script], cwd=tmp_path, env=env,
            text=True, capture_output=True, timeout=12,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("Kafka failure left the process waiting for analysis shutdown")
    assert result.returncode == 1, result.stderr
    assert "Kafka connection failed continuously" in (tmp_path / "logs/app.log").read_text(encoding="utf-8")
