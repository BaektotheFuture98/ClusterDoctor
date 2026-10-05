import importlib
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest

from cluster_doctor.bootstrap.lifecycle.app_lifecycle import RuntimeResources


@pytest.fixture
def runtime():
    events = []
    stack = ExitStack()
    stack.callback(events.append, "ch-close")
    stack.callback(events.append, "es-close")
    return RuntimeResources(object(), object(), stack), events


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    return importlib.import_module("cluster_doctor.main")


@pytest.mark.parametrize("failure", [None, "problem_log_processor-build", "consumer-build", "consumer-run", "problem_log_processor-close"])
async def test_main_closes_processor_before_clients_even_on_failure(app, runtime, monkeypatch, failure):
    resources, events = runtime
    settings = object()

    class FakeProblemLogProcessor:
        async def close(self):
            events.append("problem_log_processor-close")
            if failure == "problem_log_processor-close":
                raise RuntimeError(failure)

    problem_log_processor = FakeProblemLogProcessor()

    class Consumer:
        async def run(self):
            events.append("consumer-run")
            if failure == "consumer-run":
                raise RuntimeError(failure)

    def build_resources(actual):
        assert actual is settings
        return resources

    def build_problem_log_processor(actual, shared):
        assert actual is settings
        assert shared is resources
        if failure == "problem_log_processor-build":
            raise RuntimeError(failure)
        return problem_log_processor

    def build_consumer(actual_problem_log_processor, actual):
        assert actual_problem_log_processor is problem_log_processor
        assert actual is settings
        if failure == "consumer-build":
            raise RuntimeError(failure)
        return Consumer()

    monkeypatch.setattr(app, "get_settings", lambda: settings)
    monkeypatch.setattr(app, "build_runtime_resources", build_resources)
    monkeypatch.setattr(app, "build_problem_log_processor", build_problem_log_processor)
    monkeypatch.setattr(app, "build_kafka_consumer", build_consumer)
    if failure:
        with pytest.raises(RuntimeError, match=failure):
            await app.main()
    else:
        await app.main()
    expected = []
    if failure not in ("problem_log_processor-build", "consumer-build"):
        expected.append("consumer-run")
    if failure != "problem_log_processor-build":
        expected.append("problem_log_processor-close")
    assert events == expected + ["es-close", "ch-close"]


@pytest.fixture
def manual_script(app, monkeypatch):
    scripts = Path(__file__).resolve().parents[3] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    return importlib.import_module("run_analysis")


@pytest.mark.parametrize("failure", [None, "analysis", "build", "interrupt"])
def test_manual_run_owns_and_closes_resources(manual_script, runtime, monkeypatch, tmp_path, failure):
    resources, events = runtime
    settings = SimpleNamespace(report_dir=str(tmp_path / "reports"))
    outcome = SimpleNamespace(
        status="complete", analysis_calls=1, analysis_failed=False, gaps=(),
        diagnostics=SimpleNamespace(
            report=None, evidence=(), publication=SimpleNamespace(text_length=0),
            observations=SimpleNamespace(timeline=(), nodes=(), master_log_total=0, health=(), candidates=()),
        ),
    )

    class ManualAnalysis:
        async def handle(self, moments):
            events.append("analysis")
            if failure == "analysis":
                raise RuntimeError("analysis failed")
            if failure == "interrupt":
                raise KeyboardInterrupt
            return outcome

    def build_resources(actual):
        assert actual is settings
        return resources

    def build_analysis(actual, shared):
        assert actual is settings
        assert shared is resources
        if failure == "build":
            raise RuntimeError("build failed")
        return ManualAnalysis()

    monkeypatch.setattr(manual_script, "get_settings", lambda: settings)
    monkeypatch.setattr(manual_script, "build_runtime_resources", build_resources)
    monkeypatch.setattr(manual_script, "build_manual_analysis", build_analysis)
    if failure in ("build", "interrupt"):
        error = RuntimeError if failure == "build" else KeyboardInterrupt
        with pytest.raises(error):
            manual_script.run([object()])
    else:
        assert manual_script.run([object()]) == (1 if failure == "analysis" else 0)
    assert events == ([] if failure == "build" else ["analysis"]) + ["es-close", "ch-close"]


def test_manual_dry_run_does_not_load_settings_or_create_clients(manual_script, monkeypatch):
    def forbidden(*args):
        pytest.fail("dry-run must not load settings or create clients")

    monkeypatch.setattr(manual_script, "get_settings", forbidden)
    monkeypatch.setattr(manual_script, "build_runtime_resources", forbidden)
    monkeypatch.setattr("sys.argv", ["run_analysis.py", "--at", "2026-09-16T04:22:00", "--span", "6m", "--dry-run"])
    assert manual_script.main() == 0
