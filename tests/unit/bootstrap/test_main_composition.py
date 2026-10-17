from pathlib import Path

import pytest

from cluster_doctor.bootstrap.configuration.settings import Settings
from cluster_doctor import main as app


@pytest.fixture
def settings():
    return Settings(
        _env_file=None,
        llm_provider="nvidia_nim", nvidia_api_key="explicit-key", gemini_api_key="unused-test-key",
        nvidia_model="explicit-model",
        clickhouse_url="jdbc:clickhouse://explicit-ch:9123/diagnostics",
        clickhouse_user="reader", clickhouse_password="ch-password",
        clickhouse_slowlog_table="custom_slowlog", clickhouse_log_table="custom_query",
        clickhouse_node_metric_table="custom_metric", clickhouse_node_log_table="custom_node",
        es_host="es-one, es-two", es_port=9300, es_user="es-reader", es_password="es-password",
        ssh_user="ssh-reader", ssh_password="ssh-password", ssh_port=2222,
        cluster_name="explicit-cluster", report_dir="custom-reports",
        node_heap_warn_percent=90, node_queue_warn=120, analysis_concurrency=3,
        kafka_bootstrap_servers="explicit-kafka:9092", kafka_topic="custom-topic",
        kafka_group_id="custom-group", kafka_failure_timeout_seconds=60,
    )


class FakeClient:
    def __init__(self, name, events, *, fail_close=False):
        self.name = name
        self.events = events
        self.fail_close = fail_close

    def close(self):
        self.events.append(self.name)
        if self.fail_close:
            raise RuntimeError(f"{self.name} close failed")


@pytest.fixture
def clients(monkeypatch):
    events = []
    created = []

    def create(name, kwargs):
        client = FakeClient(name, events)
        created.append((client, kwargs))
        return client

    monkeypatch.setattr(app.clickhouse_connect, "get_client", lambda **kw: create("ch", kw))
    monkeypatch.setattr(app, "Elasticsearch", lambda **kw: create("es", kw))
    return events, created


def test_resource_factory_uses_explicit_settings(settings, clients, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "jdbc:clickhouse://wrong:8123/default")
    monkeypatch.setenv("ES_HOST", "wrong")
    events, created = clients
    with app.build_runtime_resources(settings) as resources:
        assert resources.clickhouse_client is created[0][0]
        assert resources.es_client is created[1][0]
        assert created[0][1] == dict(
            host="explicit-ch", port=9123, database="diagnostics",
            username="reader", password="ch-password", send_receive_timeout=30,
            autogenerate_session_id=False, tz_mode="aware",
        )
        assert created[1][1] == dict(
            hosts=[dict(host="es-one", port=9300, scheme="http"),
                   dict(host="es-two", port=9300, scheme="http")],
            basic_auth=("es-reader", "es-password"), request_timeout=10,
        )
    resources.close()
    assert events == ["es", "ch"]


def test_resources_are_separate_for_each_execution(settings, clients):
    with app.build_runtime_resources(settings) as first:
        with app.build_runtime_resources(settings) as second:
            assert first.clickhouse_client is not second.clickhouse_client
            assert first.es_client is not second.es_client
    assert clients[0] == ["es", "ch", "es", "ch"]


def test_resource_factory_closes_clickhouse_if_es_creation_fails(settings, clients, monkeypatch):
    def fail(**kwargs):
        raise RuntimeError("es construction failed")

    monkeypatch.setattr(app, "Elasticsearch", fail)
    with pytest.raises(RuntimeError, match="es construction failed"):
        app.build_runtime_resources(settings)
    assert clients[0] == ["ch"]


@pytest.mark.parametrize("failing_client", ["ch", "es", None])
def test_resource_cleanup_attempts_both_clients_on_body_error(settings, clients, failing_client):
    resources = app.build_runtime_resources(settings)
    for client, _ in clients[1]:
        client.fail_close = client.name == failing_client
    expected_error = "analysis failed" if failing_client is None else f"{failing_client} close failed"
    with pytest.raises(RuntimeError, match=expected_error):
        with resources:
            raise RuntimeError("analysis failed")
    resources.close()
    assert clients[0] == ["es", "ch"]


def test_composition_shares_model_seams_and_explicit_configuration(settings, clients, monkeypatch):
    from cluster_doctor import main as app
    from cluster_doctor.incident_analysis_agent.agent import subagent as analysis_module
    from cluster_doctor.incident_orchestrator_agent.agent.adapter import MainAgent
    from cluster_doctor.incident_orchestrator_agent.agent.analysis_subagent import AnalysisSubagentAdapter
    from langchain_litellm import ChatLiteLLM
    from cluster_doctor.incident_analysis_agent.agent.runtime.litellm_client import _REQUEST_TIMEOUT_SECONDS

    def forbidden(*args, **kwargs):
        pytest.fail("composition must not perform inference")

    import litellm
    monkeypatch.setattr(litellm, "completion", forbidden)
    monkeypatch.setattr(ChatLiteLLM, "_generate", forbidden)
    monkeypatch.setenv("CLUSTER_NAME", "wrong")
    monkeypatch.setenv("REPORT_DIR", "wrong")
    with app.build_runtime_resources(settings) as resources:
        analysis = app.compose_analysis_service(settings, resources)
        assert analysis._report_publisher._output_dir == Path("custom-reports")
        analyzer = analysis._analyzer
        assert isinstance(analyzer, MainAgent)
        delegate = analyzer._analysis_subagent
        assert isinstance(delegate, AnalysisSubagentAdapter)
        child = delegate._analysis_agent
        assert isinstance(child, analysis_module.AnalysisAgent)
        assert child._model is analyzer._model
        assert child._seams is analyzer._seams
        model = analyzer._model
        assert model.model == "nvidia_nim/explicit-model"
        assert model.api_key == "explicit-key"
        assert model.max_retries == 0
        assert model.request_timeout == _REQUEST_TIMEOUT_SECONDS
        assert model.model_kwargs == {"num_retries": 0}
        seams = analyzer._seams
        logs = seams.fetch_logs.__self__
        assert seams.fetch_node_logs.__self__ is logs
        assert logs._client is resources.clickhouse_client
        assert seams.cluster._client is resources.es_client
        assert seams.node_resolver._client is resources.es_client
        assert logs._slowlog_table == "custom_slowlog"
        assert logs._log_table == "custom_query"
        assert logs._node_metric_table == "custom_metric"
        assert logs._node_log_table == "custom_node"
        assert seams.metric_thresholds.heap_warn_percent == 90
        assert seams.metric_thresholds.queue_warn == 120
        assert seams.report_writer._thresholds is seams.metric_thresholds
        assert seams.analysis_concurrency == 3
        assert seams.node_log_fetcher._user == "ssh-reader"
        assert seams.node_log_fetcher._password == "ssh-password"
        assert seams.node_log_fetcher._port == 2222
        assert seams.report_writer._call_llm is seams.call_llm
        assert seams.call_llm.keywords == dict(provider="nvidia_nim", model="explicit-model", api_key="explicit-key")
    assert clients[0] == ["es", "ch"]


def test_composed_main_compiles_with_its_bound_delegate(settings, clients, monkeypatch):
    from datetime import UTC, datetime
    from cluster_doctor import main as app
    from cluster_doctor.incident_orchestrator_agent.agent import adapter
    from cluster_doctor.incident_orchestrator_agent.model.incident import Incident
    from unittest.mock import patch

    incident = Incident(incident_id="composition", cluster="test", trigger_time=datetime.now(UTC), kafka_receive_time=datetime.now(UTC))
    with app.build_runtime_resources(settings) as resources:
        analyzer = app.compose_analysis_service(settings, resources)._analyzer
        delegate = analyzer._analysis_subagent
        compiled = []
        monkeypatch.setattr(adapter, "build_main_agent", lambda **kwargs: compiled.append(kwargs) or kwargs)
        bound = []
        real_bind = delegate.bind

        def capture_bound(actual):
            result = real_bind(actual)
            bound.append(result)
            return result

        with patch.object(delegate, "bind", side_effect=capture_bound) as bind:
            graph = analyzer._compile(incident)
        bind.assert_called_once_with(incident)
        assert graph["analysis_subagent"] is bound[0]
        assert graph["model"] is analyzer._model
        assert graph["analysis_subagent"]["name"] == "analysis"
        assert len(compiled) == 1


async def test_main_uses_explicit_kafka_settings_and_pending_limit(settings, clients, monkeypatch):
    from cluster_doctor import main as app
    from cluster_doctor.kafka_consumer.consumer.kafka import consumer
    subscribed = []
    configuration = {}
    captured = []

    class Consumer:
        def __init__(self, **kwargs):
            configuration.update(kwargs)

        def subscribe(self, *, topics, listener):
            subscribed.extend(topics)

    async def run(adapter):
        captured.append(adapter)

    monkeypatch.setattr(consumer, "AIOKafkaConsumer", Consumer)
    monkeypatch.setattr(app.KafkaConsumerAdapter, "run", run)
    monkeypatch.setattr(app, "get_settings", lambda: settings)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "wrong:9092")
    await app.main()
    assert configuration == dict(bootstrap_servers="explicit-kafka:9092", group_id="custom-group", auto_offset_reset="latest", enable_auto_commit=True)
    assert subscribed == ["custom-topic"]
    adapter = captured[0]
    assert adapter._failure_timeout_seconds == 60
    processor = adapter._problem_log_processor
    assert processor._cluster == "explicit-cluster"
    assert processor._pending.maxsize == 10_000
    assert clients[0] == ["es", "ch"]


@pytest.mark.parametrize("failure", ["model-validation", "harness-registration", "analysis-harness"])
async def test_composition_failure_cleans_clients_before_consumer(settings, clients, monkeypatch, failure):
    from cluster_doctor import main as app
    from cluster_doctor.incident_orchestrator_agent.agent.runtime import chat_model
    from cluster_doctor.incident_analysis_agent.agent.runtime import harness
    from cluster_doctor.bootstrap.configuration.settings import ConfigurationError

    monkeypatch.setattr(app, "get_settings", lambda: settings)
    monkeypatch.setattr(app, "KafkaConsumerAdapter", lambda **kw: pytest.fail("consumer created before validation"))
    if failure == "model-validation":
        monkeypatch.setitem(chat_model.litellm.model_cost, "nvidia_nim/explicit-model", {"supports_function_calling": False})
        monkeypatch.setattr(chat_model.litellm, "supports_function_calling", lambda **kw: False)
        error, message = ConfigurationError, "tool call"
    elif failure == "analysis-harness":
        from cluster_doctor.incident_analysis_agent.agent import subagent
        def fail(model):
            raise RuntimeError("analysis harness preparation failed")
        monkeypatch.setattr(subagent, "restrict_harness", fail)
        error, message = RuntimeError, "analysis harness preparation failed"
    else:
        monkeypatch.setattr(settings, "nvidia_model", "harness-registration-failure")
        def fail(*args):
            raise RuntimeError("harness registration failed")
        monkeypatch.setattr(harness, "register_harness_profile", fail)
        error, message = RuntimeError, "harness registration failed"
    with pytest.raises(error, match=message):
        await app.main()
    assert clients[0] == ["es", "ch"]


def test_main_owns_client_and_publication_factories():
    from cluster_doctor import main as app

    for factory in ("_build_clickhouse_client", "_build_es_client", "build_sftp_uploader"):
        assert callable(getattr(app, factory, None)), f"main must own {factory}"
    assert app.build_runtime_resources.__module__ == "cluster_doctor.main"
    assert app.build_report_publisher.__module__ == "cluster_doctor.main"
