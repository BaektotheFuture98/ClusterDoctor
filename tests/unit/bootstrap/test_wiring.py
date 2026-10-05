from pathlib import Path
from types import SimpleNamespace

import pytest

from cluster_doctor.bootstrap.configuration.settings import Settings
from cluster_doctor.bootstrap.dependency import wiring


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
        node_heap_warn_percent=90, node_queue_warn=120,
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

    monkeypatch.setattr(wiring.clickhouse_connect, "get_client", lambda **kw: create("ch", kw))
    monkeypatch.setattr(wiring, "Elasticsearch", lambda **kw: create("es", kw))
    return events, created


def test_resource_factory_uses_explicit_settings(settings, clients, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "jdbc:clickhouse://wrong:8123/default")
    monkeypatch.setenv("ES_HOST", "wrong")
    events, created = clients
    with wiring.build_runtime_resources(settings) as resources:
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
    with wiring.build_runtime_resources(settings) as first:
        with wiring.build_runtime_resources(settings) as second:
            assert first.clickhouse_client is not second.clickhouse_client
            assert first.es_client is not second.es_client
    assert clients[0] == ["es", "ch", "es", "ch"]


def test_resource_factory_closes_clickhouse_if_es_creation_fails(settings, clients, monkeypatch):
    def fail(**kwargs):
        raise RuntimeError("es construction failed")

    monkeypatch.setattr(wiring, "Elasticsearch", fail)
    with pytest.raises(RuntimeError, match="es construction failed"):
        wiring.build_runtime_resources(settings)
    assert clients[0] == ["ch"]


@pytest.mark.parametrize("failing_client", ["ch", "es", None])
def test_resource_cleanup_attempts_both_clients_on_body_error(settings, clients, failing_client):
    resources = wiring.build_runtime_resources(settings)
    for client, _ in clients[1]:
        client.fail_close = client.name == failing_client
    expected_error = "analysis failed" if failing_client is None else f"{failing_client} close failed"
    with pytest.raises(RuntimeError, match=expected_error):
        with resources:
            raise RuntimeError("analysis failed")
    resources.close()
    assert clients[0] == ["es", "ch"]


def test_analysis_builders_share_clients_and_use_explicit_configuration(settings, clients, monkeypatch):
    monkeypatch.setenv("CLUSTER_NAME", "wrong")
    monkeypatch.setenv("REPORT_DIR", "wrong")
    # LLM initialization is external; keep real repositories and services.
    monkeypatch.setattr(wiring, "build_deepagents_incident_analyzer", lambda **kw: SimpleNamespace(**kw))
    with wiring.build_runtime_resources(settings) as resources:
        manual = wiring.build_manual_analysis(settings, resources)
        problem_log_processor = wiring.build_problem_log_processor(settings, resources)
        for service in (manual, problem_log_processor):
            assert service._cluster == "explicit-cluster"
            analysis = service._analysis_service
            assert analysis._report_publisher._output_dir == Path("custom-reports")
            analyzer = analysis._analyzer
            assert analyzer.log_repository._client is resources.clickhouse_client
            assert analyzer.cluster_repository._client is resources.es_client
            assert analyzer.node_resolver._client is resources.es_client
            assert analyzer.log_repository._slowlog_table == "custom_slowlog"
            assert analyzer.log_repository._log_table == "custom_query"
            assert analyzer.log_repository._node_metric_table == "custom_metric"
            assert analyzer.log_repository._node_log_table == "custom_node"
            assert analyzer.config.provider == "nvidia_nim"
            assert analyzer.config.api_key == "explicit-key"
            assert analyzer.config.model == "explicit-model"
            assert analyzer.config.heap_warn_percent == 90
            assert analyzer.config.queue_warn == 120
            assert analyzer.node_log_fetcher._user == "ssh-reader"
            assert analyzer.node_log_fetcher._password == "ssh-password"
            assert analyzer.node_log_fetcher._port == 2222
    assert len(clients[1]) == 2


async def test_kafka_builder_uses_explicit_configuration(settings, monkeypatch):
    subscribed = []
    configuration = {}

    class Consumer:
        def __init__(self, **kwargs):
            configuration.update(kwargs)

        def subscribe(self, *, topics, listener):
            subscribed.extend(topics)

    from cluster_doctor.kafka_consumer.consumer.kafka import consumer
    monkeypatch.setattr(consumer, "AIOKafkaConsumer", Consumer)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "wrong:9092")
    problem_log_processor = object()
    adapter = wiring.build_kafka_consumer(problem_log_processor, settings)
    assert configuration == dict(
        bootstrap_servers="explicit-kafka:9092", group_id="custom-group",
        auto_offset_reset="latest", enable_auto_commit=True,
    )
    assert subscribed == ["custom-topic"]
    assert adapter._problem_log_processor is problem_log_processor
    assert adapter._failure_timeout_seconds == 60
