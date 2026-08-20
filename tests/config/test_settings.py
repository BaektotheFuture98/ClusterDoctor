from cluster_doctor.config.settings import Settings

REQUIRED = {
    "GEMINI_API_KEY":  "test-key",
    "CLICKHOUSE_URL":  "jdbc:clickhouse://localhost:8123/default",
}


def test_defaults_applied(monkeypatch):
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
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
