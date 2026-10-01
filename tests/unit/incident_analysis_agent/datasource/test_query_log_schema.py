import json
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from cluster_doctor.incident_analysis_agent.datasource.clickhouse import query_log
from cluster_doctor.incident_analysis_agent.model.log_entries import record_json
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def source_row():
    return {
        "reg_date": T0,
        "host": "host",
        "run_time": Decimal("12.34"),
        "success": "N",
        "s_date": 20260901,
        "e_date": 20260930,
        "date_range": 30,
        "keyword": ["a", "b"],
        "keyword_count": 2,
        "url": "/search",
        "cmd": "agg",
        "service": "web",
        "env": "prod",
        "project": "project",
        "company": "company",
        "user": "user",
        "search_count": 42,
        "etc": '{"custom":"original"}',
        "cluster": "es",
    }


def test_select_all_maps_names_and_preserves_every_column():
    data = source_row() | {"new_column": {"nested": [1, 2]}}
    names = tuple(reversed(tuple(data)))

    def query(sql, parameters):
        assert "SELECT * FROM db.log" in sql
        assert "WHERE reg_date" in sql
        return SimpleNamespace(
            column_names=names, result_rows=[tuple(data[n] for n in names)]
        )

    entry = query_log.fetch(
        SimpleNamespace(query=query), "db.log", TimeRange(T0, T0 + timedelta(minutes=1))
    )[0]
    dto_fields = {f.name for f in fields(entry)}
    assert "reg_date" in dto_fields and "timestamp" not in dto_fields
    assert "keyword" in dto_fields and "keywords" not in dto_fields
    assert entry.reg_date == T0 and entry.timestamp == T0
    assert entry.success == "N" and entry.is_success is False
    assert entry.keyword == ("a", "b")
    raw = json.loads(record_json(entry))
    assert set(raw) == set(data)
    assert raw["date_range"] == 30 and raw["s_date"] == 20260901
    assert raw["etc"] == data["etc"] and raw["new_column"] == data["new_column"]
    assert raw["success"] == "N"
    record = query_log.to_records([entry])[0]
    assert record.event_time == T0 and record.severity == "ERROR"
    assert json.loads(record.raw) == raw
