from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.observations import SourceWindowStatus
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.source_status import (
    SourceSummary,
    summarize_source,
)

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def status(source, state, rows=None, error=""):
    return SourceWindowStatus(source, T0, T0 + timedelta(minutes=1), state, rows, T0, error)


def test_no_status_for_source_is_unknown():
    assert summarize_source((status("slowlog", "ok", 1),), "node_log") == SourceSummary("unknown", None, "")
    assert summarize_source((), "slowlog") == SourceSummary("unknown", None, "")


def test_ok_and_limited_count_as_ok_and_rows_are_summed():
    result = summarize_source((status("slowlog", "ok", 2), status("slowlog", "limited", 3)), "slowlog")
    assert result == SourceSummary("ok", 5, "")


def test_rows_ignore_missing_counts_and_none_when_no_counts():
    assert summarize_source((status("slowlog", "ok", 0),), "slowlog").rows == 0
    assert summarize_source((status("slowlog", "ok", None),), "slowlog").rows is None
    assert summarize_source((status("slowlog", "ok", None), status("slowlog", "ok", 4)), "slowlog").rows == 4


def test_any_failed_wins_with_first_non_empty_error():
    result = summarize_source((
        status("es_query_log", "ok", 3),
        status("es_query_log", "failed", None, ""),
        status("es_query_log", "failed", None, "first"),
        status("es_query_log", "failed", None, "second"),
    ), "es_query_log")
    assert (result.state, result.reason, result.rows) == ("failed", "first", 3)


def test_all_skipped_is_skipped_with_reason():
    result = summarize_source((status("node_log", "skipped", None, "no nodes"),), "node_log")
    assert result == SourceSummary("skipped", None, "no nodes")


def test_skipped_mixed_with_ok_is_ok():
    result = summarize_source((status("node_log", "skipped", None, "x"), status("node_log", "ok", 0)), "node_log")
    assert (result.state, result.rows) == ("ok", 0)
