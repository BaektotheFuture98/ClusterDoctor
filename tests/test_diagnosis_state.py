from datetime import datetime, timedelta

from cluster_doctor.domain.model.log_entry import NodeLogEntry
from cluster_doctor.infrastructure.outbound.llm.deepagent.diagnosis_state import (
    DiagnosisState,
)
from cluster_doctor.infrastructure.outbound.llm.deepagent.time_window import KST

TRIGGER = datetime(2026, 9, 17, 3, 0, tzinfo=KST)


def _node_log(line: str, at: datetime = TRIGGER) -> NodeLogEntry:
    return NodeLogEntry(
        timestamp=at,
        node="es-master-1",
        node_role="master",
        level="WARN",
        detected_level="WARN",
        logger="o.e.c.c.Coordinator",
        filename="es.log",
        host="10.0.0.1",
        line=line,
    )


def test_base_time_prefers_slowlog_timestamp():
    state = DiagnosisState(TRIGGER, TRIGGER + timedelta(seconds=30))
    assert state.first_seen == TRIGGER
    assert state.time_basis == "slowlog_timestamp"


def test_base_time_falls_back_on_clock_skew():
    received = TRIGGER - timedelta(minutes=1)
    state = DiagnosisState(TRIGGER, received)
    assert state.first_seen == received
    assert "clock skew" in state.time_basis


def test_base_time_falls_back_on_pipeline_delay():
    received = TRIGGER + timedelta(minutes=31)
    state = DiagnosisState(TRIGGER, received)
    assert state.first_seen == received
    assert "파이프라인 지연" in state.time_basis


def test_master_logs_dedupe_by_content():
    state = DiagnosisState(TRIGGER, TRIGGER)
    entry = _node_log("follower check failed")
    state.record_master_logs([entry, entry])
    assert len(state.master_logs) == 1


def test_master_text_extracts_level_and_logger():
    state = DiagnosisState(TRIGGER, TRIGGER)
    state.record_master_text(
        "[2026-09-17T03:00:01,123][WARN ][o.e.c.c.Coordinator      ] node left"
    )
    event = next(iter(state.master_logs.values()))
    assert event.level == "WARN"
    assert event.logger == "o.e.c.c.Coordinator"
    assert event.timestamp == datetime(2026, 9, 17, 3, 0, 1, tzinfo=KST)


def test_master_text_keeps_unparsable_lines():
    state = DiagnosisState(TRIGGER, TRIGGER)
    state.record_master_text("\tat org.elasticsearch.Foo.bar(Foo.java:42)")
    assert len(state.master_logs) == 1


def test_health_folds_identical_consecutive_points():
    state = DiagnosisState(TRIGGER, TRIGGER)
    payload = {"status": "green", "unassigned_shards": 0, "active_shards": 10,
               "number_of_nodes": 3}
    state.record_health(payload, now=TRIGGER)
    state.record_health(payload, now=TRIGGER + timedelta(seconds=30))
    assert len(state.health) == 1
    assert state.health[0].until == TRIGGER + timedelta(seconds=30)


def test_health_appends_on_status_change():
    state = DiagnosisState(TRIGGER, TRIGGER)
    state.record_health({"status": "green", "unassigned_shards": 0}, now=TRIGGER)
    state.record_health({"status": "yellow", "unassigned_shards": 2}, now=TRIGGER)
    assert [point.status for point in state.health] == ["green", "yellow"]


def test_mark_gap_records_and_echoes():
    state = DiagnosisState(TRIGGER, TRIGGER)
    assert state.mark_gap("SSH 실패") == "SSH 실패"
    assert state.gaps == ["SSH 실패"]


def test_mark_window_failed_does_not_set_degraded():
    state = DiagnosisState(TRIGGER, TRIGGER)
    end = TRIGGER + timedelta(minutes=5)
    state.mark_window_failed(TRIGGER, end, "분석 실패")
    assert state.failed == [(TRIGGER, end)]
    assert state.degraded is False


def test_unresolved_failure_is_none_when_retry_covered_it():
    state = DiagnosisState(TRIGGER, TRIGGER)
    end = TRIGGER + timedelta(minutes=5)
    state.mark_window_failed(TRIGGER, end, "실패")
    state.analyzed.append((TRIGGER, end))
    assert state.unresolved_failure() is None


def test_unresolved_failure_reports_leftover_window():
    state = DiagnosisState(TRIGGER, TRIGGER)
    end = TRIGGER + timedelta(minutes=5)
    state.mark_window_failed(TRIGGER, end, "실패")
    assert "2026-09-17T03:00:00" in state.unresolved_failure()


def test_coverage_gaps_is_silent_without_last_seen():
    state = DiagnosisState(TRIGGER, TRIGGER)
    assert state.coverage_gaps() == []


def test_coverage_gaps_accepts_split_windows():
    state = DiagnosisState(TRIGGER, TRIGGER)
    state.last_seen = TRIGGER + timedelta(minutes=20)
    state.requested = [
        (TRIGGER, TRIGGER + timedelta(minutes=10)),
        (TRIGGER + timedelta(minutes=10), TRIGGER + timedelta(minutes=20)),
    ]
    assert state.coverage_gaps() == []


def test_coverage_gaps_reports_uncovered_seconds():
    state = DiagnosisState(TRIGGER, TRIGGER)
    state.last_seen = TRIGGER + timedelta(minutes=20)
    state.requested = [(TRIGGER, TRIGGER + timedelta(minutes=5))]
    gaps = state.coverage_gaps()
    assert len(gaps) == 1
    assert "900초" in gaps[0]


def test_coverage_gaps_reports_when_never_analyzed():
    state = DiagnosisState(TRIGGER, TRIGGER)
    state.last_seen = TRIGGER + timedelta(minutes=5)
    assert "analyze_logs가 호출되지 않았다" in state.coverage_gaps()[0]


def test_to_observations_sorts_master_events_with_ssh_lines_last():
    state = DiagnosisState(TRIGGER, TRIGGER)
    state.record_master_logs([_node_log("later", TRIGGER + timedelta(minutes=1))])
    state.record_master_logs([_node_log("earlier", TRIGGER)])
    state.record_master_text("timestamp 없는 줄")
    observations = state.to_observations()
    assert [event.line for event in observations.master_events] == [
        "earlier",
        "later",
        "timestamp 없는 줄",
    ]
    assert observations.master_log_total == 3


def test_to_observations_carries_time_basis():
    state = DiagnosisState(TRIGGER, TRIGGER)
    assert state.to_observations().time_basis == "slowlog_timestamp"
