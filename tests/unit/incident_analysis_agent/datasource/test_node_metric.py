from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import (
    NodeMetricThresholds,
    to_evidence,
)
from cluster_doctor.incident_analysis_agent.model.log_entries import NodeMetricEntry

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)


def _entry(**overrides) -> NodeMetricEntry:
    fields = dict(
        timestamp=_T0,
        node_name="es-data-1",
        node_ip="10.0.0.1",
        os_cpu_percent=10,
        os_mem_used_percent=40,
        process_cpu_percent=10,
        jvm_heap_used_percent=50,
        search_active=1,
        search_queue=0,
        search_rejected=0,
        write_active=1,
        write_queue=0,
        write_rejected=0,
    )
    fields.update(overrides)
    return NodeMetricEntry(**fields)


def _new_evidence_id():
    counter = iter(range(1, 1000))

    def factory() -> str:
        return f"E-TEST-{next(counter)}"

    return factory


def test_below_threshold_produces_no_evidence():
    evidence = to_evidence([_entry()], new_evidence_id=_new_evidence_id())
    assert evidence == []


def test_message_and_raw_carry_the_same_text():
    evidence = to_evidence(
        [_entry(jvm_heap_used_percent=90)], new_evidence_id=_new_evidence_id()
    )
    assert len(evidence) == 1
    assert evidence[0].raw == evidence[0].message


def test_only_highest_sample_per_node_and_rule_is_kept():
    entries = [
        _entry(jvm_heap_used_percent=86, timestamp=_T0),
        _entry(jvm_heap_used_percent=95, timestamp=_T0.replace(minute=5)),
        _entry(jvm_heap_used_percent=88, timestamp=_T0.replace(minute=8)),
    ]
    evidence = to_evidence(entries, new_evidence_id=_new_evidence_id())
    assert len(evidence) == 1
    assert "jvm_heap=95%" in evidence[0].message


def test_rejected_floor_is_zero_not_configurable():
    evidence = to_evidence(
        [_entry(search_rejected=1)], new_evidence_id=_new_evidence_id()
    )
    assert len(evidence) == 1
    assert evidence[0].severity == "Critical"


def test_custom_thresholds_change_what_counts_as_warning():
    lenient = NodeMetricThresholds(heap_warn_percent=95, queue_warn=1000)
    evidence = to_evidence(
        [_entry(jvm_heap_used_percent=90)],
        new_evidence_id=_new_evidence_id(),
        thresholds=lenient,
    )
    assert evidence == []
