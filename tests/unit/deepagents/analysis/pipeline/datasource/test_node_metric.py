from datetime import datetime, timezone

from cluster_doctor.adapters.outbound.deepagents.analysis.pipeline.datasource.node_metric import (
    NodeMetricThresholds,
    to_evidence,
)
from cluster_doctor.domain.analysis.log_entries import NodeMetricEntry

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


def _collector():
    raw_texts: list[str] = []
    counter = iter(range(1, 1000))

    def new_evidence_id() -> str:
        return f"E-TEST-{next(counter)}"

    def put_raw(text: str) -> str:
        raw_texts.append(text)
        return f"R-TEST-{len(raw_texts)}"

    return new_evidence_id, put_raw, raw_texts


def test_below_threshold_produces_no_evidence():
    new_evidence_id, put_raw, _ = _collector()
    evidence = to_evidence([_entry()], new_evidence_id=new_evidence_id, put_raw=put_raw)
    assert evidence == []


def test_message_and_raw_ref_carry_the_same_text():
    new_evidence_id, put_raw, raw_texts = _collector()
    evidence = to_evidence(
        [_entry(jvm_heap_used_percent=90)],
        new_evidence_id=new_evidence_id,
        put_raw=put_raw,
    )
    assert len(evidence) == 1
    assert raw_texts == [evidence[0].message]


def test_only_highest_sample_per_node_and_rule_is_kept():
    new_evidence_id, put_raw, _ = _collector()
    entries = [
        _entry(jvm_heap_used_percent=86, timestamp=_T0),
        _entry(jvm_heap_used_percent=95, timestamp=_T0.replace(minute=5)),
        _entry(jvm_heap_used_percent=88, timestamp=_T0.replace(minute=8)),
    ]
    evidence = to_evidence(entries, new_evidence_id=new_evidence_id, put_raw=put_raw)
    assert len(evidence) == 1
    assert "jvm_heap=95%" in evidence[0].message


def test_rejected_floor_is_zero_not_configurable():
    new_evidence_id, put_raw, _ = _collector()
    evidence = to_evidence(
        [_entry(search_rejected=1)], new_evidence_id=new_evidence_id, put_raw=put_raw
    )
    assert len(evidence) == 1
    assert evidence[0].severity == "Critical"


def test_custom_thresholds_change_what_counts_as_warning():
    new_evidence_id, put_raw, _ = _collector()
    lenient = NodeMetricThresholds(heap_warn_percent=95, queue_warn=1000)
    evidence = to_evidence(
        [_entry(jvm_heap_used_percent=90)],
        new_evidence_id=new_evidence_id,
        put_raw=put_raw,
        thresholds=lenient,
    )
    assert evidence == []
