from datetime import datetime, timezone

import pytest

from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import (
    Evidence,
    EvidenceSource,
)

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)


def _make_evidence(**overrides) -> Evidence:
    fields = {
        "evidence_id": "E-INC-1-1",
        "event_time": _T0,
        "source": EvidenceSource.NODE_LOG,
        "message": "line",
    }
    fields.update(overrides)
    return Evidence(**fields)


def test_optional_fields_default_to_none():
    evidence = _make_evidence()
    assert evidence.node_id is None
    assert evidence.node_name is None
    assert evidence.event_type is None
    assert evidence.severity is None
    assert evidence.raw is None
    assert evidence.selection_reason is None


def test_is_frozen():
    evidence = _make_evidence()
    with pytest.raises(Exception):
        evidence.message = "changed"  # type: ignore[misc]


def test_evidence_source_values():
    assert EvidenceSource.SLOWLOG == "slowlog"
    assert EvidenceSource.QUERY_LOG == "es_query_log"
    assert EvidenceSource.NODE_METRIC == "node_metric"
    assert EvidenceSource.MASTER_LOG == "master_log"
    assert EvidenceSource.NODE_LOG == "node_log"
    assert EvidenceSource.CLUSTER_STATE == "cluster_state"
