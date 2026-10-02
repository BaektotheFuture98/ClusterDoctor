from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import (
    preserve_required_evidence,
)

T = datetime(2026, 10, 2, 8, 33, 39, tzinfo=KST)


def _query(evidence_id: str, record_key: str, message: str) -> Evidence:
    return Evidence(evidence_id=evidence_id, event_time=T, source=EvidenceSource.QUERY_LOG,
                    message=message, record_key=record_key)


def test_same_second_executions_stay_apart_and_merge_by_record_key():
    required = [_query('A', 'k1', 'long line'), _query('B', 'k2', 'long line')]
    selected = [_query('S', 'k1', 'short line')]

    result = preserve_required_evidence(required, selected)

    assert [e.evidence_id for e in result] == ['S', 'B']
