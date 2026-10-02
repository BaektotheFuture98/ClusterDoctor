from dataclasses import fields
from datetime import datetime

import pytest
from pydantic import ValidationError

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import RawRecord

T = datetime(2026, 10, 2, 9, 32, 35, tzinfo=KST)
REMOVED = {'raw', 'raw_kind', 'raw_truncated'}


def test_evidence_rejects_raw_copy():
    with pytest.raises(ValidationError):
        Evidence(evidence_id='E1', event_time=T, source=EvidenceSource.QUERY_LOG,
                 message='m', raw='{}')


def test_dtos_have_no_raw_fields():
    assert not REMOVED & set(Evidence.model_fields)
    assert not REMOVED & {f.name for f in fields(RawRecord)}
