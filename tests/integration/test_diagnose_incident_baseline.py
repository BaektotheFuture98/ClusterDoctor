# tests/integration/test_diagnose_incident_baseline.py
from datetime import datetime, timezone

import pytest

from cluster_doctor.adapters.outbound.persistence.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.domain.incident.models import IncidentStatus
from cluster_doctor.domain.incident.state import IncidentState

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
_T1 = datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc)
_T_MID = datetime(2024, 1, 1, 13, 30, tzinfo=timezone.utc)


def _report(incident_id: str, frm=_T0, to=_T1, **kw) -> LogAnalysisReport:
    return LogAnalysisReport(incident_id=incident_id, analyzed_from=frm, analyzed_to=to, **kw)


def test_store_put_get_report_roundtrip():
    store = InMemoryArtifactStore()
    r = _report("INC-001", summary="hello")
    ref = store.put_report("INC-001", r)
    assert ref.startswith("RPT-INC-001-")
    got = store.get_report(ref)
    assert got is not None
    assert got.summary == "hello"


def test_store_get_missing_report_returns_none():
    store = InMemoryArtifactStore()
    assert store.get_report("RPT-no-such") is None


def test_incident_state_initial_status():
    s = IncidentState(incident_id="INC-003")
    assert s.status == IncidentStatus.OPEN
    assert s.report_refs == []


def test_incident_state_accumulates_report_refs():
    s = IncidentState(incident_id="INC-004")
    s.report_refs.append("RPT-INC-004-1")
    s.report_refs.append("RPT-INC-004-2")
    assert len(s.report_refs) == 2
