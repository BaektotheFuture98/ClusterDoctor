import json
from datetime import UTC, datetime, timedelta
from itertools import count

from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import to_records
from cluster_doctor.incident_analysis_agent.model.evidence import (
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.graph import (
    run_analysis,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    RawRecord,
    group_into_buckets,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.schema import (
    MapOutput,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.spec import (
    AnalysisSpec,
)

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def select_all(messages, tokens, response_format):
    if response_format is MapOutput:
        return json.dumps({"selected": [{"record_id": 1}, {"record_id": 2}]})
    return json.dumps({"keep": [{"record_id": 1}, {"record_id": 2}]})


def test_same_line_from_distinct_hosts_preserves_raw_and_provenance():
    records = [
        RawRecord(
            record_id=i,
            event_time=T0,
            line="selection summary",
            raw="original <log>",
            provenance=EvidenceProvenance(
                method="ssh",
                host=host,
                file_path="/es/prod.log",
                query_from=T0,
                query_to=T0 + timedelta(minutes=1),
            ),
        )
        for i, host in enumerate(["host-a", "host-b"], 1)
    ]
    ids = count(1)
    result = run_analysis(
        AnalysisSpec(
            source=EvidenceSource.NODE_LOG,
            label="logs",
            what_matters="",
            what_is_noise="",
        ),
        group_into_buckets(records),
        select_all,
        new_evidence_id=lambda: f"E-{next(ids)}",
    )
    assert len(result.evidence) == 2
    assert [e.provenance.host for e in result.evidence] == ["host-a", "host-b"]
    assert all(e.raw == "original <log>" for e in result.evidence)
    assert all(e.message == "selection summary" for e in result.evidence)


def test_ssh_records_mark_inherited_and_fallback_times():
    p = EvidenceProvenance(method="ssh", host="host-a", file_path="/es/prod.log")
    records = to_records(
        "first without timestamp\n[2026-10-01T09:00:02,123][WARN ][logger] error\n at stack.frame",
        fallback_time=T0,
        provenance=p,
    )
    assert [r.time_origin for r in records] == ["fallback", "parsed", "inherited"]
    assert all(r.provenance == p for r in records)
    assert records[2].raw == " at stack.frame"
