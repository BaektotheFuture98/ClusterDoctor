from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    citation_text,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.ssh_log_view import (
    render_ssh_logs,
)

T = datetime(2026, 10, 2, 9, 32, 35, tzinfo=KST)


def test_citation_text_shows_message_without_raw_label():
    e = Evidence(evidence_id='E1', event_time=T, source=EvidenceSource.QUERY_LOG,
                 message='runtime=2.56s', provenance=EvidenceProvenance(method='clickhouse', table='log'))

    text = citation_text(EvidenceCitation('E1', e))

    assert text.endswith('runtime=2.56s')
    assert '원문 미확보' not in text


def test_ssh_view_shows_message_and_file_path():
    p = EvidenceProvenance(method='ssh', host='192.0.2.99', file_path='/var/log/es.log', excerpt=True)
    e = Evidence(evidence_id='E1', event_time=T, source=EvidenceSource.NODE_LOG,
                 message='[WARN] gc overhead', provenance=p)

    html = render_ssh_logs((e,))

    assert '[WARN] gc overhead' in html
    assert '/var/log/es.log' in html
    assert '원문 잘림' not in html
