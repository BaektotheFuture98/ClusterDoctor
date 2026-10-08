from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.ssh_log_view import (
    render_ssh_logs,
)

T = datetime(2026, 10, 2, 9, 32, 35, tzinfo=KST)


def test_ssh_view_shows_message_and_file_path():
    p = EvidenceProvenance(method='ssh', host='192.0.2.99', file_path='/var/log/es.log', excerpt=True)
    e = Evidence(evidence_id='E1', event_time=T, source=EvidenceSource.NODE_LOG,
                 message='[WARN] gc overhead', provenance=p)

    html = render_ssh_logs((e,))

    assert '[WARN] gc overhead' in html
    assert '/var/log/es.log' in html
    assert '원문 잘림' not in html
