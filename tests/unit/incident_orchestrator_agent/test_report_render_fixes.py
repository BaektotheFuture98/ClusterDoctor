from dataclasses import replace
from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, Observations
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.model.incident_report import CauseAssessment, IncidentAnalysisReport, Narrative
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import metric_nodes
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import render_layout
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text

NOW = datetime(2026, 10, 14, 15, 0, tzinfo=KST)


def _slow(evidence_id, node, message):
    return Evidence(evidence_id=evidence_id, event_time=datetime(2026, 10, 14, 14, 0, tzinfo=KST),
                    source=EvidenceSource.SLOWLOG, node_name=node, message=message)


def _nodes(count=12):
    return tuple(NodeMetricRow(node=f"N{i:02d}", samples=1, cpu_max=50 + i, jvm_heap_max=10,
                               search_queue_max=i, write_queue_max=0) for i in range(count))


def _report(evidence=(), narrative=None, nodes=None):
    return IncidentAnalysisReport(
        observations=Observations(nodes=_nodes() if nodes is None else nodes), evidence=evidence,
        narrative=narrative, verification_status="PASSED", cluster="es")


def _html(report):
    return render_layout(report, NOW, gaps=(), analysis_failed=False, css="")


def test_metric_card_label_names_master_wait():
    html = _html(_report())
    assert "마스터 응답 지연 노드" in html
    assert ">응답 지연 노드<" not in html


def test_slowlog_cited_line_printed_once_with_mark():
    slow = _slow("E1", "N03", "took 32s 느린쿼리")
    cite = EvidenceCitation("E1", slow)
    narrative = Narrative(causes=(CauseAssessment(statement="원인", mechanism="설명", supporting=(cite,)),))
    html = _html(_report((slow,), narrative))
    assert html.count("took 32s 느린쿼리") == 1
    assert "[판단 인용]" in html[html.index('id="slowlogs"'):]


def test_slowlog_contradicting_line_printed_once_with_mark():
    slow = _slow("E1", "N03", "took 32s 느린쿼리")
    cite = EvidenceCitation("E1", slow)
    narrative = Narrative(causes=(CauseAssessment(statement="원인", mechanism="설명", contradicting=(cite,)),))
    html = _html(_report((slow,), narrative))
    assert html.count("took 32s 느린쿼리") == 1
    assert "[반증 인용]" in html


def test_non_slowlog_citation_still_listed_in_citations():
    master = Evidence(evidence_id="E2", event_time=datetime(2026, 10, 14, 14, 0, tzinfo=KST),
                      source=EvidenceSource.MASTER_LOG, message="마스터 원문")
    cite = EvidenceCitation("E2", master)
    narrative = Narrative(causes=(CauseAssessment(statement="원인", mechanism="설명", supporting=(cite,)),))
    html = _html(_report((master,), narrative))
    assert "마스터 원문" in html[html.index('id="citations"'):]


def test_slowlog_node_leads_metric_table_and_text():
    report = _report((_slow("E1", "N00", "느림"),))
    names = [n.node for n in metric_nodes(report)]
    assert names[0] == "N00" and len(names) == 10
    assert names[1] == "N11"
    html = _html(report)
    assert html.index("<td>N00</td>") < html.index("<td>N11</td>")
    text = render_text(report)
    assert text.index("N00 · CPU") < text.index("N11 · CPU")


def test_slowlog_node_without_metric_row_is_not_invented():
    report = _report((_slow("E1", "GHOST", "느림"),))
    names = [n.node for n in metric_nodes(report)]
    assert "GHOST" not in names and names[0] == "N11"
