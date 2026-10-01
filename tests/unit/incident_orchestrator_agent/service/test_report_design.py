import re
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    TimelineRow,
)
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    RootCause,
    TimelineEvent,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    render_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
)

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def example():
    e = Evidence(
        evidence_id="E-<1>",
        event_time=T0,
        source=EvidenceSource.NODE_LOG,
        node_name="data-03",
        message="summary only",
        raw="\n".join(f"raw-{i} <tag>&" for i in range(15)),
        provenance=EvidenceProvenance(
            method="ssh",
            host="10.0.1.23",
            file_path="/es/prod.log",
            collected_at=T0,
            query_from=T0,
            query_to=T0 + timedelta(minutes=1),
        ),
    )
    report = LogAnalysisReport(
        incident_id="I-1",
        analyzed_from=T0,
        analyzed_to=T0 + timedelta(minutes=1),
        summary="검색 요청 거절",
        verification_status=VerificationStatus.PASSED,
        timeline=(
            TimelineEvent(
                at=T0, description="요청 거절", evidence_refs=(e.evidence_id,)
            ),
        ),
        root_causes=(
            RootCause(
                statement="검색 부하 집중",
                confidence="Medium",
                supporting_evidence_refs=(e.evidence_id,),
            ),
            RootCause(
                statement="다른 후보",
                confidence="Low",
                counter_evidence_refs=("missing",),
            ),
        ),
        recommendations=("큐 확인", "쿼리 확인", "노드 확인", "추가 확인"),
    )
    obs = Observations(
        timeline=(TimelineRow(minute=T0, counts={"slowlog": 1}),),
        requested=((T0, T0 + timedelta(minutes=1)),),
    )
    return to_incident_analysis_report(report, obs, [e])


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = []
        self.links = []
        self.details = []
        self.in_nav = False

    def handle_endtag(self, tag):
        if tag == "nav":
            self.in_nav = False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "nav":
            self.in_nav = True
        if "id" in a:
            self.ids.append(a["id"])
        # Nav targets (#actions, #evidence) are built in later report sections.
        if tag == "a" and not self.in_nav and a.get("href", "").startswith("#"):
            self.links.append(a["href"][1:])
        if tag == "details":
            self.details.append(a)


def test_summary_timeline_and_visible_source_logs_precede_details():
    html = render_report(example())
    assert (
        html.index('id="summary"')
        < html.index('id="timeline"')
        < html.index('id="causes"')
        < html.index('id="details"')
    )
    assert "10.0.1.23" in html and "/es/prod.log" in html
    assert "raw-0 &lt;tag&gt;&amp;" in html and "raw-14 &lt;tag&gt;&amp;" in html
    assert "<tag>" not in html
    assert "2026-10-01 09:00:00 KST" in html
    p = Links()
    p.feed(html)
    assert len(p.ids) == len(set(p.ids))
    assert set(p.links) <= set(p.ids)
    assert any(d.get("class") == "log-remainder" for d in p.details)
    assert all("open" not in d for d in p.details if d.get("class") == "detail-group")
    assert len(example().narrative.causes) == 2
    assert "存在しない" not in html
    assert "존재하지 않는 근거 참조" in html


def test_plain_text_keeps_source_and_original_logs():
    text = render_text(example())
    assert "10.0.1.23" in text and "/es/prod.log" in text
    assert "raw-14 <tag>&" in text
    assert text.index("핵심 요약") < text.index("타임라인")


def test_missing_raw_and_unverified_report_are_explicit():
    e = Evidence(
        evidence_id="E-1",
        event_time=T0,
        source=EvidenceSource.SLOWLOG,
        message="summary",
    )
    r = LogAnalysisReport(
        incident_id="I",
        analyzed_from=T0,
        analyzed_to=T0,
        verification_issues=("unsupported claim",),
    )
    html = render_report(
        to_incident_analysis_report(r, Observations(), [e]),
        analysis_failed=True,
        gaps=("SSH 수집 실패",),
    )
    assert "원문 미확보" in html and "수집 위치 미확인" in html
    assert "미검증" in html and "unsupported claim" in html
    assert "SSH 수집 실패" in html


def timeline_html(report):
    html = render_report(report)
    return html.split('id="timeline"', 1)[1].split('id="query-ranking"', 1)[0]


def test_timeline_event_is_compact_without_raw_or_evidence_blocks():
    timeline = timeline_html(example())
    assert 'class="timeline-event' in timeline
    assert 'class="raw"' not in timeline and "<pre" not in timeline
    assert "evidence-block" not in timeline and "timeline-observations" not in timeline
    assert "raw-0" not in timeline and "/es/prod.log" not in timeline
    assert "●" not in timeline


def test_timeline_refs_are_links_to_rendered_evidence():
    report = example()
    html = render_report(report)
    timeline = timeline_html(report)
    hrefs = re.findall(r'<a href="#(evidence-[0-9a-f]+)">E-&lt;1&gt;</a>', timeline)
    assert hrefs and f'id="{hrefs[0]}"' in html
    assert html.count(f'id="{hrefs[0]}"') == 1


def test_analysis_note_only_when_interpretations_exist():
    assert 'class="analysis-note"' in timeline_html(example())
    e = Evidence(
        evidence_id="E-1", event_time=T0, source=EvidenceSource.SLOWLOG, message="m"
    )
    obs = Observations(
        timeline=(TimelineRow(minute=T0, counts={"slowlog": 1}, search_rejected_max=1),)
    )
    plain = IncidentAnalysisReport(observations=obs, evidence=(e,))
    timeline = timeline_html(plain)
    assert "timeline-event" in timeline and "analysis-note" not in timeline
