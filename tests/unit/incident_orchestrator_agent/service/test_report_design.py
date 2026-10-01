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
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
    anchor,
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
        self.nav_links = []
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
        if tag == "a" and a.get("href", "").startswith("#"):
            (self.nav_links if self.in_nav else self.links).append(a["href"][1:])
        if tag == "details":
            self.details.append(a)


def test_summary_timeline_and_visible_source_logs_precede_details():
    html = render_report(example())
    assert (
        html.index('id="summary"')
        < html.index('id="timeline"')
        < html.index('id="causes"')
        < html.index('id="evidence"')
        < html.index('id="metadata"')
    )
    assert "10.0.1.23" in html and "/es/prod.log" in html
    assert "raw-0 &lt;tag&gt;&amp;" in html and "raw-14 &lt;tag&gt;&amp;" in html
    assert "<tag>" not in html
    assert "2026-10-01 09:00:00 KST" in html
    p = Links()
    p.feed(html)
    assert len(p.ids) == len(set(p.ids))
    assert set(p.links) <= set(p.ids)
    assert set(p.nav_links) <= set(p.ids)
    assert all("open" not in d for d in p.details)
    assert len(example().narrative.causes) == 2
    assert "存在しない" not in html
    causes = html.split('id="causes"', 1)[1].split('id="query-ranking"', 1)[0]
    assert "missing 근거 없음(dangling)" in causes
    assert anchor("missing") not in html


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
    evidence = html.split('id="evidence"', 1)[1].split('id="metadata"', 1)[0]
    assert 'class="evidence-message">summary<' in evidence
    assert "View raw log" not in evidence
    assert "<pre" not in evidence and "<dt>Host</dt>" not in evidence
    assert "<dt>Evidence verification</dt><dd>NOT_VERIFIED</dd>" in html
    assert "근거 검증이 완료되지 않았습니다" in html and "unsupported claim" in html
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


def evidence_html(report):
    html = render_report(report)
    return html.split('id="evidence"', 1)[1].split('id="metadata"', 1)[0]


def test_raw_is_rendered_exactly_once_in_evidence():
    report = example()
    html = render_report(report)
    assert html.count("raw-7 &lt;tag&gt;&amp;") == 1
    assert "raw-7" in evidence_html(report)
    assert html.count("raw-0 &lt;tag&gt;&amp;") == 1


def test_every_evidence_link_resolves_to_one_block():
    html = render_report(example())
    hrefs = set(re.findall(r'href="#(evidence-[0-9a-f]+)"', html))
    assert hrefs
    for h in hrefs:
        assert html.count(f'id="{h}"') == 1


def test_evidence_without_raw_has_no_raw_area_and_no_empty_fields():
    e = Evidence(
        evidence_id="E-9",
        event_time=T0,
        source=EvidenceSource.SLOWLOG,
        message="only a summary",
    )
    html = evidence_html(IncidentAnalysisReport(observations=Observations(), evidence=(e,)))
    assert "only a summary" in html
    assert "View raw log" not in html and "<pre" not in html
    for label in ("Host", "Table", "File Path", "Endpoint", "Query From", "Role", "Node", "Event Type"):
        assert f"<dt>{label}</dt>" not in html
    assert "<dt>Evidence ID</dt>" in html


def test_raw_kind_and_truncation_markers():
    q = Evidence(
        evidence_id="E-q",
        event_time=T0,
        source=EvidenceSource.SLOWLOG,
        message="m",
        raw="select 1",
        raw_kind="query",
        raw_truncated=True,
    )
    html = evidence_html(IncidentAnalysisReport(observations=Observations(), evidence=(q,)))
    assert 'class="raw raw-query"' in html and "잘림" in html
    assert "잘림" not in evidence_html(example())


def test_plain_text_copy_removed_but_details_preserved():
    html = render_report(example())
    assert 'class="source"' not in html and "리포트 평문" not in html
    assert 'id="limits"' not in html and 'id="details"' not in html
    assert "전체 분 단위 관측값" in html and "관측 상세" in html
    metadata = html.split('id="metadata"', 1)[1]
    assert "Analysis Metadata" in metadata and "PASSED" in metadata
    assert "Revision" not in metadata


def _two_event_report(status):
    e = Evidence(
        evidence_id="E-1", event_time=T0, source=EvidenceSource.SLOWLOG, message="m"
    )
    later = T0 + timedelta(minutes=30)
    log = LogAnalysisReport(
        incident_id="I",
        analyzed_from=T0,
        analyzed_to=later,
        summary="요약",
        verification_status=status,
        timeline=(
            TimelineEvent(at=T0, description="해석된 이벤트", evidence_refs=("E-1",)),
        ),
    )
    obs = Observations(
        timeline=(
            TimelineRow(minute=T0, counts={"slowlog": 1}, search_rejected_max=1),
            TimelineRow(minute=later, counts={"slowlog": 1}, search_rejected_max=1),
        )
    )
    return to_incident_analysis_report(log, obs, [e])


def test_analysis_note_is_per_event_and_only_for_passed_reports():
    passed = timeline_html(_two_event_report(VerificationStatus.PASSED))
    events = passed.split('<article class="timeline-event')[1:]
    assert len(events) == 2
    assert sum("analysis-note" in ev for ev in events) == 1
    assert "해석된 이벤트" in events[0] and "analysis-note" not in events[1]
    for status in (VerificationStatus.MISMATCH, VerificationStatus.NOT_VERIFIED):
        assert "analysis-note" not in timeline_html(_two_event_report(status))


def test_headline_and_issue_text_are_html_escaped():
    log = LogAnalysisReport(
        incident_id="I",
        analyzed_from=T0,
        analyzed_to=T0,
        summary="<script>alert(1)</script> & 거절",
        verification_status=VerificationStatus.MISMATCH,
        verification_issues=("<b>bad</b> claim",),
    )
    html = render_report(to_incident_analysis_report(log, Observations(), []))
    assert "<script>alert" not in html and "<b>bad</b>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt; &amp; 거절" in html
    assert "&lt;b&gt;bad&lt;/b&gt; claim" in html


def test_evidence_is_listed_in_time_order():
    early = Evidence(
        evidence_id="E-late-id", event_time=T0, source=EvidenceSource.SLOWLOG, message="a"
    )
    late = Evidence(
        evidence_id="E-a",
        event_time=T0 + timedelta(minutes=5),
        source=EvidenceSource.SLOWLOG,
        message="b",
    )
    html = evidence_html(
        IncidentAnalysisReport(observations=Observations(), evidence=(late, early))
    )
    assert html.index("E-late-id") < html.index("E-a")


def test_provenance_fields_appear_when_present():
    html = evidence_html(example())
    for label, value in (
        ("Method", "SSH"),
        ("Host", "10.0.1.23"),
        ("File Path", "/es/prod.log"),
        ("Query From", "2026-10-01 09:00:00 KST"),
        ("Query To", "2026-10-01 09:01:00 KST"),
        ("Collected At", "2026-10-01 09:00:00 KST"),
    ):
        assert f"<dt>{label}</dt><dd>{value}</dd>" in html


def test_message_equal_to_raw_single_line_is_shown_once():
    line = "single line failure"
    e = Evidence(
        evidence_id="E-1",
        event_time=T0,
        source=EvidenceSource.SLOWLOG,
        message=line,
        raw=line,
    )
    html = render_report(IncidentAnalysisReport(observations=Observations(), evidence=(e,)))
    assert "<dt>Message</dt>" not in html
    assert html.count(line) == 2  # summary line + raw block, no Message field
    assert "<pre" in html


def test_single_instant_title_is_representative_event_not_model_text():
    passed = timeline_html(_two_event_report(VerificationStatus.PASSED))
    first = passed.split('<article class="timeline-event')[1]
    h3 = first.split("<h3>")[1].split("</h3>")[0]
    assert "해석된 이벤트" not in h3
    assert "해석된 이벤트" in first.split("</h3>")[1]
