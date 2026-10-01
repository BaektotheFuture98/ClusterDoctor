import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    SlowCandidate,
)
from cluster_doctor.incident_analysis_agent.model.report import SuspectPick
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import (
    EvidenceCitation,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment,
    Finding,
    IncidentAnalysisReport,
    Narrative,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html import (
    report_style,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
    anchor,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    render_report,
)
from tests.unit.incident_orchestrator_agent.service.test_report_design import example

T0 = datetime(2026, 10, 1, tzinfo=UTC)
NO_COUNTER = "현재 수집된 근거에서 명시적인 반증은 확인되지 않음"


def evidence(ref="E-1", message="rejected 12 requests", raw="RAW-LINE"):
    return Evidence(
        evidence_id=ref,
        event_time=T0,
        source=EvidenceSource.NODE_LOG,
        node_name="data-03",
        message=message,
        raw=raw,
    )


def report_with(narrative, *items):
    return IncidentAnalysisReport(
        observations=Observations(), narrative=narrative, evidence=tuple(items)
    )


def causes_of(html):
    return html.split('id="causes"', 1)[1].split("</section>", 1)[0]


def test_cause_lists_evidence_message_links_and_no_counter_phrase():
    e = evidence()
    cause = CauseAssessment(
        statement="검색 부하",
        confidence="high",
        supporting=(EvidenceCitation("E-1", e),),
    )
    html = render_report(report_with(Narrative(causes=(cause,)), e))
    causes = causes_of(html)
    assert f'<a href="#{anchor("E-1")}">E-1</a>' in causes
    assert "rejected 12 requests" in causes
    assert "RAW-LINE" not in causes and "<pre" not in causes
    assert "HIGH" in causes and "판단 근거" in causes
    assert NO_COUNTER in causes
    assert html.count(f'id="{anchor("E-1")}"') == 1


def test_counter_evidence_replaces_no_counter_phrase():
    e = evidence()
    cause = CauseAssessment(statement="x", contradicting=(EvidenceCitation("E-1", e),))
    causes = causes_of(render_report(report_with(Narrative(causes=(cause,)), e)))
    assert NO_COUNTER not in causes and "반증" in causes
    assert "rejected 12 requests" in causes


def test_dangling_cause_ref_is_marked_not_linked():
    cause = CauseAssessment(
        statement="x", supporting=(EvidenceCitation("E-missing", None),)
    )
    html = render_report(report_with(Narrative(causes=(cause,))))
    assert "E-missing 근거 없음(dangling)" in causes_of(html)
    assert f'href="#{anchor("E-missing")}"' not in html


def test_root_cause_fallback_uses_same_structure():
    n = Narrative(root_cause="디스크 부족", supporting=("로그 A",))
    causes = causes_of(render_report(report_with(n)))
    assert "cause-assessment" in causes and "디스크 부족" in causes
    assert "로그 A" in causes and NO_COUNTER in causes


def test_unverified_block_only_with_data_and_numbering():
    base = Narrative(root_cause="x")
    assert "미확인 사항" not in render_report(report_with(base))
    one = render_report(report_with(replace(base, unverified=("q1",))))
    assert "미확인 사항" in one and '<span class="marker">?</span>' in one
    many = render_report(report_with(replace(base, unverified=("q1", "q2", "q3"))))
    for n in ("01", "02", "03"):
        assert f'<span class="marker">{n}</span>' in many
    assert '<span class="marker">?</span>' not in many


def finding(title, refs, severity="Critical", detail=""):
    return Finding(
        severity=severity,
        title=title,
        citations=tuple(EvidenceCitation(r, None) for r in refs),
        detail=detail,
    )


def test_findings_covered_by_timeline_are_omitted_and_section_vanishes():
    report = example()
    covered = finding("이미 표시됨", ["E-<1>"])
    shown = finding("새 발견", ["E-<1>", "E-other"], detail="부연")
    plain = replace(report, narrative=replace(report.narrative, findings=(covered,)))
    assert 'id="findings"' not in render_report(plain)
    html = render_report(
        replace(report, narrative=replace(report.narrative, findings=(covered, shown)))
    )
    findings = html.split('id="findings"', 1)[1].split('id="timeline"', 1)[0]
    assert "새 발견" in findings and "이미 표시됨" not in findings
    assert "CRITICAL" in findings and "부연" in findings
    assert "cause-assessment" not in findings
    assert html.index('id="summary"') < html.index('id="findings"') < html.index(
        'id="timeline"'
    )


def test_findings_are_capped_at_four():
    items = tuple(finding(f"F{i}", [f"E-x{i}"]) for i in range(6))
    html = render_report(report_with(Narrative(findings=items)))
    findings = html.split('id="findings"', 1)[1].split('id="causes"', 1)[0]
    assert findings.count('class="finding-line"') == 4


def test_actions_numbered_list_and_absent_when_empty():
    n = Narrative(recommendations=("큐 확인", "쿼리 확인", "노드 확인", "추가 확인"))
    html = render_report(report_with(n))
    actions = html.split('id="actions"', 1)[1].split("</section>", 1)[0]
    for i, text in enumerate(n.recommendations, 1):
        assert f'<span class="marker">{i:02d}</span><span>{text}</span>' in actions
    assert 'id="actions"' not in render_report(report_with(Narrative()))


def entry(i, *, run_time="2", keyword=("kw",)):
    return QueryLogEntry(
        reg_date=T0 + timedelta(seconds=i),
        host="h",
        run_time=Decimal(run_time),
        success="Y",
        s_date=20260901,
        e_date=20260930,
        date_range=30,
        keyword=keyword,
        url="/s",
        cmd="search",
        service="web",
        env="prod",
        project="p",
        company="co",
        user="us",
        search_count=1,
        etc="",
        cluster="es",
    )


def candidate(cid, e):
    return SlowCandidate(
        candidate_id=cid,
        source="es_query_log",
        timestamp=e.reg_date,
        node=e.host,
        run_time=e.run_time,
        cmd=e.cmd,
        company=e.company,
        user=e.user,
    )


def ranking_html(rows, candidates=(), picks=()):
    r = IncidentAnalysisReport(
        observations=Observations(query_requests=rows, candidates=tuple(candidates)),
        narrative=Narrative(suspect_picks=tuple(picks)),
    )
    return render_report(r).split('id="query-ranking"', 1)[1].split("</section>", 1)[0]


def test_ranking_columns_and_values():
    html = ranking_html((entry(0, run_time="3"), entry(1, run_time="5")))
    assert re.findall(r"<th>(.*?)</th>", html) == [
        "ID",
        "Query",
        "Cmd",
        "Range",
        "Avg",
        "Max",
    ]
    assert ">30d<" in html and 'title="2026-09-01 ~ 2026-09-30"' in html
    assert "4.000초" in html and "5.000초" in html
    assert "co · us" in html and "<td>search</td>" in html
    assert 'class="picked"' not in html and '<td class="mono">—</td>' in html


def test_pick_row_is_highlighted_with_reason_only_on_exact_match():
    a = entry(0, run_time="9", keyword=("a",))
    b = entry(1, run_time="1", keyword=("b",))
    pick = SuspectPick(candidate_id="C1", reason="가장 느림")
    html = ranking_html((a, b), [candidate("C1", a)], [pick])
    assert html.count('class="picked"') == 1
    assert "C1" in html and "가장 느림" in html and "pick-reason" in html
    unmatched = replace(candidate("C1", a), run_time=Decimal("8"))
    miss = ranking_html((a, b), [unmatched], [pick])
    assert "C1" not in miss and "가장 느림" not in miss
    assert 'class="picked"' not in miss


def test_candidate_matching_two_groups_gets_no_id():
    a = entry(0, keyword=("a",))
    twin = replace(a, keyword=("b",))
    html = ranking_html((a, twin), [candidate("C1", a)], [SuspectPick("C1", "r")])
    assert "C1" not in html and 'class="picked"' not in html


def test_query_css_is_folded_into_report_style():
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html import (
        query_ranking,
    )

    assert not hasattr(query_ranking, "QUERY_CSS")
    assert "query-ranking-table" in report_style.REPORT_CSS
