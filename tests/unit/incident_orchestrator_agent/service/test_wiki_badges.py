from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportRecommendation,
    RootCause,
    WikiPatternRef,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.wiki_label import (
    wiki_label,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import (
    render_layout,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
)

AT = datetime(2026, 1, 1, 14, 0, tzinfo=KST)
PATTERN_ID = "massive-agg-query-timeout"
TITLE = "대량의 Aggregation 쿼리로 인한 타임아웃"
PATTERNS = (WikiPatternRef(PATTERN_ID, TITLE),)
LABEL = f"{PATTERN_ID} — {TITLE}"


def _log_report(*, with_wiki=True):
    ref = PATTERN_ID if with_wiki else ""
    return LogAnalysisReport(
        incident_id="i-1", analyzed_from=AT, analyzed_to=AT, summary="요약",
        root_causes=(RootCause(statement="원인 문장", mechanism="설명", wiki_ref=ref),),
        recommendations=(ReportRecommendation(text="조치 문장", cause_index=0, wiki_ref=ref),),
        wiki_patterns=PATTERNS if with_wiki else (),
    )


def _delivered(**kwargs):
    return to_incident_analysis_report(_log_report(**kwargs), Observations(), [])


def test_label_has_title_when_known():
    assert wiki_label(PATTERN_ID, PATTERNS) == LABEL


def test_label_falls_back_to_the_id():
    assert wiki_label(PATTERN_ID, ()) == PATTERN_ID
    assert wiki_label(PATTERN_ID, (WikiPatternRef(PATTERN_ID, PATTERN_ID),)) == PATTERN_ID


def test_label_is_empty_without_a_ref():
    assert wiki_label("", PATTERNS) == ""
    assert wiki_label("  ", PATTERNS) == ""


def test_mapping_carries_cause_ref_and_patterns():
    delivered = _delivered()
    assert delivered.narrative.causes[0].wiki_ref == PATTERN_ID
    assert delivered.narrative.recommendations[0].wiki_ref == PATTERN_ID
    assert delivered.wiki_patterns == PATTERNS


def test_html_shows_the_badge_on_the_cause_and_the_recommendation():
    html = render_layout(_delivered(), AT, gaps=(), analysis_failed=False, css="")
    assert html.count(f'<span class="wiki-badge">위키: {LABEL}</span>') == 2
    assert html.index("원인 문장") < html.index(LABEL)


def test_html_has_no_badge_without_wiki_refs():
    html = render_layout(_delivered(with_wiki=False), AT, gaps=(), analysis_failed=False, css="")
    assert 'class="wiki-badge"' not in html


def test_text_fallback_shows_wiki_lines():
    text = render_text(_delivered())
    assert f"위키 근거: {LABEL}" in text
    assert f"확인·조치: 조치 문장 (위키: {LABEL})" in text


def test_text_fallback_has_no_wiki_lines_without_refs():
    text = render_text(_delivered(with_wiki=False))
    assert "위키" not in text
