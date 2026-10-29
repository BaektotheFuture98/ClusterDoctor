"""AI 해석이 없거나 숨겨져도 코드 판정과 분석 구간이 리포트에 남는지 확인한다."""
import html
import re
from datetime import datetime

import pytest

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment,
    IncidentAnalysisReport,
    Narrative,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import (
    analysis_span,
    display_narrative,
    result_caveats,
    withheld_reason,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_renderer import (
    render_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
)

NO_LOG_VERDICT = "수집된 쿼리 실행 로그가 없습니다."
NOT_GENERATED = "AI 분석을 생성하지 못해 관측값만 표시합니다."
MISMATCHED = "AI 분석에 원문과 맞지 않는 부분이 남아 관측값만 표시합니다."
PARTIAL_ANALYSIS = "일부 분석 실패 — 일부 구간 또는 단계가 끝나지 않은 상태의 해석입니다."
PARTIAL = "일부 검증 — 근거 대조를 끝까지 마치지 못한 해석입니다."
FALLBACK_SPAN_HINT = "AI 분석 구간이 없어 수집 요청 구간 기준으로 표시했습니다."
PLACEHOLDER_CAUSE = "수집된 자료로 지연 원인을 확정할 수 없습니다."
ALL_NOTICES = (NOT_GENERATED, MISMATCHED, PARTIAL, PARTIAL_ANALYSIS)

T = lambda h, m: datetime(2026, 10, 23, h, m, tzinfo=KST)  # noqa: E731
REQUESTED = ((T(18, 54), T(19, 4)),)


def _text(page: str, section_id: str) -> str:
    match = re.search(rf'<section id="{section_id}">.*?</section>', page, re.S)
    assert match, f"{section_id} 섹션이 없다"
    return html.unescape(" ".join(re.sub(r"<[^>]+>", " ", match.group(0)).split()))


def _page(report: IncidentAnalysisReport, *, analysis_failed: bool = False) -> str:
    return render_report(report, generated_at=T(23, 0), analysis_failed=analysis_failed)


def _ai_report(*, status: str = "PASSED", **overrides) -> IncidentAnalysisReport:
    fields = dict(
        observations=Observations(requested=REQUESTED),
        narrative=Narrative(causes=(CauseAssessment(statement="AI가 본 원인", mechanism="AI 메커니즘"),)),
        verification_status=status,
        summary="AI 요약 문장",
        analyzed_from=T(18, 55),
        analyzed_to=T(19, 1),
    )
    fields.update(overrides)
    return IncidentAnalysisReport(**fields)


SECTION_ORDER = ("overview", "incident-analysis", "recommendations", "query-trend", "timeline", "evidence")


def test_sections_put_conclusion_and_actions_before_supporting_data():
    narrative = Narrative(
        causes=(CauseAssessment(statement="AI가 본 원인"),),
        recommendations=("대상 쿼리의 호출 빈도를 줄인다",),
    )
    page = _page(_ai_report(narrative=narrative))

    shown = re.findall(r'id="(overview|incident-analysis|recommendations|query-trend|timeline|evidence)"', page)

    assert shown[:3] == ["overview", "incident-analysis", "recommendations"]
    assert shown == [section for section in SECTION_ORDER if section in shown]
    assert "future-improvements" not in page


def test_code_verdict_and_notice_remain_when_ai_analysis_is_missing():
    report = IncidentAnalysisReport(observations=Observations(requested=REQUESTED))

    analysis = _text(_page(report, analysis_failed=True), "incident-analysis")

    assert NO_LOG_VERDICT in analysis
    assert NOT_GENERATED in analysis
    assert PLACEHOLDER_CAUSE in analysis


def test_overview_falls_back_to_requested_span_when_ai_analysis_is_missing():
    report = IncidentAnalysisReport(observations=Observations(requested=REQUESTED))

    overview = _text(_page(report, analysis_failed=True), "overview")

    assert "2026-10-23 18:54:00+09:00 KST" in overview
    assert "2026-10-23 19:04:00+09:00 KST" in overview
    assert "10분" in overview
    assert FALLBACK_SPAN_HINT in overview
    assert "미확인" not in overview


def test_overview_falls_back_when_ai_report_lacks_one_end_of_its_span():
    report = _ai_report(analyzed_to=None)

    overview = _text(_page(report), "overview")

    assert "2026-10-23 18:54:00+09:00 KST" in overview
    assert "2026-10-23 19:04:00+09:00 KST" in overview
    assert FALLBACK_SPAN_HINT in overview


def test_overview_shows_first_observed_time_not_window_start():
    onset = T(19, 0).replace(second=37)
    overview = _text(_page(_ai_report(onset=onset, analyzed_to=T(19, 6))), "overview")

    assert "발생 및 인지 일시 2026-10-23 19:00:37+09:00 KST" in overview
    assert "18:55" not in overview
    assert "총 장애 시간 5분" in overview


def test_overview_stays_unknown_without_any_span():
    overview = _text(_page(IncidentAnalysisReport(observations=Observations()), analysis_failed=True), "overview")

    assert overview.count("미확인") == 3
    assert FALLBACK_SPAN_HINT not in overview


def test_verified_ai_analysis_shows_code_verdict_summary_and_causes_without_notice():
    page = _page(_ai_report())
    analysis = _text(page, "incident-analysis")

    assert NO_LOG_VERDICT in analysis
    assert "AI 요약 문장" in analysis
    assert "AI가 본 원인" in analysis
    assert PLACEHOLDER_CAUSE not in analysis
    assert not any(text in analysis for text in ALL_NOTICES)
    overview = _text(page, "overview")
    assert "2026-10-23 18:55:00+09:00 KST" in overview
    assert FALLBACK_SPAN_HINT not in overview


def test_partially_verified_ai_analysis_is_shown_with_a_notice():
    page = _page(_ai_report(status="NOT_VERIFIED"))
    analysis = _text(page, "incident-analysis")

    assert NO_LOG_VERDICT in analysis
    assert PARTIAL in analysis
    assert "AI 요약 문장" in analysis
    assert "AI가 본 원인" in analysis
    assert PLACEHOLDER_CAUSE not in analysis
    assert not any(text in analysis for text in (NOT_GENERATED, MISMATCHED, PARTIAL_ANALYSIS))


@pytest.mark.parametrize(
    ("status", "analysis_failed", "notices"),
    [
        ("PASSED", True, (PARTIAL_ANALYSIS,)),
        ("NOT_VERIFIED", True, (PARTIAL, PARTIAL_ANALYSIS)),
    ],
)
def test_ai_analysis_of_a_partially_failed_run_is_shown_with_notices(status, analysis_failed, notices):
    page = _page(_ai_report(status=status), analysis_failed=analysis_failed)
    analysis = _text(page, "incident-analysis")

    assert NO_LOG_VERDICT in analysis
    assert all(note in analysis for note in notices)
    assert "AI 요약 문장" in analysis
    assert "AI가 본 원인" in analysis
    assert PLACEHOLDER_CAUSE not in analysis
    assert not any(text in analysis for text in (NOT_GENERATED, MISMATCHED))


@pytest.mark.parametrize("analysis_failed", [False, True])
def test_mismatched_ai_analysis_is_hidden_with_its_reason(analysis_failed):
    page = _page(_ai_report(status="MISMATCH"), analysis_failed=analysis_failed)
    analysis = _text(page, "incident-analysis")

    assert MISMATCHED in analysis
    assert NO_LOG_VERDICT in analysis
    assert PARTIAL not in analysis
    assert PARTIAL_ANALYSIS not in analysis
    assert "AI 요약 문장" not in analysis
    assert "AI가 본 원인" not in analysis
    assert PLACEHOLDER_CAUSE in analysis


def test_ai_span_wins_over_requested_span():
    report = _ai_report()

    assert analysis_span(report) == (T(18, 55), T(19, 1))


def test_requested_span_covers_all_requested_ranges():
    report = IncidentAnalysisReport(
        observations=Observations(requested=((T(19, 0), T(19, 5)), (T(18, 54), T(18, 58)), (T(19, 2), T(19, 4))))
    )

    assert analysis_span(report) == (T(18, 54), T(19, 5))


def test_no_span_without_ai_report_and_requests():
    assert analysis_span(IncidentAnalysisReport(observations=Observations())) == (None, None)


@pytest.mark.parametrize(
    ("report", "reason_shown"),
    [
        (_ai_report(), False),
        (_ai_report(status="NOT_VERIFIED"), False),
        (_ai_report(status="MISMATCH"), True),
        (IncidentAnalysisReport(observations=Observations()), True),
    ],
)
def test_display_narrative_follows_withheld_reason(report, reason_shown):
    narrative = display_narrative(report)

    assert bool(withheld_reason(report)) is reason_shown
    assert (narrative is report.narrative) is (not reason_shown)


@pytest.mark.parametrize(
    ("status", "analysis_failed", "expected"),
    [
        ("PASSED", False, ()),
        ("PASSED", True, (PARTIAL_ANALYSIS,)),
        ("NOT_VERIFIED", False, (PARTIAL,)),
        ("NOT_VERIFIED", True, (PARTIAL, PARTIAL_ANALYSIS)),
    ],
)
def test_result_caveats_name_unfinished_verification_and_failed_analysis(status, analysis_failed, expected):
    assert result_caveats(_ai_report(status=status), analysis_failed=analysis_failed) == expected


def test_plain_text_report_states_why_ai_analysis_is_missing():
    report = IncidentAnalysisReport(observations=Observations(requested=REQUESTED))

    text = render_text(report, analysis_failed=True)

    assert NO_LOG_VERDICT in text
    assert NOT_GENERATED in text


def test_plain_text_report_shows_ai_analysis_of_a_partially_failed_run_with_a_notice():
    text = render_text(_ai_report(), analysis_failed=True)

    assert PARTIAL_ANALYSIS in text
    assert "AI가 본 원인" in text


def test_plain_text_report_hides_mismatched_analysis_and_states_why():
    text = render_text(_ai_report(status="MISMATCH"))

    assert MISMATCHED in text
    assert "AI가 본 원인" not in text


def test_plain_text_report_marks_partially_verified_analysis():
    text = render_text(_ai_report(status="NOT_VERIFIED"))

    assert PARTIAL in text
    assert "AI가 본 원인" in text
