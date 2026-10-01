from datetime import datetime, timezone
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    SlowCandidate,
    TimelineRow,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    offender_lines,
    render_text,
)

_UTC = timezone.utc


def _query_candidate(candidate_id: str, company: str, user: str, run_time: str) -> SlowCandidate:
    return SlowCandidate(
        candidate_id=candidate_id,
        source="es_query_log",
        timestamp=datetime(2026, 9, 30, 15, 22, tzinfo=_UTC),
        run_time=Decimal(run_time),
        cmd="agg",
        company=company,
        user=user,
    )


def test_offender_lines_ranks_by_total_runtime_desc():
    candidates = (
        _query_candidate("C1", "A사", "u1@a.com", "5.0"),
        _query_candidate("C2", "B사", "u2@b.com", "20.0"),
    )
    lines = offender_lines(candidates)
    assert lines[0].startswith("가해자 집계 (회사 2곳, es_query_log 기준)")
    body = "\n".join(lines)
    assert body.index("[1] B사") < body.index("[2] A사")
    assert "합계 20.0s, 최고 20.0s" in body


def test_offender_lines_excludes_slowlog_candidates():
    candidates = (
        SlowCandidate(
            candidate_id="C1",
            source="slowlog",
            timestamp=datetime(2026, 9, 30, 15, 22, tzinfo=_UTC),
            took="10s",
        ),
    )
    assert offender_lines(candidates) == []


def test_offender_lines_groups_missing_company_as_unknown():
    candidates = (
        _query_candidate("C1", "", "", "3.0"),
        _query_candidate("C2", "", "", "4.0"),
    )
    lines = offender_lines(candidates)
    body = "\n".join(lines)
    assert "[1] 미상 — 2건" in body


def test_offender_lines_caps_companies_and_notes_the_cut():
    candidates = tuple(
        _query_candidate(f"C{i}", f"회사{i}", f"user{i}@x.com", str(i + 1))
        for i in range(12)
    )
    lines = offender_lines(candidates)
    assert lines[0].startswith("가해자 집계 (회사 12곳")
    assert lines[-1] == "… 외 2곳"


def test_offender_lines_caps_users_per_company_and_notes_the_cut():
    candidates = tuple(
        _query_candidate(f"C{i}", "한 회사", f"user{i}@x.com", "1.0")
        for i in range(7)
    )
    lines = offender_lines(candidates)
    user_lines = [line for line in lines if line.strip().startswith("user:")]
    assert len(user_lines) == 5
    assert any(line.strip() == "… 외 2명" for line in lines)


def test_render_text_excludes_offender_grouping():
    obs = Observations(
        candidates=(_query_candidate("C1", "마크로밀엠브레인", "mqtai02@embrain.com", "28.66"),)
    )
    report = IncidentAnalysisReport(observations=obs, evidence=())
    text = render_text(report)
    assert "가해자 집계" not in text
    assert "느린 개별 실행 로그" in text


def test_timeline_card_lines_include_source_grouped_citations():
    minute = datetime(2026, 9, 30, 15, 22, tzinfo=_UTC)
    row = TimelineRow(minute=minute, counts={"slowlog": 1}, search_rejected_max=1)
    obs = Observations(timeline=(row,))
    evidence = (
        Evidence(
            evidence_id="E-1",
            event_time=minute,
            source=EvidenceSource.SLOWLOG,
            message="slowlog took=12s",
        ),
        Evidence(
            evidence_id="E-2",
            event_time=minute,
            source=EvidenceSource.NODE_METRIC,
            event_type="node_metric_rejected",
            message="search rejected 1",
        ),
    )
    report = IncidentAnalysisReport(observations=obs, evidence=evidence)
    text = render_text(report)
    assert "slowlog took=12s" in text and "원문 없음" in text
    assert "E-1" not in text and "E-2" not in text
