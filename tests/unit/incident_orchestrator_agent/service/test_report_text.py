from datetime import datetime, timezone
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.basemodel.observations import SlowCandidate
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    offender_lines,
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
