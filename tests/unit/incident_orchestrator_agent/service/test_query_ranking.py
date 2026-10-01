from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    merge_observations,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.observation.builder import (
    ObservationBuilder,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_ranking import (
    query_ranking,
)

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def request(i=0, *, duration="2", cmd="search", keywords=("<keyword>",)):
    return QueryLogEntry(
        reg_date=T0 + timedelta(seconds=i),
        host="es-host",
        run_time=Decimal(duration),
        success="Y",
        cmd=cmd,
        service="web",
        env="prod",
        project="project",
        cluster="es",
        keyword=keywords,
        s_date=20260901,
        e_date=20260930,
        date_range=30,
        search_count=42,
        url="/search",
        etc="",
        company="company",
        user="user",
    )


def test_full_requests_survive_state_roundtrip_and_overlapping_windows():
    b = ObservationBuilder(TimeRange(T0, T0 + timedelta(minutes=1)))
    rows = [request(i) for i in range(9)] + [request(0)]
    b.record_log_observations(rows)
    obs = b.to_observations()
    assert len(obs.candidates) == 5
    assert len(obs.query_requests) == 10
    restored = ObservationBuilder.from_state(b.window, b.state_update())
    restored.record_log_observations(rows)
    assert len(restored.to_observations().query_requests) == 10
    merged = merge_observations(obs, restored.to_observations())
    assert len(merged.query_requests) == 10
    assert merged.query_requests[0].cmd == "search"
    assert merged.query_requests[0].keyword == ("<keyword>",)


def test_ranking_uses_mean_separates_cmd_and_keeps_keyword_combination():
    rows = (
        request(0, duration="10"),
        request(1, duration="2"),
        request(2, duration="9", cmd="agg"),
        request(3, duration="1", keywords=("a", "b")),
    )
    ranks = query_ranking(rows)
    assert [r.cmd for r in ranks[:2]] == ["agg", "search"]
    assert ranks[1].average == Decimal(6)
    assert ranks[1].total == Decimal(12)
    assert ranks[1].count == 2
    assert ranks[1].first == T0 and ranks[1].last == T0 + timedelta(seconds=1)
    assert ranks[2].keywords == ("a", "b")
    assert ranks[2].count == 1
    assert query_ranking((request(cmd="custom"),))[0].query_type == "기타"


def test_invalid_duration_is_not_zero_and_identity_dimensions_stay_separate():
    rows = (
        request(duration="NaN"),
        replace(request(1), user="other"),
        replace(request(2), company="other"),
        request(3, duration="-1"),
    )
    ranks = query_ranking(rows)
    missing = next(r for r in ranks if r.user == "user" and r.company == "company")
    assert missing.average is None and missing.total is None
    assert missing.valid_count == 0 and missing.count == 2
    assert len(ranks) == 3


def test_report_ranking_keeps_cmd_reg_date_identity_and_source():
    from cluster_doctor.incident_analysis_agent.model.evidence import EvidenceProvenance
    from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
        IncidentAnalysisReport,
    )
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
        render_report,
    )
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
        render_text,
    )

    r = replace(
        request(cmd="agg"),
        provenance=EvidenceProvenance(
            method="clickhouse", table="db.log", excerpt=True
        ),
    )
    report = IncidentAnalysisReport(observations=Observations(query_requests=(r,)))
    html = render_report(report)
    assert 'id="query-ranking"' in html
    assert "&lt;keyword&gt;" in html and "<keyword>" not in html
    assert "company" in html and "user" in html and "agg" in html
    assert "reg_date" in html and "2026-10-01 09:00:00 KST" in html
    assert "db.log" in html and "부분 집계" in html
    assert ">30d<" in html and "2026-09-01 ~ 2026-09-30" in html
    assert "keyword=['<keyword>']" in render_text(report)


def test_subagent_result_keeps_full_request_snapshot():
    from types import SimpleNamespace

    from cluster_doctor.incident_analysis_agent.agent.state import AnalysisAgentState
    from cluster_doctor.incident_analysis_agent.agent.subagent import _observations

    rows = (request(),)
    state = {
        "request": SimpleNamespace(
            analysis_window=TimeRange(T0, T0 + timedelta(minutes=1))
        ),
        "query_requests": rows,
    }
    assert "query_requests" in AnalysisAgentState.__annotations__
    assert _observations(state).query_requests == rows


def test_fetch_marks_partial_query_log_records(monkeypatch):
    from types import SimpleNamespace

    from cluster_doctor.incident_analysis_agent.datasource.clickhouse import query_log

    monkeypatch.setattr(query_log, "MAX_ROWS_PER_SEGMENT_PER_SOURCE", 1)
    entry = replace(request(), keyword=("keyword",), cmd="agg")
    data = {
        f.name: getattr(entry, f.name)
        for f in fields(entry)
        if f.name not in ("provenance", "additional_fields")
    }
    client = SimpleNamespace(
        query=lambda *args, **kwargs: SimpleNamespace(
            column_names=tuple(data), result_rows=[tuple(data.values())]
        )
    )
    fetched = query_log.fetch(
        client, "db.log", TimeRange(T0, T0 + timedelta(minutes=1))
    )
    assert fetched[0].provenance.excerpt
    assert fetched[0].timestamp == T0 and fetched[0].cmd == "agg"
    assert fetched[0].keyword == ("keyword",)


def test_search_period_is_visible_and_different_periods_are_ranked_separately():
    from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
        IncidentAnalysisReport,
    )
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
        render_report,
    )
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
        render_text,
    )

    rows = (
        request(),
        replace(
            request(1), s_date=20260924, e_date=20260930, date_range=7, success="N"
        ),
    )
    assert len(query_ranking(rows)) == 2
    report = IncidentAnalysisReport(observations=Observations(query_requests=rows))
    html = render_report(report)
    assert "2026-09-01" in html and "2026-09-30" in html
    assert ">30d<" in html and ">7d<" in html
    assert "개별 요청" not in html and "/search" not in html
    assert "시작일·종료일 정보가 없습니다" not in html
    assert "date_range=30일" in render_text(report)
