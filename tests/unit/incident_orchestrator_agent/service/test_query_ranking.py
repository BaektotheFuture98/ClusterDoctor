from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.query_url import request_fields
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    merge_observations,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.observation.builder import (
    ObservationBuilder,
)
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests

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
        **request_fields("/search"),
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
    skipped = {"provenance", "target_host", "index_name", "conditions"}
    data = {
        f.name: getattr(entry, f.name) for f in fields(entry) if f.name not in skipped
    } | {"url": "/search"}
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





def test_ranking_keeps_same_partial_keywords_as_individual_executions():
    rows=(request(duration='10'),request(1,duration='2'),request(2,duration='9',cmd='bulk'))
    ranked=rank_query_requests(tuple(rows))
    assert [r.execution_seconds for r in ranked]==[Decimal('10'),Decimal('9'),Decimal('2')]
    assert len(ranked)==3 and ranked[1].record.cmd=='bulk'


def test_invalid_runtimes_remain_records_but_do_not_win():
    ranked=rank_query_requests(tuple(request(i,duration=v) for i,v in enumerate(('NaN','Infinity','-1','0','2'))))
    assert [r.execution_seconds for r in ranked]==[Decimal(2),Decimal(0),None,None,None]
    assert len(ranked)==5


def test_table_has_ten_individual_rows_no_average_or_internal_ids():
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_ranking import render_query_ranking
    rows=tuple(request(i,duration=str(i+1)) for i in range(12))
    html=render_query_ranking(rows)
    assert html.count('class="execution-row"')==10
    assert '<th>실행시간</th>' in html and '12s' in html
    assert 'Avg' not in html and '<th>ID</th>' not in html
    assert '&lt;keyword&gt;' in html


def test_row_without_extracted_conditions_shows_placeholder():
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_ranking import render_query_ranking
    entry=request(0)
    assert entry.conditions==()
    assert '조건 미추출' in render_query_ranking((entry,))
