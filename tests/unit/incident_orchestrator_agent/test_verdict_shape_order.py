from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.observation.correlation import (
    CrossLinks,
    Verdict,
    judge_spread,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.cross_links_text import verdict_line


def _q(seconds, *, cmd="agg", days=365):
    return QueryLogEntry(
        reg_date=datetime(2026, 10, 11, 19, 0, 5, tzinfo=KST), host="h", run_time=Decimal(seconds),
        success="Y", s_date=0, e_date=0, date_range=days, keyword=(), target_host="192.168.1.30",
        index_name="a", conditions=(), cmd=cmd, service="", env="", project="", company="", user="",
        search_count=0, etc="", cluster="",
    )


def _links(verdict):
    return CrossLinks(slow_threshold=Decimal(10), overlaps=(), minutes=(), nodes=(), profile=(),
                      verdict=verdict, profile_total=0)


def test_slow_shapes_ordered_by_slow_count_not_lexicographically():
    rows = [_q("11", days=181)] + [_q("12", days=400)] * 3 + [_q("30", cmd="bulk", days=1)]
    shapes = judge_spread(Observations(query_requests=tuple(rows))).slow_shapes
    assert [s[0:2] for s in shapes] == [("agg", 400), ("bulk", 1), ("agg", 181)]


def test_verdict_line_reports_omitted_shapes():
    shapes = tuple(("agg", d, 1) for d in range(1, 6))
    line = verdict_line(_links(Verdict("specific", shapes, Decimal("0.2"), Decimal("0.3"), slow_counts=(5, 4, 3, 2, 1)))).detail
    assert "주요 형태: agg·1일·인덱스 1개 5건, agg·2일·인덱스 1개 4건, agg·3일·인덱스 1개 3건 외 2개 형태" in line
    assert "agg·4일" not in line


def test_verdict_line_without_omission_has_no_suffix():
    shapes = (("agg", 1, 1), ("agg", 2, 1), ("agg", 3, 1))
    for scope in ("specific", "undetermined"):
        assert " 외 " not in verdict_line(_links(Verdict(scope, shapes, Decimal("0.2"), Decimal("0.3"), slow_counts=(3, 2, 1)))).detail


def test_undetermined_line_reports_omitted_shapes():
    shapes = tuple(("agg", d, 1) for d in range(1, 6))
    assert "외 2개 형태" in verdict_line(_links(Verdict("undetermined", shapes, None, None, slow_counts=(5, 4, 3, 2, 1)))).detail
