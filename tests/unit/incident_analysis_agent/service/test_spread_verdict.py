from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links, judge_spread


def _query(minute: int, second: int, seconds: str, *, cmd: str = "search", date_range: int = 1) -> QueryLogEntry:
    return QueryLogEntry(
        reg_date=datetime(2026, 10, 11, 19, minute, second, tzinfo=KST), host="h", run_time=Decimal(seconds),
        success="Y", s_date=0, e_date=0, date_range=date_range, keyword=(), target_host="192.168.1.30",
        index_name="a", conditions=(), cmd=cmd, service="", env="", project="", company="", user="",
        search_count=0, etc="", cluster="",
    )


def _window(during_seconds: str, count: int = 25) -> Observations:
    reference = [_query(2, i % 60, "0.20") for i in range(count)]
    during = [_query(3, i % 60, during_seconds) for i in range(count)]
    heavy = [_query(3, 18, "19.43", cmd="agg", date_range=365)]
    return Observations(query_requests=tuple(reference + during + heavy))


def test_other_requests_unchanged_means_specific():
    verdict = judge_spread(_window("0.25"))
    assert verdict.scope == "specific"
    assert verdict.slow_shapes == (("agg", 365, 1),)
    assert (verdict.reference_median, verdict.slow_minute_median) == (Decimal("0.20"), Decimal("0.25"))


def test_other_requests_slowed_means_cluster_wide():
    # 늘어난 형태가 2개 이상이어야 전반 지연이므로 두 번째 형태도 함께 올린다.
    window = _window("1.00")
    more = [_query(2, i % 60, "0.20", cmd="count") for i in range(25)] + \
           [_query(3, i % 60, "1.00", cmd="count") for i in range(25)]
    assert judge_spread(Observations(query_requests=window.query_requests + tuple(more))).scope == "cluster_wide"
    assert judge_spread(window).scope == "specific"


def test_too_few_samples_is_undetermined():
    assert judge_spread(_window("0.25", count=5)).scope == "undetermined"


def test_no_slow_request():
    assert judge_spread(Observations(query_requests=(_query(2, 0, "0.20"),))).scope == "no_slow"


def test_cross_links_carry_verdict():
    assert build_cross_links(_window("0.25")).verdict.scope == "specific"
