from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.observation.correlation import judge_spread

CMDS = ("search", "count", "agg", "bulk")


def _query(minute: int, second: int, seconds: str, cmd: str) -> QueryLogEntry:
    return QueryLogEntry(
        reg_date=datetime(2026, 10, 11, 19, minute, second, tzinfo=KST), host="h", run_time=Decimal(seconds),
        success="Y", s_date=0, e_date=0, date_range=1, keyword=(), target_host="192.168.1.30",
        index_name="a", conditions=(), cmd=cmd, service="", env="", project="", company="", user="",
        search_count=0, etc="", cluster="",
    )


def _window(cmds, *, quiet: str, slow_minute: dict[str, str], count: int = 25, outlier_cmd: str = "bulk"):
    """minute 2는 느린 요청이 없는 분, minute 3은 outlier가 하나 있는 분이다."""
    rows = []
    for cmd in cmds:
        rows += [_query(2, i, quiet, cmd) for i in range(count)]
        rows += [_query(3, i, slow_minute.get(cmd, quiet), cmd) for i in range(count)]
    rows.append(_query(3, 59, "19.43", outlier_cmd))
    return Observations(query_requests=tuple(rows))


def test_shape_with_one_slow_outlier_stays_in_comparison():
    # bulk는 느린 요청이 1건뿐이어도 비교에서 빠지지 않고, 그 요청을 뺀 나머지로 비교된다.
    verdict = judge_spread(_window(("bulk",), quiet="0.20", slow_minute={"bulk": "1.00"}))
    assert [s[0] for s in verdict.slow_shapes] == ["bulk"]
    assert verdict.compared_shapes == 1
    assert len(verdict.widened_shapes) == 1


def test_single_widened_shape_is_specific_even_if_it_is_the_only_one_compared():
    verdict = judge_spread(_window(("bulk",), quiet="0.20", slow_minute={"bulk": "1.00"}))
    assert verdict.scope == "specific"


def test_two_widened_shapes_out_of_two_is_cluster_wide():
    verdict = judge_spread(_window(("search", "bulk"), quiet="0.20", slow_minute={"search": "1.00", "bulk": "1.00"}))
    assert (verdict.compared_shapes, len(verdict.widened_shapes), verdict.scope) == (2, 2, "cluster_wide")


def test_two_widened_shapes_without_majority_is_specific():
    rise = {"search": "1.00", "bulk": "1.00"}
    verdict = judge_spread(_window(("search", "bulk", "count", "agg"), quiet="0.20", slow_minute=rise))
    assert (verdict.compared_shapes, len(verdict.widened_shapes), verdict.scope) == (4, 2, "specific")


def test_outlier_itself_does_not_inflate_during_median():
    verdict = judge_spread(_window(("bulk",), quiet="0.20", slow_minute={"bulk": "0.25"}))
    assert verdict.scope == "specific"
    assert verdict.slow_minute_median == Decimal("0.25")


def test_many_shapes_rising_is_cluster_wide():
    rise = {"search": "1.00", "count": "1.00", "agg": "1.00"}
    verdict = judge_spread(_window(CMDS, quiet="0.20", slow_minute=rise))
    assert verdict.scope == "cluster_wide"
    assert (verdict.compared_shapes, len(verdict.widened_shapes)) == (4, 3)


def test_only_slow_shape_changed_is_specific():
    verdict = judge_spread(_window(CMDS, quiet="0.20", slow_minute={"bulk": "1.00"}))
    assert verdict.scope == "specific"
    assert (verdict.compared_shapes, len(verdict.widened_shapes)) == (4, 1)


def test_not_enough_samples_is_undetermined():
    verdict = judge_spread(_window(CMDS, quiet="0.20", slow_minute={"search": "1.00"}, count=5))
    assert verdict.scope == "undetermined"
    assert verdict.compared_shapes == 0
    assert verdict.slow_shapes
