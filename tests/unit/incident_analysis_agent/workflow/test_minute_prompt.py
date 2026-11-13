from datetime import datetime

import pytest

from cluster_doctor.incident_analysis_agent.datasource.clickhouse import (
    master_log,
    query_log,
    slowlog,
)
from cluster_doctor.incident_analysis_agent.datasource.ssh import node_log
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    MinuteBucket,
    MinuteResult,
    RawRecord,
    SelectedRecord,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.prompt import (
    build_map_prompt,
    build_reduce_prompt,
)

_MINUTE = datetime(2026, 11, 12, 10, 0, tzinfo=KST)
_RECORD = RawRecord(record_id=1, event_time=_MINUTE, line="line")
_SPECS = (slowlog.SPEC, query_log.SPEC, master_log.SPEC, node_log.SPEC)


def _map(spec):
    return build_map_prompt(spec, MinuteBucket(minute=_MINUTE, records=(_RECORD,)))


def _reduce(spec):
    result = MinuteResult(minute=_MINUTE, selected=(SelectedRecord(record_id=1),))
    return build_reduce_prompt(spec, [result], {1: _RECORD}, limit=25)


@pytest.mark.parametrize("spec", _SPECS, ids=lambda spec: spec.source.value)
def test_window_criteria_go_to_reduce_only(spec):
    if not spec.what_stands_out_in_window:
        pytest.skip("구간 기준이 없는 소스")
    assert spec.what_stands_out_in_window not in _map(spec)
    assert spec.what_stands_out_in_window in _reduce(spec)


@pytest.mark.parametrize("spec", _SPECS, ids=lambda spec: spec.source.value)
def test_event_type_examples_stay_with_their_source(spec):
    assert spec.event_type_examples in _map(spec)
    for other in _SPECS:
        if other is not spec:
            assert other.event_type_examples not in _map(spec)


@pytest.mark.parametrize("spec", (slowlog.SPEC, master_log.SPEC, node_log.SPEC), ids=lambda spec: spec.source.value)
def test_query_log_keyword_rule_is_not_shared(spec):
    assert "앞 5개" not in _map(spec)
    assert "앞 5개" not in _reduce(spec)


def test_query_log_keyword_rule_reaches_query_log():
    assert "앞 5개" in _map(query_log.SPEC)
    assert "앞 5개" in _reduce(query_log.SPEC)


def test_node_log_does_not_claim_code_counts():
    # node_log 타임라인의 "N건"은 선별된 근거 수라 코드가 원본 전체를 세지 않는다.
    assert "따로 센다" not in node_log.SPEC.what_is_noise
