import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    MasterEvent,
    Observations,
    SourceWindowStatus,
)
from cluster_doctor.incident_analysis_agent.service.observation.correlation import (
    build_cross_links,
    cross_links_payload,
    judge_spread,
)
from cluster_doctor.incident_analysis_agent.service.report_generation import prompts

FOLLOWER = (
    "Received response for a request that has timed out, sent [13.4s/13415ms] ago, "
    "timed out [3.4s/3402ms] ago, action [internal:coordination/fault_detection/follower_check], "
    "node [{RC6-18}{Vt2xoFlXTDmYm-BHlsgvBg}{JULUWPt3SymLu6RSbm2GyQ}{RC6-18}{192.168.0.143}"
    "{192.168.0.143:9300}{d}], id [1]"
)


def _at(minute, second, micro=0):
    return datetime(2026, 10, 11, 19, minute, second, micro, tzinfo=KST)


def _query(at, seconds="16"):
    return QueryLogEntry(
        reg_date=at, host="h", run_time=Decimal(seconds), success="Y", s_date=0, e_date=0,
        date_range=365, keyword=(), target_host="192.168.1.30", index_name="a", conditions=(),
        cmd="agg", service="", env="", project="", company="", user="", search_count=0, etc="", cluster="",
    )


def _event(at, line=FOLLOWER):
    return MasterEvent(timestamp=at, node="RC13-03", level="WARN", logger="o.e.t.TransportService", line=line)


def _follower(node, ip, *, sent="13.4s/13415ms"):
    return FOLLOWER.replace("RC6-18", node).replace("192.168.0.143", ip).replace("13.4s/13415ms", sent)


def test_empty_query_log_is_not_no_slow():
    assert judge_spread(Observations()).scope == "no_query_log"


def test_failed_query_log_is_flagged():
    failed = SourceWindowStatus("es_query_log", _at(0, 0), _at(1, 0), "failed", None, _at(2, 0), error="timeout")
    verdict = judge_spread(Observations(source_statuses=(failed,)))
    assert (verdict.scope, verdict.query_log_failed) == ("no_query_log", True)
    assert judge_spread(Observations()).query_log_failed is False


def test_minute_master_waits_count_every_touched_minute_and_skip_windowless():
    # 대기 구간 19:02:54.6 ~ 19:03:08 은 2분과 3분에 걸친다. sent_at 없는 줄은 세지 않는다.
    events = (_event(_at(3, 8)), _event(_at(5, 0), _follower("A", "10.0.0.1").replace("sent [13.4s/13415ms] ago, ", "")))
    by_minute = {m.minute: m.master_waits for m in build_cross_links(Observations(master_events=events)).minutes}
    assert by_minute[_at(2, 0)] == 1 and by_minute[_at(3, 0)] == 1
    assert _at(5, 0) not in by_minute or by_minute[_at(5, 0)] == 0


def _many_overlaps():
    # 앞 시각의 대기 12건은 쿼리와 안 겹치고, 마지막 대기 하나만 느린 쿼리와 겹친다.
    events = [_event(_at(0, 20 + i), _follower(f"N{i:02d}", f"10.0.0.{i}", sent="1.0s/1000ms")) for i in range(12)]
    events.append(_event(_at(3, 37), _follower("Z99", "10.0.0.99")))
    queries = (_query(_at(3, 30), "10.88"),)
    return build_cross_links(Observations(query_requests=queries, master_events=tuple(events)))


def test_payload_ranks_overlaps_with_queries_first_and_counts_empty_ones():
    payload = cross_links_payload(_many_overlaps(), {})
    [overlap] = payload["master_wait_overlaps"]
    assert overlap["target_node"] == "Z99"
    assert payload["omitted"]["master_wait_overlaps_without_queries"] == 12
    assert "master_wait_overlaps" not in payload["omitted"]


def test_payload_node_cards_carry_late_responses():
    events = (_event(_at(3, 37), _follower("A", "10.0.0.1")),
              _event(_at(3, 40), _follower("A", "10.0.0.1").replace("sent [13.4s/13415ms] ago, ", "")))
    payload = cross_links_payload(build_cross_links(Observations(master_events=events)), {})
    assert payload["node_cards"][0]["late_responses"] == 1
    assert payload["node_cards"][0]["master_waits"] == 2


def test_payload_records_profile_cut():
    queries = tuple(
        replace(_query(_at(1, i)), cmd=f"c{i}") for i in range(10))
    payload = cross_links_payload(build_cross_links(Observations(query_requests=queries)), {})
    assert len(payload["slow_query_profile"]) == 8
    assert payload["omitted"]["slow_query_profile"] == 2
    json.dumps(payload)


def test_prompt_rules_explain_new_scopes_and_late_responses():
    rules = prompts._CROSS_LINK_RULES
    assert "no_query_log" in rules and "no_slow" in rules
    assert "late_responses>0" in rules
