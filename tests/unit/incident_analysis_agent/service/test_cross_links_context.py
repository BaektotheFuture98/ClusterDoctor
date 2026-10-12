import json
from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry, query_record_key
from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent, Observations, TimelineRow
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import build_analysis_context

FOLLOWER = (
    "Received response for a request that has timed out, sent [13.4s/13415ms] ago, "
    "timed out [3.4s/3402ms] ago, action [internal:coordination/fault_detection/follower_check], "
    "node [{RC6-18}{Vt2xoFlXTDmYm-BHlsgvBg}{JULUWPt3SymLu6RSbm2GyQ}{RC6-18}{192.168.0.143}"
    "{192.168.0.143:9300}{d}], id [1]"
)


def test_context_carries_cross_links_with_evidence_ids():
    query = QueryLogEntry(
        reg_date=datetime(2026, 10, 11, 19, 3, 28, tzinfo=KST), host="h", run_time=Decimal("10.88"),
        success="Y", s_date=0, e_date=0, date_range=365, keyword=(), target_host="192.168.1.33",
        index_name="a", conditions=(), cmd="agg", service="", env="", project="", company="",
        user="", search_count=0, etc="", cluster="",
    )
    evidence = Evidence(evidence_id="E7", event_time=query.reg_date, source=EvidenceSource.QUERY_LOG,
                        message="run_time=10.88", record_key=query_record_key(query))
    observations = Observations(
        query_requests=(query,),
        master_events=(MasterEvent(timestamp=datetime(2026, 10, 11, 19, 3, 37, 967471, tzinfo=KST),
                                   node="RC13-03", level="WARN", logger="o.e.t.TransportService",
                                   line=FOLLOWER),),
    )
    data = json.loads(build_analysis_context(observations, [evidence]))["cross_links"]
    [overlap] = data["master_wait_overlaps"]
    assert overlap["target_node"] == "RC6-18"
    assert overlap["overlap_under_both_assumptions"][0]["evidence_id"] == "E7"
    assert data["minutes"][0]["master_waits"] == 1
    assert data["verdict"]["scope"] == "undetermined"


def _query(at: datetime, **overrides) -> QueryLogEntry:
    values = dict(
        reg_date=at, host="h", run_time=Decimal("10.88"), success="Y", s_date=0, e_date=0,
        date_range=365, keyword=(), target_host="192.168.1.33", index_name="a", conditions=(),
        cmd="agg", service="", env="", project="", company="", user="", search_count=0,
        etc="", cluster="",
    )
    values.update(overrides)
    return QueryLogEntry(**values)


def _follower(node: str, ip: str, *, with_sent: bool = True) -> str:
    line = FOLLOWER.replace("RC6-18", node).replace("192.168.0.143", ip)
    return line if with_sent else line.replace("sent [13.4s/13415ms] ago, ", "")


def _event(second: int, line: str, minute: int = 3) -> MasterEvent:
    return MasterEvent(timestamp=datetime(2026, 10, 11, 19, minute, second, tzinfo=KST), node="RC13-03",
                       level="WARN", logger="o.e.t.TransportService", line=line)


def test_cross_links_shrinks_within_budget_and_records_omissions():
    # 분 단위 줄이 많아 cross_links가 예산을 넘기는 입력. 다른 필드는 비어 있다.
    events = tuple(_event(37, _follower("RC6-18", "192.168.0.143"), minute=m) for m in range(0, 40))
    observations = Observations(master_events=events)
    full = len(build_analysis_context(observations, [], max_chars=10**7))
    limit = full - 1500

    raw = build_analysis_context(observations, [], max_chars=limit)

    assert len(raw) <= limit
    data = json.loads(raw)
    assert data["context_omissions"]["cross_links.minutes"] > 0
    assert data["cross_links"]["verdict"]["scope"] == "no_query_log"


def test_cross_links_only_cite_evidence_ids_that_survived_trimming():
    query = _query(datetime(2026, 10, 11, 19, 3, 28, tzinfo=KST))
    evidence = Evidence(evidence_id="E7", event_time=query.reg_date, source=EvidenceSource.QUERY_LOG,
                        message="run_time=10.88", record_key=query_record_key(query))
    observations = Observations(query_requests=(query,),
                                master_events=(_event(37, _follower("RC6-18", "192.168.0.143")),))
    full = len(build_analysis_context(observations, [evidence], max_chars=10**7))

    for limit in range(full, 0, -25):
        data = json.loads(build_analysis_context(observations, [evidence], max_chars=limit))
        kept = {item["evidence_id"] for item in data["evidence"]}
        for overlap in data["cross_links"]["master_wait_overlaps"]:
            for key in ("overlap_under_both_assumptions", "overlap_under_one_assumption"):
                assert all(i["evidence_id"] in kept | {None} for i in overlap[key])


def test_payload_records_omitted_counts_and_keeps_real_waits_first():
    from cluster_doctor.incident_analysis_agent.service.observation.correlation import (
        build_cross_links,
        cross_links_payload,
    )

    # 앞 이름순 노드는 sent_at이 없는 대기, 뒤 이름순 노드 하나만 실제 늦은 응답이다.
    events = [_event(10 + i, _follower(f"A{i:02d}", f"10.0.0.{i}", with_sent=False)) for i in range(11)]
    events.append(_event(37, _follower("Z99", "10.0.0.99")))
    queries = tuple(_query(datetime(2026, 10, 11, 19, 3, 30, i * 50000, tzinfo=KST)) for i in range(12))
    links = build_cross_links(Observations(query_requests=queries, master_events=tuple(events)))

    payload = cross_links_payload(links, {})

    assert payload["node_cards"][0]["node"] == "Z99"
    assert len(payload["node_cards"]) == 10
    assert payload["omitted"]["node_cards"] == 2
    [overlap] = payload["master_wait_overlaps"]
    assert len(overlap["overlap_under_both_assumptions"]) == 10
    assert payload["omitted"]["overlap_under_both_assumptions"] == 2
    assert "minutes" not in payload["omitted"]


def test_large_cross_links_shrinks_before_primary_fields():
    events = tuple(_event(37, _follower("RC6-18", "192.168.0.143"), minute=m) for m in range(0, 40))
    query = _query(datetime(2026, 10, 11, 19, 3, 28, tzinfo=KST))
    evidence = Evidence(evidence_id="E7", event_time=query.reg_date, source=EvidenceSource.QUERY_LOG,
                        message="run_time=10.88", record_key=query_record_key(query))
    observations = Observations(query_requests=(query,), master_events=events)
    full = len(build_analysis_context(observations, [evidence], max_chars=10**7))

    raw = build_analysis_context(observations, [evidence], max_chars=full - 1500)

    data = json.loads(raw)
    assert len(raw) <= full - 1500
    assert data["evidence"] and data["slow_executions"]
    assert any(key.startswith("cross_links.") for key in data["context_omissions"])
    assert "evidence" not in data["context_omissions"]
    assert "slow_executions" not in data["context_omissions"]


def test_quiet_minutes_are_dropped_before_active_minutes():
    # 19시 0~29분은 느린 쿼리도 마스터 대기도 없는 조용한 분, 19시 30분만 마스터 대기가 있는 활성 분이다.
    timeline = tuple(TimelineRow(minute=datetime(2026, 10, 11, 19, m, tzinfo=KST)) for m in range(0, 30))
    events = (_event(37, _follower("RC6-18", "192.168.0.143"), minute=30),)
    observations = Observations(timeline=timeline, master_events=events)
    full = len(build_analysis_context(observations, [], max_chars=10**7))

    data = json.loads(build_analysis_context(observations, [], max_chars=full - 400))

    assert data["context_omissions"]["cross_links.minutes"] > 0
    assert any(m["master_waits"] == 1 for m in data["cross_links"]["minutes"])
