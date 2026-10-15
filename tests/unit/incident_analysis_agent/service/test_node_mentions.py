from datetime import UTC, datetime, timedelta
from itertools import count
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
    ProblemNodeCandidate,
)
from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
from cluster_doctor.incident_analysis_agent.model.resolved_node import ResolvedNode
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.node_investigation.node_investigation import (
    investigate_nodes,
    select_problem_nodes,
)
from cluster_doctor.incident_analysis_agent.service.observation.node_mentions import (
    node_mentions,
)

T0 = datetime(2026, 10, 1, tzinfo=UTC)
WINDOW = TimeRange(start=T0, end=T0 + timedelta(minutes=1))
ID_A = "AAAAAAAAAAAAAAAAAAAAAA"
ID_B = "BBBBBBBBBBBBBBBBBBBBBB"
ID_C = "CCCCCCCCCCCCCCCCCCCCCC"


def full(node_id, name="data-03", ip="10.0.1.23"):
    return "{%s}{%s}{eph}{host}{%s}{%s:9300}{dir}" % (name, node_id, ip, ip)


def event(text, level="WARN"):
    return MasterEvent(timestamp=T0, level=level, line=text)


def test_full_notation_yields_name_and_ip():
    (m,) = node_mentions([event(f"failed [x], node [{full(ID_A)}] timed out")])
    assert (m.node_id, m.node_name, m.ip, m.count) == (ID_A, "data-03", "10.0.1.23", 1)
    assert m.worst_level == "WARN"


def test_id_only_notations_and_invalid_ids_ignored():
    mentions = node_mentions(
        [
            event(f"failed to retrieve stats for node [{ID_A}]"),
            event(f"lost node [{ID_B}]"),
            event("for node [data-03]"),
            event(f"node [{{n}}{{short}}{{e}}{{h}}{{1.1.1.1}}{{a}}]"),
        ]
    )
    assert [m.node_id for m in mentions] == [ID_A, ID_B]


def test_merge_keeps_name_ip_and_worst_level():
    mentions = node_mentions(
        [
            event(f"for node [{ID_A}]", "INFO"),
            event(f"node [{full(ID_A)}]", "ERROR"),
            event(f"for node [{ID_A}]", "WARN"),
        ]
    )
    (m,) = mentions
    assert (m.count, m.node_name, m.ip, m.worst_level) == (3, "data-03", "10.0.1.23", "ERROR")


def test_ranking_severity_then_count_then_first_seen_with_cap():
    events = [
        event(f"for node [{ID_A}]", "INFO"),
        event(f"for node [{ID_A}]", "INFO"),
        event(f"for node [{ID_B}]", "WARN"),
        event(f"for node [{ID_C}]", "ERROR"),
    ]
    evidence = [
        Evidence(evidence_id="E1", event_time=T0, source=EvidenceSource.MASTER_LOG,
                 message=f"node {ID_C} left")
    ]
    result = select_problem_nodes(events, evidence)
    assert [c.node_id for c in result] == [ID_C, ID_B]
    assert result[0].evidence_refs == ("E1",) and result[1].evidence_refs == ()
    assert result[0].reason == "마스터 로그에 노드 ID가 1회 나옴 (ERROR 포함)"
    assert [c.node_id for c in select_problem_nodes(events, [], limit=3)] == [ID_C, ID_B, ID_A]


class Fetcher:
    def __init__(self):
        self.calls = []

    def fetch(self, host, path, cluster, **kwargs):
        self.calls.append((host, path, cluster))
        return "[2026-10-01T09:00:02,123][WARN ][logger] original error"


def select_first(messages, response_format):
    import json
    from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.schema import MapOutput

    return json.dumps(
        {"selected": [{"record_id": 1}]}
        if response_format is MapOutput
        else {"keep": [{"record_id": 1}]}
    )


def run(candidates, resolver, fetcher):
    ids = count(1)
    return investigate_nodes(
        candidates, WINDOW, resolver=resolver, fetcher=fetcher,
        call_llm=select_first, new_evidence_id=lambda: f"E-{next(ids)}",
    )


def resolver(defaults=("/var/log/es", "prod")):
    calls = []
    return SimpleNamespace(
        resolve=lambda _: None,
        cluster_log_defaults=lambda: calls.append(1) or defaults,
        defaults_calls=calls,
    )


def test_no_mentions_means_no_resolver_or_fetcher_calls():
    fetcher = Fetcher()
    assert select_problem_nodes([event("cluster state updated")], []) == []
    res = run([], SimpleNamespace(resolve=lambda _: pytest.fail("called")), fetcher)
    assert res.source_statuses[0].status == "skipped" and fetcher.calls == []


def test_not_found_with_ip_connects_by_address_with_cluster_defaults():
    fetcher, res = Fetcher(), resolver()
    cand = ProblemNodeCandidate(node_id=ID_A, node_name="data-03", ip="10.0.1.23")
    result = run([cand, cand.model_copy(update={"node_id": ID_B})], res, fetcher)
    assert fetcher.calls == [("10.0.1.23", "/var/log/es", "prod")] * 2
    assert len(res.defaults_calls) == 1
    assert result.evidence[0].node_id == ID_A
    assert f"{ID_A} 노드가 ES 노드 목록에 없어 마스터 로그의 주소 10.0.1.23로 접속했다" in result.gaps


@pytest.mark.parametrize(
    "ip,defaults",
    [("", ("/var/log/es", "prod")), ("not-an-ip", ("/var/log/es", "prod")), ("10.0.1.23", (None, ""))],
)
def test_not_found_without_usable_address_keeps_reason(ip, defaults):
    fetcher = Fetcher()
    result = run([ProblemNodeCandidate(node_id=ID_A, ip=ip)], resolver(defaults), fetcher)
    reason = f"ES 노드 목록에 없는 노드라 로그를 수집하지 못했다 (id={ID_A})"
    assert result.source_statuses[0].error == reason and reason in result.gaps
    assert fetcher.calls == []


def test_resolved_node_does_not_query_defaults():
    res = SimpleNamespace(
        resolve=lambda _: ResolvedNode(node_id=ID_A, host="h", log_path="/l", cluster_name="c"),
        cluster_log_defaults=lambda: pytest.fail("called"),
    )
    fetcher = Fetcher()
    run([ProblemNodeCandidate(node_id=ID_A, ip="10.0.1.23")], res, fetcher)
    assert fetcher.calls == [("h", "/l", "c")]
