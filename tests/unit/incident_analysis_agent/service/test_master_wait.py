from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
from cluster_doctor.incident_analysis_agent.service.observation.master_wait import (
    parse_master_wait,
    parse_master_waits,
)

FOLLOWER = (
    "Received response for a request that has timed out, sent [13.4s/13415ms] ago, "
    "timed out [3.4s/3402ms] ago, action [internal:coordination/fault_detection/follower_check], "
    "node [{RC91-18}{NodeIdA0000000000000a1}{EphIdA00000000000000a1}{RC91-18}{10.20.0.143}"
    "{10.20.0.143:9300}{d}{8.12.2}], id [3721132349]"
)
STATS_FAILED = "failed to retrieve stats for node [NodeIdB0000000000000b1]"
STATS_TIMEOUT = (
    "Received response for a request that has timed out, sent [16s/16018ms] ago, "
    "timed out [1s/1001ms] ago, action [cluster:monitor/nodes/stats[n]], "
    "node [{RC92-10}{NodeIdB0000000000000b1}{EphIdB00000000000000b1}{RC92-10}{10.20.1.12}"
    "{10.20.1.12:9300}{d}], id [3721132413]"
)


def _event(at: datetime, line: str, logger: str = "o.e.t.TransportService") -> MasterEvent:
    return MasterEvent(timestamp=at, node="RC93-03", level="WARN", logger=logger, line=line)


def test_follower_check_wait_window():
    received = datetime(2026, 10, 11, 19, 3, 37, 967471, tzinfo=KST)
    wait = parse_master_wait(_event(received, FOLLOWER))
    assert (wait.target_node, wait.target_node_id, wait.target_ip) == (
        "RC91-18", "NodeIdA0000000000000a1", "10.20.0.143")
    assert wait.action == "internal:coordination/fault_detection/follower_check"
    assert wait.sent_at == received - timedelta(milliseconds=13415)
    assert wait.received_at == received
    assert wait.timeout_ms == 13415 - 3402


def test_action_with_brackets_is_kept_whole():
    wait = parse_master_wait(_event(datetime(2026, 10, 11, 19, 3, 40, tzinfo=KST), STATS_TIMEOUT))
    assert wait.action == "cluster:monitor/nodes/stats[n]"


def test_id_only_line_is_resolved_from_same_batch():
    waits = parse_master_waits([
        _event(datetime(2026, 10, 11, 19, 3, 39, tzinfo=KST), STATS_FAILED, "o.e.c.InternalClusterInfoService"),
        _event(datetime(2026, 10, 11, 19, 3, 40, tzinfo=KST), STATS_TIMEOUT),
    ])
    assert [w.target_node for w in waits] == ["RC92-10", "RC92-10"]
    assert waits[0].sent_at is None
    assert waits[0].target_ip == "10.20.1.12"


def test_line_without_node_is_ignored():
    assert parse_master_wait(_event(datetime(2026, 10, 11, 19, 3, tzinfo=KST), "updating number_of_replicas to [1]")) is None
